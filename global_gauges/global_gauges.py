import json
import logging
import asyncio
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from platformdirs import user_config_dir
import pandas as pd
import geopandas as gpd
import fire

from .providers import PROVIDER_MAP, BaseProvider


CONFIG_DIR = Path(user_config_dir("global_gauges"))
CONFIG_PATH = CONFIG_DIR / "config.json"
_default_data_dir = None


def _load_default_data_dir():
    global _default_data_dir
    if CONFIG_PATH.exists():
        try:
            with open(CONFIG_PATH, "r") as f:
                config = json.load(f)
            data_dir = config.get("data_dir")
            if data_dir:
                _default_data_dir = Path(data_dir)
        except Exception:
            pass


_load_default_data_dir()


def set_default_data_dir(path: str | Path):
    global _default_data_dir
    data_dir = Path(path)
    _default_data_dir = data_dir
    try:
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        with open(CONFIG_PATH, "w") as f:
            json.dump({"data_dir": str(data_dir)}, f)
    except Exception as e:
        print(f"Warning: Could not save default data_dir to {CONFIG_PATH}: {e}")
    return data_dir


class GaugeDataFacade:
    providers: dict[str, BaseProvider]

    def __init__(
        self,
        data_dir: str | Path = None,
        providers: str | list[str] | set[str] = None,
    ):
        if data_dir is not None:
            self.data_dir = Path(data_dir)
        elif _default_data_dir is not None:
            self.data_dir = _default_data_dir
        else:
            raise ValueError(
                "No data_dir provided and no default set. "
                "Please provide a data_dir or call set_default_data_dir()."
            )

        # Set up logging
        log_dir = self.logs_dir = self.data_dir / "_logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        logging.basicConfig(
            filename=log_dir / (pd.Timestamp.now().isoformat(timespec="minutes") + ".log"),
            level=logging.WARNING,
            format="%(asctime)s - %(levelname)s - %(message)s",
            force=True,
        )

        self.set_providers(providers)

        age_days = self.get_database_ages()
        for name, age in age_days.items():
            if age > 30:
                print(f"Warning: {name.upper()} database is {age} days old. Consider updating.")

    def __str__(self) -> str:
        """Returns a user-friendly string representation of the facade."""
        providers_str = "\n".join(f"    {k}: {v.desc}" for k, v in self.providers.items())
        return (
            f"GaugeDataFacade\n"
            f"  Data Directory: {self.data_dir}\n"
            f"  Active Providers:\n" + providers_str
        )

    def __repr__(self) -> str:
        """Returns an unambiguous string representation of the facade."""
        provider_names = list(self.providers.keys())
        return f"GaugeDataFacade(data_dir='{self.data_dir!s}', providers={provider_names})"

    def add_providers(self, to_add: str | list[str]):
        to_add = self._validate_providers(to_add)
        for p in to_add:
            self.providers[p] = PROVIDER_MAP[p](self.data_dir)

    def remove_providers(self, to_remove: str | list[str]):
        to_remove = self._validate_providers(to_remove)
        for p in to_remove:
            # Throws a KeyError if p is not found.
            self.providers.pop(p)

    def set_providers(self, providers):
        providers = self._validate_providers(providers)
        self.providers = {name: PROVIDER_MAP[name](self.data_dir) for name in providers}

    def download(
        self, providers: str | list[str] = None, workers: int = 1, force_update: bool = False
    ):
        if providers:
            self.set_providers(providers)

        # Now use None for providers to keep what we just set.
        self.download_station_info(None, workers, force_update)
        self.download_daily_values(None, None, workers, force_update)

    def download_station_info(
        self, providers: str | list[str] = None, workers: int = 1, force_update: bool = False
    ):
        if providers:
            self.set_providers(providers)

        def worker_fn(provider: BaseProvider):
            provider.download_station_info(force_update)

        args_iter = [(provider,) for provider in self.providers.values()]
        self._run_workers(worker_fn, args_iter, workers)

    def download_daily_values(
        self,
        providers: str | list[str] = None,
        sites: str | list[str] = None,
        workers: int = 1,
        force_update: bool = True,
    ):
        if providers:
            self.set_providers(providers)

        sites_dict = self._preprocess_sites(sites)

        def worker_fn(provider: BaseProvider, p_sites: list):
            asyncio.run(provider.download_daily_values(p_sites, force_update))

        args_iter = list(sites_dict.items())
        self._run_workers(worker_fn, args_iter, workers)

    def get_database_ages(self) -> dict[str, int]:
        ages = {name: provider.get_database_age_days() for name, provider in self.providers.items()}
        return ages

    def get_station_info(self) -> gpd.GeoDataFrame:
        provider_info = []
        for name, provider in self.providers.items():
            p_stations = provider.get_station_info()
            p_stations["provider"] = name
            provider_info.append(p_stations)

        return pd.concat(provider_info)

    def get_active_stations(self) -> pd.DataFrame:
        gdf = self.get_station_info()
        return gdf[gdf["active"]]

    def get_daily_values(
        self, sites: str | list[str] = None, start_date: str = None, end_date: str = None
    ) -> pd.DataFrame:
        # sites = self._validate_sites(sites)
        sites_dict = self._preprocess_sites(sites)

        provider_dfs = [
            p.get_daily_data(p_sites, start_date, end_date) for p, p_sites in sites_dict.items()
        ]
        if provider_dfs:
            return pd.concat(provider_dfs)

    @staticmethod
    def _validate_providers(providers: str | list[str] | set[str] | None) -> set[str]:
        # Accept None or empty list/set as 'all providers'
        if providers is None or providers == [] or providers == set():
            providers = set(PROVIDER_MAP.keys())
        elif isinstance(providers, str):
            providers = {providers}
            missing = providers - set(PROVIDER_MAP)
            if missing:
                raise ValueError(f"Provider(s) {missing} not recognized.")
        elif isinstance(providers, list):
            providers = set(providers)
            missing = providers - set(PROVIDER_MAP)
            if missing:
                raise ValueError(f"Provider(s) {missing} not recognized.")
        elif isinstance(providers, set):
            missing = providers - set(PROVIDER_MAP)
            if missing:
                raise ValueError(f"Provider(s) {missing} not recognized.")
        else:
            raise TypeError("Providers must be of type None, str, list, or set.")

        return providers

    def _run_workers(self, worker_fn, args_iter, workers):
        """Helper to run worker_fn over args_iter with optional threading."""
        if workers > 1:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = [executor.submit(worker_fn, *args) for args in args_iter]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as exc:
                        print(f"A provider download failed: {exc}")
        else:
            for args in args_iter:
                worker_fn(*args)

    def _preprocess_sites(self, sites: str | list[str] | None) -> dict[BaseProvider, list[str]]:
        if sites is None:
            # Return a dict with all active providers and None for sites, indicating all sites
            return {provider: None for provider in self.providers.values()}

        if isinstance(sites, str):
            sites = [sites]
        elif not isinstance(sites, list):
            raise TypeError("Sites must be of type None, str, or list.")

        # Split the site IDS
        provider_sites_map = defaultdict(list)
        for site_id in sites:
            try:
                provider_name, _ = site_id.split("-", 1)
                provider_name = provider_name.lower()
            except ValueError:
                raise ValueError(
                    f"Invalid site_id format: '{site_id}'. "
                    "Expected format is '<PROVIDER>-<station_id>'."
                )

            provider = self.providers.get(provider_name)
            if provider is None:
                raise ValueError(
                    f"Provider '{provider_name}' from site_id '{site_id}' is not in the "
                    f"list of active providers: {list(self.providers)}"
                )

            provider_sites_map[provider].append(site_id)

        return dict(provider_sites_map)


"""
CLI interface for downloading.
"""


class Config:
    @staticmethod
    def set_data_dir(path: str):
        """Set the default data directory for downloads."""
        set_default_data_dir(path)
        print(f"Default data directory set to: {path}")


class Download:
    @staticmethod
    def all(providers=None, workers=1, force_update=False):
        """Download all data (station info and timeseries)."""
        downloader = GaugeDataFacade(providers=providers)
        downloader.download(force_update=force_update, workers=workers)

    @staticmethod
    def stations(providers=None, workers=1, force_update=False):
        """Download only station info."""
        downloader = GaugeDataFacade(providers=providers)
        downloader.download_station_info(force_update=force_update, workers=workers)

    @staticmethod
    def timeseries(providers=None, workers=1, force_update=False):
        """Download only timeseries data."""
        downloader = GaugeDataFacade(providers=providers)
        downloader.download_daily_values(workers=workers, force_update=force_update)


if __name__ == "__main__":
    fire.Fire(
        {
            "config": Config,
            "download": Download,
        }
    )
