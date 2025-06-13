import json
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from platformdirs import user_config_dir
import pandas as pd
import fire

from providers import PROVIDER_MAP


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
        self.providers = self._make_provider_map(providers)

        age_days = self.get_database_ages()
        for name, age in age_days.items():
            if age > 30:
                print(f"Warning: {name.upper()} database is {age} days old. Consider updating.")

    def add_providers(self, to_add: str | list[str]):
        to_add = self._validate_providers(to_add)
        for p in to_add:
            self.providers[p] = PROVIDER_MAP[p](self.data_dir)

    def remove_providers(self, to_remove: str | list[str]):
        to_remove = self._validate_providers(to_remove)
        for p in to_remove:
            # Throws a KeyError if p is not found.
            self.providers.pop(p)
            
    def set_providers(self, providers: str | list[str] = None):
        self.providers = self._make_provider_map(providers)

    def _make_provider_map(self, providers):
        providers = self._validate_providers(providers)
        return {name: PROVIDER_MAP[name](self.data_dir) for name in providers}

    def download(self, providers: str | list[str] = None, workers: int = 1, update: bool = False):
        if providers:
            self.set_providers(providers)

        def worker_fn(p):
            self.providers[p].download_station_info(update)
            if p != "hydat":
                self.providers[p].download_daily_values(None)

        if workers > 1:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = [executor.submit(worker_fn, p) for p in self.providers]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as exc:
                        print(f"A provider download failed: {exc}")
        else:
            for p in self.providers:
                worker_fn(p)

    def download_station_info(
        self, providers: str | list[str] = None, workers: int = 1, update: bool = False
    ):
        if providers:
            self.set_providers(providers)

        def worker_fn(p):
            self.providers[p].download_station_info(update)

        if workers > 1:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = [executor.submit(worker_fn, p) for p in self.providers]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as exc:
                        print(f"A provider download failed: {exc}")
        else:
            for p in self.providers:
                worker_fn(p)

    def download_daily_values(
        self,
        providers: str | list[str] = None,
        sites: str | list[str] = None,
        workers: int = 1,
    ):
        if providers:
            self.set_providers(providers)
        sites = self._validate_sites(sites)

        def worker_fn(p):
            self.providers[p].download_daily_values(sites)

        if workers > 1:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = [executor.submit(worker_fn, p) for p in self.providers]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as exc:
                        print(f"A provider download failed: {exc}")
        else:
            for p in self.providers:
                worker_fn(p)

    def get_database_ages(self) -> dict[str, int]:
        ages = {p: self.providers[p].get_db_age() for p in self.providers}
        return ages

    def get_station_info(self) -> pd.DataFrame:
        provider_info = [self.providers[p].get_station_info() for p in self.providers]
        gdf = pd.concat([df for df in provider_info if df is not None and not df.empty])
        return gdf

    def get_active_stations(self) -> pd.DataFrame:
        gdf = self.get_station_info()
        return gdf[gdf["active"]]

    def get_daily_values(
        self, sites: str | list[str] = None, start_date: str = None, end_date: str = None
    ) -> pd.DataFrame:
        # sites = self._validate_sites(sites)
        sites_dict = self._preprocess_sites(sites)

        provider_dfs = [
            self.providers[_p].get_daily_data(_s, start_date, end_date)
            for _p, _s in sites_dict.items()
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

    def _preprocess_sites(self, sites: str | list[str] | None) -> dict[str, list[str]] | None:
        if sites is None:
            # Return a dict with all active providers and None for sites, indicating all sites
            return {p: None for p in self.providers}

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

            if provider_name not in self.providers:
                raise ValueError(
                    f"Provider '{provider_name}' from site_id '{site_id}' is not in the "
                    f"list of active providers: {list(self.providers)}"
                )

            provider_sites_map[provider_name].append(site_id)

        return dict(provider_sites_map)


class Config:
    @staticmethod
    def set_data_dir(path: str):
        """Set the default data directory for downloads."""
        set_default_data_dir(path)
        print(f"Default data directory set to: {path}")


class Download:
    @staticmethod
    def all(providers=None, workers=1, update=False):
        """Download all data (station info and timeseries)."""
        downloader = GaugeDataFacade(providers=providers)
        downloader.download(update=update, workers=workers)

    @staticmethod
    def stations(providers=None, workers=1, update=False):
        """Download only station info."""
        downloader = GaugeDataFacade(providers=providers)
        downloader.download_station_info(update=update, workers=workers)

    @staticmethod
    def timeseries(providers=None, workers=1):
        """Download only timeseries data."""
        downloader = GaugeDataFacade(providers=providers)
        downloader.download_daily_values(workers=workers)


if __name__ == "__main__":
    fire.Fire(
        {
            "config": Config,
            "download": Download,
        }
    )
