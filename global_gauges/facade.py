import json
import logging
import asyncio
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from platformdirs import user_config_dir
import pandas as pd
import geopandas as gpd

from .providers import PROVIDER_MAP, BaseProvider

# Private manager for the config file and data directory.
class _ConfigManager:
    """
    Manages loading and saving the default data directory.
    It lazy-loads the configuration from the file on first access.
    """
    def __init__(self):
        self.config_dir = Path(user_config_dir("global_gauges"))
        self.config_path = self.config_dir / "config.json"
        self._default_data_dir: Path = None
        self._loaded_from_file: bool = False

    def _load(self):
        """Loads configuration from the JSON file."""
        if self.config_path.exists():
            try:
                with self.config_path.open("r") as f:
                    config = json.load(f)
                path_str = config.get("data_dir")
                if path_str:
                    self._default_data_dir = Path(path_str)
            except (IOError, json.JSONDecodeError) as e:
                print(f"Warning: Could not read config file at {self.config_path}: {e}")
        self._loaded_from_file = True

    def get_default_data_dir(self) -> Path | None:
        """Returns the default data directory, loading it from file if necessary."""
        if not self._loaded_from_file:
            self._load()
        return self._default_data_dir

    def set_default_data_dir(self, path: str | Path) -> Path:
        """Sets and persists the default data directory."""
        data_dir = Path(path).resolve()
        self._default_data_dir = data_dir
        self._loaded_from_file = True  # The value is now set, no need to load from file again
        try:
            self.config_dir.mkdir(parents=True, exist_ok=True)
            with self.config_path.open("w") as f:
                json.dump({"data_dir": str(data_dir)}, f)
        except IOError as e:
            print(f"Warning: Could not save default data_dir to {self.config_path}: {e}")
        return data_dir

# Instantiate the data dir manager. Does not actually load anything yet. 
config = _ConfigManager()



# --- Public facing functions --- 

def set_default_data_dir(path: str | Path):
    """Public function to set the default data directory for the library."""
    return config.set_default_data_dir(path)


class GaugeDataFacade:
    providers: dict[str, BaseProvider]

    def __init__(
        self,
        data_dir: str | Path = None,
        providers: str | list[str] | set[str] = None,
    ):
        # Use the provided data_dir if it exists. 
        # Otherwise, ask the config manager for the default.
        data_dir = data_dir or config.get_default_data_dir()

        if data_dir is None:
            raise ValueError(
                "No data_dir provided and no default set. "
                "Please provide a data_dir or call set_default_data_dir()."
            )
        
        self.data_dir = Path(data_dir)

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
        self,
        providers: str | list[str] = None,
        tolerance: int = 1,
        force_update: bool = False,
        workers: int = 1,
    ):
        if providers:
            self.set_providers(providers)

        # Now use None for providers to keep what we just set.
        self.download_station_info(None, force_update, workers)
        self.download_daily_values(None, None, tolerance, force_update, workers)

    def download_station_info(
        self, providers: str | list[str] = None, force_update: bool = False, workers: int = 1
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
        tolerance: int = 1,
        force_update: bool = False,
        workers: int = 1,
    ):
        if providers:
            self.set_providers(providers)

        sites_dict = self._preprocess_sites(sites)

        def worker_fn(provider: BaseProvider, p_sites: list):
            asyncio.run(provider.download_daily_values(p_sites, tolerance, force_update))

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

    def get_active_stations(self) -> gpd.GeoDataFrame:
        gdf = self.get_station_info()
        return gdf[gdf["active"]]
    
    def get_stations_n_days(self, days:int) -> gpd.GeoDataFrame:
        gdf = self.get_station_info()
        cutoff = pd.Timestamp.now().normalize() - pd.Timedelta(days, 'days')
        return gdf[gdf['max_date'] >= cutoff]

    def get_daily_values(
        self, sites: str | list[str] = None, start_date: str = None, end_date: str = None
    ) -> pd.DataFrame:
        # sites = self._validate_sites(sites)
        sites_dict = self._preprocess_sites(sites)

        provider_dfs = []
        for _provider, _sites in sites_dict.items():
            p_df = _provider.get_daily_data(_sites, start_date, end_date)
            p_df["provider"] = _provider.name.upper()
            provider_dfs.append(p_df)

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
            try:
                with ThreadPoolExecutor(max_workers=workers) as executor:
                    futures = [executor.submit(worker_fn, *args) for args in args_iter]
                    for future in as_completed(futures):
                        try:
                            future.result()
                        except Exception as exc:
                            print(f"A provider download failed: {exc}")
            except KeyboardInterrupt:
                print("\nKeyboardInterrupt received. Attempting to shut down threads...")
                executor.shutdown(wait=False, cancel_futures=True)
                raise
        else:
            try:
                for args in args_iter:
                    worker_fn(*args)
            except KeyboardInterrupt:
                print("\nKeyboardInterrupt received. Exiting...")
                raise

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

