import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

from .providers import PROVIDER_MAP


class GaugeDataFacade:
    def __init__(
        self,
        data_dir: str | Path,
        providers: str | list[str] | set[str] = None,
        workers: int = 1,
        update: bool = False,
    ):
        self.data_dir = Path(data_dir)
        self.providers = self._validate_providers(providers)
        self.provider_map = self.make_provider_map()
        self.workers = workers
        self.update = update

        age_days = self.get_database_ages()
        for name, age in age_days.items():
            if age > 30:
                print(f"Warning: {name.upper()} database is {age} days old. Consider updating.")

    def add_provider(self, provider: str | list[str] | set[str]):
        validated = self._validate_providers(provider)
        self.providers |= validated
        self.provider_map = self.make_provider_map()

    def remove_provider(self, provider: str | list[str] | set[str]):
        validated = self._validate_providers(provider)
        self.providers -= validated
        self.provider_map = self.make_provider_map()

    def set_providers(self, providers: str | list[str] | set[str] = None):
        self.providers = self._validate_providers(providers)
        self.provider_map = self.make_provider_map()

    def make_provider_map(self):
        return {name: PROVIDER_MAP[name](self.data_dir) for name in self.providers}

    def download_all(self):
        def worker_fn(p):
            print(f"\nDownloading data for {p.upper()}")
            self.provider_map[p].download_station_info(self.update)
            if p != "hydat":
                self.provider_map[p].download_daily_values(None, self.update)

        if self.workers > 1:
            with ThreadPoolExecutor(max_workers=self.workers) as executor:
                futures = [executor.submit(worker_fn, p) for p in self.providers]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as exc:
                        print(f"A provider download failed: {exc}")
        else:
            for p in self.providers:
                worker_fn(p)

    def download_station_info(self):
        def worker_fn(p):
            print(f"Downloading station info for {p.upper()}")
            self.provider_map[p].download_station_info(update=self.update)

        if self.workers > 1:
            with ThreadPoolExecutor(max_workers=self.workers) as executor:
                futures = [executor.submit(worker_fn, p) for p in self.providers]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as exc:
                        print(f"A provider download failed: {exc}")
        else:
            for p in self.providers:
                worker_fn(p)

    def download_daily_values(self, sites: str | list[str] = None):
        sites = self._validate_sites(sites)

        def worker_fn(p):
            print(f"Downloading daily data for {p.upper()}")
            self.provider_map[p].download_daily_values(sites, self.update)

        if self.workers > 1:
            with ThreadPoolExecutor(max_workers=self.workers) as executor:
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
        ages = {p: self.provider_map[p].get_db_age() for p in self.providers}
        return ages

    def get_station_info(self) -> pd.DataFrame:
        provider_info = [self.provider_map[p].get_station_info() for p in self.providers]
        gdf = pd.concat([df for df in provider_info if df is not None and not df.empty])
        return gdf

    def get_active_stations(self) -> pd.DataFrame:
        gdf = self.get_station_info()
        return gdf[gdf["active"]]

    def get_daily_values(
        self, sites: str | list[str] = None, start_date: str = None, end_date: str = None
    ) -> pd.DataFrame:
        sites = self._validate_sites(sites)
        provider_dv = [
            self.provider_map[p].get_daily_data(sites, start_date=start_date, end_date=end_date)
            for p in self.providers
        ]
        df = pd.concat([df for df in provider_dv if df is not None and not df.empty])
        return df

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

    @staticmethod
    def _validate_sites(sites: str | list[str] | None) -> list[str]:
        if sites is None or isinstance(sites, list):
            return sites
        elif isinstance(sites, str):
            return [sites]
        else:
            raise TypeError("Sites must be of type None, str, or list.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Global Gauges Data Downloader")
    parser.add_argument(
        "--data-dir",
        required=True,
        help="Path to the data directory.",
    )
    parser.add_argument(
        "--providers",
        nargs="*",
        default=None,
        help="Provider name(s) to download. If omitted, all providers are used.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of parallel workers for provider downloads.",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        default=False,
        help="If set, update existing data.",
    )
    arg_dict = vars(parser.parse_args())
    downloader = GaugeDataFacade(**arg_dict)
    downloader.download_all()
