import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from .providers import PROVIDER_MAP


def download_all_data(providers: str | list[str] = None, update: bool = False, workers: int = 1):
    """
    Download all data (station info and daily values) for one or more specified providers.

    Parameters
    ----------
    providers : str or list of str, optional
        Provider name(s). If None, all providers are used.
    workers : int, default 1
        Number of parallel workers for provider downloads.
    """
    providers = _validate_providers(providers)

    def worker_fn(p):
        print(f"Downloading data for {p.upper()}")
        PROVIDER_MAP[p].download_station_info(update=update)
        PROVIDER_MAP[p].download_daily_values(update=update)
        print(f"Data for {p.upper()} downloaded successfully.")

    if workers > 1:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(worker_fn, p) for p in providers]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    print(f"A provider download failed: {exc}")
    else:
        for p in providers:
            worker_fn(p)


def download_station_info(
    providers: str | list[str] = None, update: bool = False, workers: int = 1
):
    """
    Download station metadata for one or more specified providers.

    Parameters
    ----------
    providers : str or list of str, optional
        Provider name(s). If None, all providers are used.
    workers : int, default 1
        Number of parallel workers for provider downloads.
    """
    providers = _validate_providers(providers)

    def worker_fn(p):
        print(f"Downloading data for {p.upper()}")
        PROVIDER_MAP[p].download_station_info(update=update)

    if workers > 1:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(worker_fn, p) for p in providers]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    print(f"A provider download failed: {exc}")
    else:
        for p in providers:
            worker_fn(p)


def download_daily_values(
    providers: str | list[str] = None,
    sites: str | list[str] = None,
    update: bool = False,
    workers: int = 1,
):
    """
    Download daily discharge data for one or more specified providers and optional sites.

    Parameters
    ----------
    providers : str or list of str, optional
        Provider name(s). If None, all providers are used.
    sites : str or list of str, optional
        Site identifier(s) to download. If None, all sites are used.
    update : bool, default False
        If True, update existing data.
    workers : int, default 1
        Number of parallel workers for provider downloads.
    """
    providers = _validate_providers(providers)
    sites = _validate_sites(sites)

    def worker_fn(p):
        print(f"Downloading data for {p.upper()}")
        PROVIDER_MAP[p].download_daily_values(sites, update)

    if workers > 1:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(worker_fn, p) for p in providers]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    print(f"A provider download failed: {exc}")
    else:
        for p in providers:
            worker_fn(p)


def get_database_age(providers: str | list[str] = None) -> dict[str, int]:
    """
    Get the age (in days) of the SQLite database for each provider.

    Parameters
    ----------
    providers : str or list of str, optional
        Provider name(s). If None, all providers are used.

    Returns
    -------
    dict
        Mapping of provider name to database age in days (-1 if not found).
    """
    providers = _validate_providers(providers)
    ages = {p: PROVIDER_MAP[p].get_db_age() for p in providers}

    return ages


def get_station_info(providers: str | list[str] = None) -> pd.DataFrame:
    """
    Retrieve metadata for all sites from the specified data provider(s).

    Parameters
    ----------
    providers : str or list of str, optional
        Provider name(s). If None, all providers are used.

    Returns
    -------
    DataFrame
        Metadata for all sites from the selected providers.
    """
    providers = _validate_providers(providers)

    provider_info = [PROVIDER_MAP[p].get_station_info() for p in providers]
    gdf = pd.concat([df for df in provider_info if df is not None and not df.empty])
    return gdf


def get_active_stations(providers: str | list[str] = None) -> pd.DataFrame:
    """
    Return only active stations from the specified provider(s).

    Parameters
    ----------
    providers : str or list of str, optional
        Provider name(s). If None, all providers are used.

    Returns
    -------
    DataFrame
        Active stations only.
    """
    gdf = get_station_info(providers)
    return gdf[gdf["active"]]


def get_daily_values(
    *,
    providers: str | list[str] = None,
    sites: str | list[str] = None,
    start_date: str = None,
    end_date: str = None,
) -> pd.DataFrame:
    """
    Query daily discharge data for the specified provider(s), optional site(s), and date range.

    Parameters
    ----------
    providers : str or list of str, optional
        Provider name(s). If None, all providers are used.
    sites : str or list of str, optional
        Site identifier(s) to query. If None, all sites are used.
    start_date : str, optional
        Start date (YYYY-MM-DD).
    end_date : str, optional
        End date (YYYY-MM-DD).

    Returns
    -------
    DataFrame
        Daily discharge data for the selected sites/providers.
    """
    providers = _validate_providers(providers)
    sites = _validate_sites(sites)

    provider_dv = (
        PROVIDER_MAP[p].get_daily_data(sites, start_date=start_date, end_date=end_date)
        for p in providers
    )
    df = pd.concat([df for df in provider_dv if df is not None and not df.empty])
    return df


def _validate_providers(providers: str | list[str] | None) -> list[str]:
    # Accept None or empty list as 'all providers'
    if providers is None or providers == []:
        providers = PROVIDER_MAP.keys()
    elif isinstance(providers, str):
        providers = [providers]
    elif not isinstance(providers, list):
        raise TypeError("Providers must be of type None, str, or list.")

    for provider in providers:
        if provider not in PROVIDER_MAP:
            raise ValueError(f"Provider '{provider}' not recognized.")

    return providers


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

    # Unpack args dict directly into download_all_data
    download_all_data(**arg_dict)
