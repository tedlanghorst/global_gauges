from datetime import datetime

import pandas as pd
import dataretrieval.nwis as nwis
from tqdm.auto import tqdm

from ._base import BaseProvider


class UsgsProvider(BaseProvider):
    """Data provider for the USGS National Water Information System (NWIS)."""

    name = "usgs"

    def _download_station_info(self) -> None:
        """Download and save metadata for all USGS sites with discharge data."""
        # All sites with discharge data
        sites_list = []
        for huc in range(1, 23):
            sites, _ = nwis.get_info(huc=f"{huc:02d}", parameterCd="00060")
            sites_list.append(sites)
        sites = pd.concat(sites_list, ignore_index=True)

        # Get active sites (per NWIS definition)
        active_sites_list = []
        for huc in range(1, 23):
            active, _ = nwis.get_info(huc=f"{huc:02d}", parameterCd="00060", siteStatus="active")
            active_sites_list.append(active)
        active_sites = pd.concat(active_sites_list, ignore_index=True)
        active_ids = set(active_sites["site_no"])

        # Now setup columns to match other sources
        sites = sites.to_crs("EPSG:4326")
        sites["area"] = sites["drain_area_va"] * (1.60934**2)  # mi2 to km2
        sites = sites.rename(
            columns={"agency_cd": "source", "site_no": "site_id", "station_nm": "name"}
        )
        sites = sites[["site_id", "name", "area", "source", "geometry"]]
        sites["active"] = sites["site_id"].isin(active_ids)

        sites.to_file(self.station_path, driver="GeoJSON")

    def _download_daily_values(self, site_ids: list[str], conn):
        """
        Download daily discharge data for the given site_ids and store in a SQLite database.
        If update=True, download and append new data for each site (after the latest date in the DB).
        """
        end_date = datetime.now()

        for site_id in tqdm(site_ids):
            last_date = self.get_last_entry_date(site_id)
            if last_date:
                if last_date >= end_date:
                    continue  # Already up to date
                else:
                    start_date = last_date + pd.Timedelta(days=1)
            else:
                start_date = pd.to_datetime("1950-01-01")

            data, _ = nwis.get_dv(
                sites=site_id,
                start=start_date.strftime("%Y-%m-%d"),
                end=end_date.strftime("%Y-%m-%d"),
                parameterCd="00060",
            )
            if not data.empty and "00060_Mean" in data.columns:
                data = data.reset_index()
                data = data.rename(
                    columns={
                        "site_no": "site_id",
                        "00060_Mean": "discharge",
                        "00060_Mean_cd": "quality_flag",
                        "datetime": "datetime",
                    }
                )
                data["discharge"] *= 0.3048**3  # ft3 to m3
                data["date"] = pd.to_datetime(data["datetime"]).dt.date
                cols = ["site_id", "date", "discharge", "quality_flag"]
                data[cols].to_sql("discharge", conn, if_exists="append", index=False)
