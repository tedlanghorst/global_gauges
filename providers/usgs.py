from datetime import datetime

import pandas as pd
import dataretrieval.nwis as nwis

from ._base import BaseProvider


class USGSProvider(BaseProvider):
    """Data provider for the USGS National Water Information System (NWIS).

    Accessed through the `dataretrieval` package by the USGS.
    https://github.com/DOI-USGS/dataretrieval-python
    """

    name = "usgs"
    # https://help.waterdata.usgs.gov/codes-and-parameters/daily-value-qualification-code-dv_rmk_cd
    quality_map = {
        "A": "good",
        "P": "provisional",
        "e": "suspect",
        "Ice": "bad",
        "Fld": "bad",
        "Eqp": "bad",
        "<": "bad",
        ">": "bad",
    }

    def _download_station_info(self) -> pd.DataFrame:
        """Download and save metadata for all USGS sites with discharge data."""
        # All sites with discharge data
        sites_list = []
        for huc in range(1, 23):
            sites, _ = nwis.get_info(huc=f"{huc:02d}", parameterCd="00060")
            sites_list.append(sites)
        sites = pd.concat(sites_list, ignore_index=True)

        # Now setup columns to match other sources
        sites = sites.to_crs("EPSG:4326")
        sites["area"] = sites["drain_area_va"] * (1.60934**2)  # mi2 to km2
        sites = sites.rename(
            columns={"agency_cd": "source", "site_no": "site_id", "station_nm": "name"}
        )

        # Extract latitude and longitude from geometry column
        sites["latitude"] = sites.geometry.y
        sites["longitude"] = sites.geometry.x

        return sites

    def _download_daily_values(self, site_id: str):
        """
        Download daily discharge data for the given site_ids and store in a SQLite database.
        If update=True, download and append new data for each site (after the latest date in the DB).
        """
        end_date = datetime.now()

        last_date = self.get_last_updated(site_id)
        if last_date:
            start_date = last_date + pd.Timedelta(days=1)
            if start_date >= end_date:
                return
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
            return data[["site_id", "date", "discharge", "quality_flag"]]
        else:
            return None
