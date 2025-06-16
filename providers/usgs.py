import pandas as pd
import geopandas as gpd
import dataretrieval.nwis as nwis

from ._base import BaseProvider
from ._dmodel import QualityFlag


class USGSProvider(BaseProvider):
    """Data provider for the USGS National Water Information System (NWIS).

    Accessed through the `dataretrieval` package by the USGS.
    https://github.com/DOI-USGS/dataretrieval-python
    """

    name = "usgs"
    # https://help.waterdata.usgs.gov/codes-and-parameters/daily-value-qualification-code-dv_rmk_cd
    # TODO use new 'ESTIMATED' enum. Maybe change < and > to suspect.
    quality_map = {
        "A": QualityFlag.GOOD,
        "P": QualityFlag.PROVISIONAL,
        "e": QualityFlag.ESTIMATED,
        "&": QualityFlag.ESTIMATED,
        "E": QualityFlag.ESTIMATED,
        "<": QualityFlag.SUSPECT,
        ">": QualityFlag.SUSPECT,
        "Ice": QualityFlag.BAD,
        "Fld": QualityFlag.BAD,
        "Eqp": QualityFlag.BAD,
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
        sites = gpd.GeoDataFrame(sites).to_crs("EPSG:4326")
        sites["area"] = sites["drain_area_va"] * (1.60934**2)  # mi2 to km2
        sites["active"] = False
        sites = sites.rename(columns={"site_no": "site_id", "station_nm": "name"})

        # Extract latitude and longitude from geometry column
        sites["latitude"] = sites.geometry.y
        sites["longitude"] = sites.geometry.x

        return sites

    def _download_daily_values(self, site_id: str, start: pd.Timestamp, misc: dict) -> pd.DataFrame:
        """
        Download daily discharge data for the given site_ids and store in a SQLite database.
        If update=True, download and append new data for each site (after the latest date in the DB).
        """
        end = pd.Timestamp.now().date()

        data, _ = nwis.get_dv(
            sites=site_id,
            start=start.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
            parameterCd="00060",
        )

        if data.empty or "00060_Mean" not in data.columns:
            return None

        data = data.reset_index()
        data = data.rename(
            columns={
                "site_no": "site_id",
                "00060_Mean": "discharge",
                "00060_Mean_cd": "quality_flag",
            }
        )
        data["discharge"] *= 0.3048**3  # ft3 to m3
        data["date"] = pd.to_datetime(data["datetime"]).dt.date
        return data[["site_id", "date", "discharge", "quality_flag"]]
