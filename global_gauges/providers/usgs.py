import asyncio

import pandas as pd
import geopandas as gpd
import dataretrieval.nwis as nwis

from ._base import BaseProvider
from ..database import QualityFlag


class USGSProvider(BaseProvider):
    """Data provider for the USGS National Water Information System (NWIS).

    Accessed through the `dataretrieval` package by the USGS.
    https://github.com/DOI-USGS/dataretrieval-python
    """

    name = "usgs"
    # https://help.waterdata.usgs.gov/codes-and-parameters/daily-value-qualification-code-dv_rmk_cd
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

        # Get active sites (per NWIS definition)
        # A bit silly to double pull this subset but I couldn't find a way
        # to return this as a field of the larger dataset.
        active_sites_list = []
        for huc in range(1, 23):
            active, _ = nwis.get_info(huc=f"{huc:02d}", parameterCd="00060", siteStatus="active")
            active_sites_list.append(active)
        active_sites = pd.concat(active_sites_list, ignore_index=True)
        active_ids = set(active_sites["site_no"])

        # Now setup columns to match other sources
        sites = gpd.GeoDataFrame(sites).to_crs("EPSG:4326")
        sites["area"] = sites["drain_area_va"] * (1.60934**2)  # mi2 to km2
        sites["active"] = sites["site_id"].isin(active_ids)
        sites["latitude"] = sites.geometry.y
        sites["longitude"] = sites.geometry.x

        sites = sites.rename(columns={"site_no": "site_id", "station_nm": "name"})

        return sites[["site_id", "name", "area", "active", "latitude", "longitude"]]

    def _nwis_sync_get(self, site_id: str, start: pd.Timestamp) -> pd.DataFrame:
        """
        Calls the synchronous nwis.get_dv method. Have to contain it here so that
        we can wrap it in an async.to_thread call
        """
        end = pd.Timestamp.now().date()

        data, _ = nwis.get_dv(
            sites=site_id,
            start=start.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
            parameterCd="00060",
        )

        if data.empty or "00060_Mean" not in data.columns:
            return pd.DataFrame()

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

    async def _download_daily_values(
        self, site_id: str, start: pd.Timestamp, misc: dict
    ) -> pd.DataFrame:
        """Download daily discharge data for the given site_ids."""
        df = await asyncio.to_thread(self._nwis_sync_get, site_id, start)

        return df
