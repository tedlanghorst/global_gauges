import requests
import asyncio

import pandas as pd
import aiohttp

from ._base import BaseProvider
from ..database import QualityFlag  # noqa: F401


class KrWAMISProvider(BaseProvider):
    name = "krwamis"  # Unique string identifier for your provider
    desc = "Korean WAter Management Information System"  # Short description of source.
    quality_map = {1: QualityFlag.GOOD}

    """
    API key is free and relatively easy to attain by creating an account for the OpenAPI at http://www.wamis.go.kr.
    There is an English version of the WAMIS website, but I could not find equivalent account options there. 
    """
    requires_key: bool = True

    def _download_station_info(self, api_key: str | None) -> pd.DataFrame:
        # First we just get a list of all discharge sites.
        # Interestingly, changing 'oper' param to 'n' does not currently change output.
        url = "http://www.wamis.go.kr:8080/wamis/openapi/wkw/flw_dubobsif"
        params = {
            "key": api_key,
            "oper": "y",
            "output": "json",
        }
        response = requests.get(url, params)
        site_ids = [d["obscd"] for d in response.json()["list"]]

        # Not sure it's possible to request all at once, seems to require the first query
        # for a list of sites.
        url = "http://www.wamis.go.kr:8080/wamis/openapi/wkw/wl_obsinfo"

        site_info_list = []
        for site_id in site_ids:
            params = {
                "key": api_key,
                "obscd": site_id,
                "output": "json",
            }
            response = requests.get(url, params)
            if response.json()["count"] == 0:
                # Very few stations return "해당 데이터가 없습니다", translated as "there is no such data"
                # Not sure why when the first request gave us these IDs.
                continue

            raw_info = response.json()["list"][0]
            river_name = raw_info["rivnm"] if raw_info["rivnm"] else "N/A"
            station_name = raw_info["obsnm"] if raw_info["obsnm"] else "N/A"
            area = float(raw_info["bsnara"]) if raw_info["bsnara"] else None
            lat = dms_to_dd(raw_info["lat"])
            lon = dms_to_dd(raw_info["lon"])

            if lat is None or lon is None:
                # skip stations without coordinates
                continue

            site_info_list.append(
                {
                    "site_id": site_id,
                    "name": river_name + station_name,
                    "latitude": lat,
                    "longitude": lon,
                    "area": area,
                    "active": True,  # Specified in first request for sites
                }
            )

        return pd.DataFrame(site_info_list)

    async def _download_daily_values(
        self, site_id: str, start: pd.Timestamp, api_key: str | None, misc: dict
    ) -> pd.DataFrame:
        url = "http://www.wamis.go.kr:8080/wamis/openapi/wkw/flw_dtdata"
        params = {"key": api_key, "obscd": site_id, "output": "json"}

        df_list = []
        async with aiohttp.ClientSession() as session:
            for year in range(start.year, pd.Timestamp.now().year + 1):
                params["year"] = year
                async with session.get(url, params=params) as response:
                    data = await response.json()

                    if data.get("count", 0) == 0:
                        # No data
                        continue

                    yr_df = pd.DataFrame(data["list"])

                    # Gracefully handle malformed date strings
                    yr_df["ymd"] = pd.to_datetime(
                        yr_df["ymd"].astype(str).str.strip(),
                        format="%Y%m%d",
                        errors="coerce",  # invalid dates become NaT
                    )
                    # Convert flow to numeric safely
                    yr_df["fw"] = pd.to_numeric(yr_df["fw"], errors="coerce")

                    # Drop rows with invalid or missing dates or flow values
                    yr_df = yr_df.dropna(subset=["ymd", "fw"])

                    if yr_df.empty:
                        continue

                    yr_df.rename(columns={"ymd": "date", "fw": "discharge"}, inplace=True)
                    df_list.append(yr_df)

        if not df_list:
            # No valid data for any year
            return pd.DataFrame(columns=["date", "discharge", "quality_flag"])

        df = pd.concat(df_list).sort_values(by="date")
        df = df[df["date"] >= start]
        df["quality_flag"] = 1  # Does not return a flag.

        return df


def dms_to_dd(dms_str: str) -> float:
    """Convert DMS string like '128-33-04' to decimal degrees."""
    # Remove spaces and split by dash or other separators
    if dms_str is None:
        return

    parts = dms_str.strip().replace(" ", "").split("-")
    dms_arr = pd.to_numeric(parts)
    if len(dms_arr) == 3:
        dd = dms_arr[0] + dms_arr[1] / 60 + dms_arr[2] / 3600
        return dd
