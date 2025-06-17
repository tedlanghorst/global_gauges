import requests

import pandas as pd
import aiohttp

from ._base import BaseProvider
from ..database import QualityFlag


class UKEAProvider(BaseProvider):
    """
    Data provider for the UK Environment Agency's hydrology data.

    API reference:
    https://environment.data.gov.uk/hydrology/doc/reference
    """

    name = "ukea"
    quality_map = {
        "Good": QualityFlag.GOOD,
        "Estimated": QualityFlag.ESTIMATED,
        "Suspect": QualityFlag.SUSPECT,
        "Unchecked": QualityFlag.PROVISIONAL,
        "Missing": QualityFlag.BAD,
    }

    def _download_station_info(self) -> pd.DataFrame:
        BASE_URL = "http://environment.data.gov.uk/hydrology/id/stations.json"
        LIMIT = 100

        def process_response(d):
            site_id = d["wiskiID"]
            if isinstance(site_id, list):
                site_id = site_id[0]  # Get the first GUID.

            d_out = {
                "site_id": site_id,
                "name": d["label"],
                "active": d["status"][0]["label"].lower() == "active",
                "longitude": d["long"],
                "latitude": d["lat"],
                "provider_misc": {"guid": d["stationGuid"]},
            }
            return d_out

        session = requests.Session()
        params = {"observedProperty": "waterFlow", "_limit": LIMIT, "_offset": 0}
        response = session.get(BASE_URL, params=params)

        all_data = []
        while (response.status_code == 200) and response.json()["items"]:
            all_data.extend([process_response(d) for d in response.json()["items"]])
            params["_offset"] += LIMIT
            response = session.get(BASE_URL, params=params)

        return pd.DataFrame(all_data)

    async def _download_daily_values(
        self, site_id: str, start: pd.Timestamp, misc: dict
    ) -> pd.DataFrame:
        end = pd.Timestamp.now()

        guid = misc["guid"]
        guid = guid[0] if isinstance(guid, list) else guid

        base_url = "http://environment.data.gov.uk/hydrology/id/measures/"
        site_url = base_url + guid + "-flow-m-86400-m3s-qualified/readings"
        params = {
            "mineq-date": start.strftime("%Y-%m-%d"),
            "maxeq-date": end.strftime("%Y-%m-%d"),
            "_limit": 100000,  # 273 years should be fine... Need to paginate if we do multiple sites.
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(site_url, params=params) as response:
                response.raise_for_status()
                json_data = await response.json()

                df = pd.DataFrame(json_data["items"])

                if df.empty:
                    return pd.DataFrame()

                df.rename(columns={"value": "discharge", "quality": "quality_flag"}, inplace=True)
                df["site_id"] = site_id

                return df[["site_id", "date", "discharge", "quality_flag"]]
