import requests
from datetime import datetime

from shapely.geometry import Point
from tqdm.auto import tqdm
import pandas as pd
import geopandas as gpd

from ._base import BaseProvider

"""
API Reference
https://environment.data.gov.uk/hydrology/doc/reference
"""


class UKEnvironmentAgencyProvider(BaseProvider):
    name = "ukea"

    def _download_station_info(self):
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
                "geometry": Point(d["long"], d["lat"]),
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

        stations = gpd.GeoDataFrame(all_data)
        stations["source"] = self.name.upper()
        stations = stations.set_crs("EPSG:4326")
        stations.to_file(self.station_path, driver="GeoJSON")

    def _download_daily_values(self, site_ids, conn):
        # Get all site_ids that already exist in the database
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

            data = download_site_dv(site_id, start_date, end_date)
            data.to_sql("discharge", conn, if_exists="append", index=False)


def download_site_dv(site_id: str, start_date: str, end_date) -> pd.DataFrame:
    BASE_URL = "http://environment.data.gov.uk/hydrology/data/readings.json"

    params = {
        "station.wiskiID": site_id,
        "observedProperty": "waterFlow",
        "period": 86400,
        "mineq-date": start_date.strftime("%Y-%m-%d"),
        "maxeq-date": end_date.strftime("%Y-%m-%d"),
    }

    session = requests.Session()
    response = session.get(BASE_URL, params=params)
    items = response.json().get("items", [])
    if not items:
        # Return empty DataFrame with correct columns if no data
        return pd.DataFrame(columns=["site_id", "date", "discharge"])

    df = pd.DataFrame(items)
    df.rename(columns={"value": "discharge"}, inplace=True)
    df["site_id"] = site_id
    return df[["site_id", "date", "discharge"]]
