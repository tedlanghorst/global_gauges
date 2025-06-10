import requests
import sqlite3
from datetime import datetime

from shapely.geometry import Point
from tqdm.auto import tqdm
import pandas as pd
import geopandas as gpd

from .base_provider import BaseProvider

"""
API Reference
https://environment.data.gov.uk/hydrology/doc/reference
"""


class UKEnvironmentAgencyProvider(BaseProvider):
    name = "ukea"

    def download_station_info(self, update=False):
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

    def download_daily_values(self, site_ids, update=False):
        print("test1")
        if site_ids is None:
            sites = self.get_station_info()
            site_ids = sites.index.tolist()

        # Get all site_ids that already exist in the database
        conn = sqlite3.connect(self.db_path)
        try:
            existing_site_ids = set(
                row[0] for row in conn.execute("SELECT DISTINCT site_id FROM discharge").fetchall()
            )
        except sqlite3.OperationalError:
            existing_site_ids = set()

        print(len(existing_site_ids))

        end_date = datetime.now().strftime("%Y-%m-%d")
        for site_id in tqdm(site_ids):
            start_date = "1950-01-01"

            if site_id in existing_site_ids:
                if not update:
                    # If not updating, skip this site
                    continue
                else:
                    # If updating, find the latest date in the database and set start_date to the next day
                    result = conn.execute(
                        "SELECT MAX(date) FROM discharge WHERE site_id=?", (site_id,)
                    ).fetchone()
                    latest_date = result[0]
                    start_date = (pd.to_datetime(latest_date) + pd.Timedelta(days=1)).strftime(
                        "%Y-%m-%d"
                    )
                    if start_date > end_date:
                        continue  # Already up to date

            data = download_site_dv(site_id, start_date, end_date)
            data.to_sql("discharge", conn, if_exists="append", index=False)

        conn.close()


def download_site_dv(site_id: str, start_date: str, end_date) -> pd.DataFrame:
    BASE_URL = "http://environment.data.gov.uk/hydrology/data/readings.json"

    params = {
        "station.wiskiID": site_id,
        "observedProperty": "waterFlow",
        "period": 86400,
        "mineq-date": start_date,
        "maxeq-date": end_date,
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
