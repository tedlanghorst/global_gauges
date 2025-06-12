import requests
from bs4 import BeautifulSoup
import re
import shutil
from tqdm.auto import tqdm

import zipfile
import io
import sqlite3
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from datetime import datetime, timezone
import tempfile
import os

from ._base import BaseProvider


class CanadaProvider(BaseProvider):
    """Data provider for the Canadian HYDAT hydrometric database."""

    name = "canada"

    # HYDAT is a single SQLite database, so we download site info and daily values together
    def _download_station_info(self) -> None:
        self._download()

    def _download_daily_values(self, site_ids: list[str]) -> None:
        self._download()

    # HYDAT is a single SQLite database, so we download site info and daily values together
    def _download(self) -> None:
        """
        Downloads the HYDAT database and extracts it, only if update is True and a newer file is available.
        After extraction, processes DLY_FLOWS into a 'discharge' table matching the standard format.
        """
        print(
            "The HYDAT database is retrieved as a zipped sqlite3 database which contains both "
            "station information and daily values. As such, you only need to call ONE of the "
            "download methods. Updating the database requires fully downloading new postings of "
            "the zipped file."
        )

        URL = "https://collaboration.cmc.ec.gc.ca/cmc/hydrometrics/www/"
        db_url, remote_date = _get_latest_url(URL, return_date=True)

        if self.db_path.exists():
            # We can only update HYDAT if they have updated the database file on their site.
            # Most providers we assume they are updated at least daily.
            local_mtime = self.db_path.stat().st_mtime
            local_date = datetime.fromtimestamp(local_mtime, tz=timezone.utc).replace(tzinfo=None)
            if remote_date is not None and local_date >= remote_date:
                print("Local HYDAT database is up to date. Skipping download.")
                return

        print(f"Downloading HYDAT database from {db_url}...")
        try:
            response = requests.get(db_url, timeout=60)
            response.raise_for_status()
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                for file_info in z.infolist():
                    filename = file_info.filename
                    if filename.endswith(".sqlite3"):
                        # Extract to a temp file
                        with tempfile.NamedTemporaryFile(delete=False) as tmp:
                            tmp_sqlite_path = tmp.name
                            with z.open(file_info) as source, open(tmp_sqlite_path, "wb") as target:
                                shutil.copyfileobj(source, target)
                        print(f"HYDAT database extracted to temp file '{tmp_sqlite_path}'")
                        self._save_station_geojson(tmp_sqlite_path)
                        self._process_dly_flows_to_discharge(tmp_sqlite_path)
                        os.remove(tmp_sqlite_path)
                        break
            return

        except requests.exceptions.RequestException as e:
            print(f"Failed to download HYDAT: {e}")
            return

    def _save_station_geojson(self, source_db_path: str) -> None:
        """Save station metadata as a GeoJSON file."""

        with sqlite3.connect(source_db_path) as conn:
            query = "SELECT * FROM STATIONS"
            df = pd.read_sql_query(query, conn)

        df["geometry"] = df.apply(lambda row: Point(row["LONGITUDE"], row["LATITUDE"]), axis=1)
        gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")

        # Now setup columns to match other sources
        gdf["source"] = self.name
        gdf["active"] = gdf["HYD_STATUS"] == "A"
        gdf = gdf.rename(
            columns={
                "STATION_NUMBER": "site_id",
                "STATION_NAME": "name",
                "DRAINAGE_AREA_GROSS": "area",
            }
        )
        gdf = gdf[["site_id", "source", "active", "name", "area", "geometry"]]

        gdf.to_file(self.station_path, driver="GeoJSON")

    def _process_dly_flows_to_discharge(self, source_db_path: str):
        """
        Process DLY_FLOWS from HYDAT and write a minimal SQLite db with only the 'discharge' table.
        Processes in chunks to avoid high memory usage.
        """
        # Read and process in chunks
        source_conn = sqlite3.connect(source_db_path)
        query = "SELECT * FROM DLY_FLOWS"
        daily_cols = [f"FLOW{d}" for d in range(1, 32)]
        first = True

        # Prepare new minimal SQLite db
        new_conn = self.connect_to_db()

        for df in tqdm(
            pd.read_sql_query(query, source_conn, chunksize=10000), desc="Processing DLY_FLOWS"
        ):
            if df.empty:
                continue
            df_long = df.melt(
                id_vars=["STATION_NUMBER", "YEAR", "MONTH"],
                value_vars=daily_cols,
                var_name="DAY",
                value_name="discharge",
            )
            df_long["DAY"] = df_long["DAY"].str.extract(r"(\d+)").astype(int)
            df_long["date"] = pd.to_datetime(
                {"year": df_long.YEAR, "month": df_long.MONTH, "day": df_long.DAY},
                errors="coerce",
            )
            df_long.dropna(subset=["date", "discharge"], inplace=True)
            df_long.rename(columns={"STATION_NUMBER": "site_id"}, inplace=True)
            discharge_df = df_long[["site_id", "date", "discharge"]].copy()
            discharge_df.to_sql(
                "discharge", new_conn, index=False, if_exists="replace" if first else "append"
            )
            first = False

        source_conn.close()


def _get_latest_url(URL, return_date=False):
    response = requests.get(URL)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    # Find all links that end in .zip and contain "sqlite"
    zip_links = [
        (link.get("href"), link.find_next_sibling("td"))
        for link in soup.find_all("a", href=True)
        if re.search(r"sqlite3.*\.zip$", link["href"], re.IGNORECASE)
    ]

    if not zip_links:
        print("No SQLite ZIP file found.")
        return (None, None) if return_date else None

    # Sort to find the latest based on name (assumes filenames have dates or versions)
    zip_links.sort(key=lambda x: x[0], reverse=True)
    latest_href, td = zip_links[0]
    print("Latest SQLite ZIP file:", latest_href)
    if len(zip_links) > 1:
        print(f"Warning! Multiple SQLite files found: {[z[0] for z in zip_links]}")

    # Try to get the 'Last modified' date from the table
    remote_date = None
    if td is not None:
        try:
            date_str = td.text.strip().split()[0]  # e.g. '2025-04-17'
            remote_date = datetime.strptime(date_str, "%Y-%m-%d")
        except Exception:
            remote_date = None

    if return_date:
        return URL + latest_href, remote_date
    else:
        return URL + latest_href
