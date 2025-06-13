from abc import ABC, abstractmethod
from datetime import datetime, timezone, timedelta
from pathlib import Path

import duckdb
from tqdm.auto import tqdm
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point


class BaseProvider(ABC):
    name = "base"  # Override in subclasses to provide provider name
    quality_map = {}  # Override in subclasses to provide quality flag mappings

    """
    Abstract base class for all data providers.
    Defines the standard interface and file structure for provider implementations.
    """

    # Enforce these columns for site_metadata
    _METADATA_SCHEMA = {
        "site_id": "TEXT",
        "name": "TEXT",
        "area": "DOUBLE",
        "active": "BOOLEAN",
        "latitude": "DOUBLE",
        "longitude": "DOUBLE",
        "last_updated": "TIMESTAMP",
        "min_date": "DATE",
        "max_date": "DATE",
        "min_discharge": "DOUBLE",
        "max_discharge": "DOUBLE",
        "mean_discharge": "DOUBLE",
        "count_discharge": "BIGINT"
    }

    # Enforce these columns for discharge data
    _DISCHARGE_COLUMNS = [
        "site_id",
        "date",
        "discharge",
        "quality_flag",  # quality_flag is not always provided
    ]

    @classmethod
    def append_prefix(cls, site_id: str | list[str] | pd.Series) -> str | list[str]:
        """Appends the provider prefix to a single ID or a list of IDs."""
        if isinstance(site_id, pd.Series):
            site_id = site_id.tolist()  # Convert Series to list

        if isinstance(site_id, list):
            return [f"{cls.name.upper()}-{i}" for i in site_id]
        return f"{cls.name.upper()}-{site_id}"

    @classmethod
    def strip_prefix(cls, site_id: str | list[str] | pd.Series) -> str | list[str]:
        """Strips the provider prefix from a single ID or a list of IDs."""
        prefix = f"{cls.name.upper()}-"

        def _strip(s_id: str) -> str:
            if s_id.startswith(prefix):
                return s_id[len(prefix) :]
            return s_id

        if isinstance(site_id, pd.Series):
            site_id = site_id.tolist()  # Convert Series to list

        if isinstance(site_id, list):
            return [_strip(i) for i in site_id]
        return _strip(site_id)

    def __init__(self, data_dir: str | Path):
        """
        Initializes the provider and sets up a data directory under the package's /data dir.
        """
        self.data_dir = Path(data_dir) / self.name
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.db_path = self.data_dir / f"{self.name}.duckdb"

        self._conn = None  # Lazy connection

    def connect_to_db(self) -> duckdb.DuckDBPyConnection:
        """
        Returns an open DuckDB connection. Opens it if not already open.
        """
        if self._conn is None:
            self._conn = duckdb.connect(self.db_path)
        return self._conn

    def __del__(self):
        if self._conn:
            self._conn.close()

    def download_station_info(self, update: bool = False):
        """
        Downloads all necessary station metadata for the provider, with validation.

        If the station file already exists and update is False, downloading is skipped.
        Otherwise, calls the provider-specific _download_station_info().
        """
        conn = self.connect_to_db()
        if not update:
            try:
                result = conn.execute("SELECT COUNT(*) FROM site_metadata").fetchone()
                if result[0] > 0:
                    print("Station metadata already exists. Skipping download.")
                    print("Set update=True to force redownload.")
                    return
            except duckdb.CatalogException:
                pass  # Table doesn't exist yet
        station_df = self._download_station_info()

        # Add provider prefix to the site_id
        station_df["site_id"] = self.append_prefix(station_df["site_id"])
        self.store_station_metadata(station_df)

    @abstractmethod
    def _download_station_info(self) -> pd.DataFrame:
        """
        Provider-specific implementation to download all necessary station metadata.

        Saves a GeoJSON file at self.station_path with the following columns:
            - site_id: Unique site identifier (string)
            - active: Boolean indicating if the site is active
            - name: Site/station name (string)
            - area: Drainage area in km^2 (float, may be None)
            - geometry: Point geometry (longitude, latitude, WGS84/EPSG:4326)
        """
        pass

    def store_station_metadata(self, df: pd.DataFrame):
        """
        Utility method to insert a DataFrame into the 'site_metadata' table.
        """
        conn = self.connect_to_db()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS site_metadata (
                site_id TEXT PRIMARY KEY,
                name TEXT,
                area DOUBLE,
                active BOOLEAN,
                latitude DOUBLE,
                longitude DOUBLE,
                last_updated TIMESTAMP,
                min_date DATE,
                max_date DATE,
                min_discharge DOUBLE,
                max_discharge DOUBLE,
                mean_discharge DOUBLE,
                count_discharge BIGINT
            )
        """)

        # Ensure the DataFrame has all required metadata columns
        # Fill missing columns with None or appropriate default if necessary
        for col in self._METADATA_COLUMNS:
            if col not in df.columns:
                df[col] = None

        # Select and reorder columns to match the defined schema
        df_to_store = df[self._METADATA_COLUMNS]

        conn.register("metadata_df", df_to_store)
        conn.execute("""
            INSERT OR REPLACE INTO site_metadata
            SELECT * FROM metadata_df
        """)

    def get_station_info(self) -> gpd.GeoDataFrame:
        """Returns station metadata as a GeoDataFrame using latitude and longitude."""
        conn = self.connect_to_db()
        try:
            df = conn.execute("SELECT * FROM site_metadata").fetchdf()
            geometry = [Point(xy) for xy in zip(df.longitude, df.latitude)]
            gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
            return gdf.set_index("site_id")
        except duckdb.CatalogException:
            raise ValueError(
                f"No station metadata found for {self.name.upper()}. Run download_station_info()."
            )

    def download_daily_values(self, site_ids: list[str] = None):
        """
        Downloads daily discharge data for the given site_ids and stores it in a SQLite database.
        Handles validation and connection setup. Calls provider-specific _download_daily_values.
        """
        conn = self.connect_to_db()
        station_info = self.get_station_info()  # Fetch all metadata once

        # Default site_ids to all available if not provided
        if site_ids is None:
            site_ids = station_info.index.tolist()

        # Create a lookup for last_updated dates, converting to UTC dates
        # NaT will result for sites never updated
        last_updated = pd.to_datetime(station_info["last_updated"]).dt.date
        today = datetime.now(timezone.utc).date()

        # Prepare a list of sites that need downloading
        outdated = last_updated.get(site_ids) != today
        sites_to_update = outdated[outdated].index.to_list()

        for s_id in tqdm(sites_to_update, desc=f"{self.name.upper()}: downloading daily values"):
            daily_df = self._download_daily_values(self.strip_prefix(s_id))

            if daily_df is not None and not daily_df.empty:
                # Ensure the prefixed site_id is in the DataFrame for writing
                daily_df["site_id"] = s_id
                self.write_discharge_data(daily_df)
                self.update_discharge_metadata(s_id)
            else:
                # If no data was returned, still update the 'last_updated' timestamp
                # to record that an attempt was made today.
                conn.execute(
                    """
                    UPDATE site_metadata
                    SET last_updated = ?
                    WHERE site_id = ?
                    """,
                    (datetime.now(timezone.utc).isoformat(), s_id),
                )

    @abstractmethod
    def _download_daily_values(self, site_ids: list[str]) -> pd.DataFrame:
        """
        Provider-specific implementation to download daily discharge data for the given site_ids.

        Saves a SQLite database at self.db_path with a table named 'discharge' containing at least:
            - site_id: Unique site identifier (string)
            - date: Date of observation (datetime or string, format YYYY-MM-DD)
            - discharge: Discharge value (float, units: m^3/s)
        Optionally, additional columns such as 'quality_flag' may be included.
        """
        pass

    def write_discharge_data(self, df: pd.DataFrame):
        """
        Writes daily discharge data to DuckDB and updates metadata (min/max dates).
        Expects a DataFrame with columns: site_id, date, discharge, [quality_flag]
        """
        conn = self.connect_to_db()

        # Create table if it doesn't exist
        conn.execute("""
            CREATE TABLE IF NOT EXISTS discharge (
                site_id TEXT,
                date DATE,
                discharge DOUBLE,   
                quality_flag TEXT,
                PRIMARY KEY (site_id, date)
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_discharge_site_date ON discharge(site_id, date)"
        )

        # Select and reorder columns to match the defined schema, handling optional quality_flag
        cols_to_insert = [col for col in self._DISCHARGE_COLUMNS if col in df.columns]
        df_to_store = df[cols_to_insert]

        # Insert or replace records
        conn.register("df_temp", df_to_store)
        conn.execute(f"""
            INSERT OR REPLACE INTO discharge ({",".join(cols_to_insert)})
            SELECT {",".join(cols_to_insert)} FROM df_temp
        """)

    def update_discharge_metadata(self, site_id: str):
        """Calculates stats over the full history in the table."""
        conn = self.connect_to_db()
        stats_df = conn.execute("""
            SELECT
                site_id,
                MIN(date) as min_date,
                MAX(date) as max_date,
                MIN(discharge) as min_discharge,
                MAX(discharge) as max_discharge,
                AVG(discharge) as mean_discharge,
                COUNT(discharge) as count_discharge
            FROM discharge
            WHERE site_id = ?
            GROUP BY site_id
        """, (site_id,)).fetchdf()

        stats_row = stats_df.iloc[0]
        active_cutoff = datetime.now(timezone.utc).date() - timedelta(days=30)
        is_active = pd.to_datetime(stats_row["max_date"]).date() >= active_cutoff

        # Now put the discharge stats into the metadata table.
        conn.execute(
            """
            INSERT OR REPLACE INTO site_metadata (
                site_id, 
                min_date, max_date, 
                min_discharge, max_discharge, mean_discharge, count_discharge,
                active,
                last_updated
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                stats_row["site_id"],
                stats_row["min_date"],
                stats_row["max_date"],
                stats_row["min_discharge"],
                stats_row["max_discharge"],
                stats_row["mean_discharge"],
                stats_row["count_discharge"],
                is_active,
                datetime.now(timezone.utc).isoformat(),
            ),
        )

    def get_daily_data(
        self, sites: list[str], start_date: str = None, end_date: str = None
    ) -> pd.DataFrame:
        """
        Retrieves daily time series data for given sites.

        Parameters
        ----------
        sites : list of str
            Site identifiers to query.
        start_date : str, optional
            Start date (YYYY-MM-DD).
        end_date : str, optional
            End date (YYYY-MM-DD).

        Returns
        -------
        DataFrame
            Daily discharge data for the selected sites.
        """

        if not self.db_path.exists():
            raise FileNotFoundError("DuckDB database not found.")

        conn = self.connect_to_db()
        clauses = []
        params = []

        if sites:
            placeholders = ",".join(["?"] * len(sites))
            clauses.append(f"site_id IN ({placeholders})")
            params.extend(sites)

        if start_date:
            clauses.append("date >= ?")
            params.append(start_date)

        if end_date:
            clauses.append("date <= ?")
            params.append(end_date)

        where_clause = "WHERE " + " AND ".join(clauses) if clauses else ""
        query = f"SELECT * FROM discharge {where_clause}"

        df = conn.execute(query, params).fetchdf()

        # Normalize quality_flag using stored mapping
        if "quality_flag" in df.columns:
            df["quality_flag"] = df["quality_flag"].map(self.quality_map).fillna("unknown")

        df.set_index(["site_id", "date"], inplace=True)
        return df

    def get_last_updated(self, site_id: str) -> datetime | None:
        """
        Returns the last_updated timestamp for a given site_id from the site_metadata table.
        Returns None if no entry exists.
        """
        conn = self.connect_to_db()
        query = "SELECT last_updated FROM site_metadata WHERE site_id = ?"
        result = conn.execute(query, (site_id,)).fetchone()

        if result and result[0]:
            return pd.to_datetime(result[0])
        return None

    def set_last_updated(self, site_id: str):
        """
        Updates the last_updated timestamp for the given site_id in site_metadata table.
        Inserts a new row if it doesn't exist.
        """
        conn = self.connect_to_db()
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            """
            INSERT INTO site_metadata (site_id, last_updated)
            VALUES (?, ?)
            ON CONFLICT(site_id) DO UPDATE SET last_updated = excluded.last_updated
        """,
            (site_id, now),
        )
        conn.commit()

    def get_db_age(self) -> int:
        """
        Returns the age in days since the most recent 'last_updated' timestamp in the database.
        Returns -1 if the table or column is missing or empty.
        """
        if not self.db_path.exists():
            return -1
        try:
            conn = self.connect_to_db()
            result = conn.execute("SELECT MAX(last_updated) FROM site_metadata").fetchone()
            if not result or result[0] is None:
                return -1
            last_update = pd.to_datetime(result[0], utc=True)
            age_days = (datetime.now(timezone.utc) - last_update).days
            return age_days
        except Exception:
            # Occurs if the table or column doesn't exist
            return -1
