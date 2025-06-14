from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
import sqlite3

import pandas as pd
import geopandas as gpd


class BaseProvider(ABC):
    """
    Abstract base class for all data providers.
    Defines the standard interface and file structure for provider implementations.
    """

    name = "base"

    def __init__(self, data_dir: str | Path):
        """
        Initializes the provider and sets up a data directory under the package's /data dir.
        """
        self.data_dir = Path(data_dir) / self.name
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.db_path = self.data_dir / f"{self.name}.sqlite3"
        self.station_path = self.data_dir / f"{self.name}.geojson"

        self._conn = None  # Lazy connection

    def connect_to_db(self):
        """
        Returns an open SQLite connection. Opens it if not already open.
        """
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path)
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
        if self.station_path.exists() and not update:
            print(f"Station info already exists at {self.station_path}. Skipping download.")
            print("Set update=True if you want to redownload station data.")
            return
        self._download_station_info()

    @abstractmethod
    def _download_station_info(self):
        """
        Provider-specific implementation to download all necessary station metadata.

        Saves a GeoJSON file at self.station_path with the following columns:
            - site_id: Unique site identifier (string)
            - source: Provider/source name (string)
            - active: Boolean indicating if the site is active
            - name: Site/station name (string)
            - area: Drainage area in km^2 (float, may be None)
            - geometry: Point geometry (longitude, latitude, WGS84/EPSG:4326)
        """
        pass

    def download_daily_values(self, site_ids: list[str] = None, update: bool = False):
        """
        Downloads daily discharge data for the given site_ids and stores it in a SQLite database.
        Handles validation and connection setup. Calls provider-specific _download_daily_values.
        """
        # Default site_ids to all available if not provided
        if not site_ids:  # None or []
            site_ids = self.get_station_info().index.tolist()

        # If DB does not exist or update is requested, proceed
        if not self.db_path.exists() or update:
            self._download_daily_values(site_ids)
        else:
            print(
                f"Database already exists at {self.db_path}. Skipping download. Use update=True to force update."
            )

    @abstractmethod
    def _download_daily_values(self, site_ids: list[str]):
        """
        Provider-specific implementation to download daily discharge data for the given site_ids.

        Saves a SQLite database at self.db_path with a table named 'discharge' containing at least:
            - site_id: Unique site identifier (string)
            - date: Date of observation (datetime or string, format YYYY-MM-DD)
            - discharge: Discharge value (float, units: m^3/s)
        Optionally, additional columns such as 'quality_flag' may be included.
        """
        pass

    def get_db_age(self) -> int:
        """
        Returns the age of the SQLite database in days.

        Returns
        -------
        int
            Age in days, or -1 if the database does not exist.
        """
        if not self.db_path.exists():
            return -1
        try:
            local_mtime = self.db_path.stat().st_mtime
            local_date = datetime.fromtimestamp(local_mtime, tz=timezone.utc)
            age_days = (datetime.now(timezone.utc) - local_date).days
            return age_days
        except Exception:
            print(f"Warning: Could not determine {self.name.upper()} database age.")
            return -1

    def get_station_info(self) -> pd.DataFrame:
        """
        Loads station metadata from standardized stations.geojson as a DataFrame indexed by site_id.

        Returns
        -------
        DataFrame
            Station metadata indexed by site_id.
        """
        if not self.station_path.is_file():
            raise FileNotFoundError(
                "Station info not found. Run download() or download_station_info() first."
            )

        return gpd.read_file(self.station_path).set_index("site_id")

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
            raise FileNotFoundError(
                f"SQLite database not found at {self.db_path}. Run download() or download_daily_values() first."
            )
        conn = self.connect_to_db()

        try:
            query = "SELECT * FROM discharge"
            clauses = []
            params = []
            if sites is not None:
                placeholders = ",".join(["?"] * len(sites))
                clauses.append(f"site_id IN ({placeholders})")
                params.extend(sites)
            if start_date:
                clauses.append("date >= ?")
                params.append(start_date)
            if end_date:
                clauses.append("date <= ?")
                params.append(end_date)
            if clauses:
                query += " WHERE " + " AND ".join(clauses)

            df = pd.read_sql_query(query, conn, parse_dates=["date"], params=params)
            df.set_index(["site_id", "date"], inplace=True)

            return df

        except Exception as e:
            raise RuntimeError(
                f"Error loading data from SQLite. It is likely that the requested data is missing. "
                f"Original error: {e}\nTry running download_daily_values() for the sites you need."
            )

    def get_last_entry_date(self, conn: sqlite3.Connection, site_id: str):
        """
        Returns the latest date for a given site_id from the specified table.
        Returns None if no entry exists.
        """
        query = "SELECT MAX(date) FROM discharge WHERE site_id=?"
        try:
            result = conn.execute(query, (site_id,)).fetchone()
            last_date = pd.to_datetime(result[0]) if result and result[0] else None
        except sqlite3.DatabaseError:
            # If the db is empty. Returning None indicates that all data needs downloaded.
            last_date = None

        return last_date
