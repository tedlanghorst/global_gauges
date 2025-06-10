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

    def __init__(self):
        """
        Initializes the provider and sets up a data directory under the package's /data dir.
        """
        # Get the package root (one level up from this file)
        package_root = Path(__file__).parent.parent
        data_root = package_root / "data"
        data_root.mkdir(exist_ok=True)
        # Each provider gets its own subdirectory
        self.data_dir = data_root / self.name
        self.data_dir.mkdir(exist_ok=True)

    @property
    def db_path(self):
        """Standardized path to the provider's daily SQLite database."""
        return self.data_dir / f"{self.name}.sqlite3"

    @property
    def station_path(self):
        """Standardized path to the provider's site geojson database."""
        return self.data_dir / f"{self.name}.geojson"

    @property
    @abstractmethod
    def name(self) -> str:
        """The name of the provider."""
        pass

    @abstractmethod
    def download_station_info(self, update: bool = False):
        """Downloads all necessary data and files for the provider."""
        pass

    @abstractmethod
    def download_daily_values(self, site_ids: list[str], update: bool = False):
        """Downloads all necessary data and files for the provider."""
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

        try:
            conn = sqlite3.connect(self.db_path)
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
            conn.close()

            df.set_index(["site_id", "date"], inplace=True)

            return df

        except Exception as e:
            raise RuntimeError(
                f"Error loading data from SQLite. It is likely that the requested data is missing. "
                f"Original error: {e}\nTry running download_daily_values() for the sites you need."
            )
