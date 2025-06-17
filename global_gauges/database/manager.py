import json
from pathlib import Path

# from functools import wraps
from typing import Optional

import duckdb
import pandas as pd

from .models import SiteMetadata, DischargeRecord

# def with_connection(write: bool = False):
#     """
#     A decorator to manage database connections for a method.

#     Opens a connection before the method is called and ensures it's closed
#     afterward. It passes the connection object as a 'conn' keyword
#     argument to the decorated method.
#     """
#     def decorator(func: Callable) -> Callable:
#         @wraps(func)
#         def wrapper(self: 'DuckDBManager', *args: Any, **kwargs: Any) -> Any:
#             # Get the appropriate connection
#             conn = self.get_conn(write=write)
#             try:
#                 # Call the original method, passing the connection in
#                 result = func(self, *args, conn=conn, **kwargs)
#                 return result
#             finally:
#                 # Always close the connection
#                 self.close_conn()
#         return wrapper
#     return decorator


class DatabaseManager:
    """
    Handles all database operations using Pydantic models.

    This class encapsulates all DuckDB operations and ensures
    consistent use of Pydantic models throughout.
    """

    def __init__(self, data_dir: Path, provider_name: str):
        # Create data directory
        provider_data_dir = data_dir / provider_name
        provider_data_dir.mkdir(parents=True, exist_ok=True)

        self.database_path = provider_data_dir / f"{provider_name}.duckdb"

        self._write_conn: Optional[duckdb.DuckDBPyConnection] = None
        self._read_conn: Optional[duckdb.DuckDBPyConnection] = None

        self._initialize_tables()

    def get_conn(self, write=True) -> duckdb.DuckDBPyConnection:
        """Get database connection, creating if necessary."""
        if write:
            if self._write_conn:
                return self._write_conn
            else:
                self.close_conn()  # In case _read_conn is open
                self._write_conn = duckdb.connect(self.database_path)
                return self._write_conn
        else:
            if self._read_conn:
                return self._read_conn
            else:
                self.close_conn()  # In case _write_conn is open
                self._read_conn = duckdb.connect(self.database_path, read_only=True)
                return self._read_conn

    def close_conn(self):
        """Close database connection(s)."""
        if self._write_conn:
            self._write_conn.close()
            self._write_conn = None

        if self._read_conn:
            self._read_conn.close()
            self._read_conn = None

    def __del__(self):
        """Ensure connection is closed when object is destroyed."""
        self.close_conn()

    def _initialize_tables(self):
        """Create database tables if they don't exist."""
        conn = self.get_conn(write=True)

        # Create site_metadata table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS site_metadata (
                site_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                area DOUBLE,
                active BOOLEAN NOT NULL DEFAULT TRUE,
                latitude DOUBLE NOT NULL,
                longitude DOUBLE NOT NULL,
                last_updated TIMESTAMP,
                min_date DATE,
                max_date DATE,
                min_discharge DOUBLE,
                max_discharge DOUBLE,
                mean_discharge DOUBLE,
                count_discharge BIGINT,
                provider_misc JSON
            )
        """)

        # Create discharge table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS discharge (
                site_id TEXT NOT NULL,
                date DATE NOT NULL,
                discharge DOUBLE NOT NULL,   
                quality_flag TEXT,
                PRIMARY KEY (site_id, date)
            )
        """)

        # Create indexes for better performance
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_discharge_site_date 
            ON discharge(site_id, date)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_metadata_active 
            ON site_metadata(active)
        """)

        self.close_conn()

    def store_site_metadata(self, metadata: SiteMetadata | list[SiteMetadata]):
        """
        Store site metadata in database.

        Args:
            metadata: Single SiteMetadata object or list of SiteMetadata objects
        """
        conn = self.get_conn(write=True)

        if isinstance(metadata, SiteMetadata):
            metadata = [metadata]

        # Convert Pydantic models to DataFrames for bulk insert
        data_dicts = [site.model_dump() for site in metadata]
        df = pd.DataFrame(data_dicts)

        # DuckDB requires a proper JSON string, not a Python dict's string representation.
        if "provider_misc" in df.columns:
            df["provider_misc"] = df["provider_misc"].apply(
                lambda x: json.dumps(x) if isinstance(x, dict) else None
            )

        # Register DataFrame and insert
        conn.register("metadata_temp", df)
        conn.execute("""
            INSERT OR REPLACE INTO site_metadata
            SELECT * FROM metadata_temp
        """)

        self.close_conn()

    def get_site_metadata(self, site_ids: Optional[list[str]] = None) -> list[SiteMetadata]:
        """
        Retrieve site metadata from database.

        Args:
            site_ids: Optional list of site IDs to filter by

        Returns:
            List of SiteMetadata objects
        """
        conn = self.get_conn()

        if site_ids:
            placeholders = ",".join(["?"] * len(site_ids))
            query = f"SELECT * FROM site_metadata WHERE site_id IN ({placeholders})"
            df = conn.execute(query, site_ids).fetchdf()
        else:
            df = conn.execute("SELECT * FROM site_metadata").fetchdf()

        # Convert DataFrame rows to Pydantic models
        metadata_list = []
        for _, row in df.iterrows():
            # Convert row to dict and handle NaN values
            row_dict = row.to_dict()
            row_dict = {k: (None if pd.isna(v) else v) for k, v in row_dict.items()}

            # Parse the provider misc json data
            if "provider_misc" in row_dict and isinstance(row_dict["provider_misc"], str):
                try:
                    row_dict["provider_misc"] = json.loads(row_dict["provider_misc"])
                except json.JSONDecodeError:
                    # Handle cases where the string is not valid JSON
                    row_dict["provider_misc"] = None

            metadata_list.append(SiteMetadata(**row_dict))

        self.close_conn()

        return metadata_list

    def store_discharge_data(self, records: DischargeRecord | list[DischargeRecord]):
        """
        Store discharge records in database.

        Args:
            records: Single DischargeRecord or list of DischargeRecord objects
        """
        conn = self.get_conn(write=True)

        if isinstance(records, DischargeRecord):
            records = [records]

        # Convert to DataFrame for bulk insert
        data_dicts = [record.model_dump() for record in records]
        df = pd.DataFrame(data_dicts)

        # Register and insert
        conn.register("discharge_temp", df)
        conn.execute("""
            INSERT OR REPLACE INTO discharge
            SELECT * FROM discharge_temp
        """)

        self.close_conn()

    def get_discharge_data(
        self, site_ids: list[str], start_date: Optional[str] = None, end_date: Optional[str] = None
    ) -> list[DischargeRecord]:
        """
        Retrieve discharge data from database.

        Args:
            site_ids: List of site IDs to query
            start_date: Optional start date (YYYY-MM-DD)
            end_date: Optional end date (YYYY-MM-DD)

        Returns:
            List of DischargeRecord objects
        """
        conn = self.get_conn()

        # Build query with optional date filters
        clauses = []
        params = []

        if site_ids:
            placeholders = ",".join(["?"] * len(site_ids))
            clauses.append(f"site_id IN ({placeholders})")
            params.extend(site_ids)

        if start_date:
            clauses.append("date >= ?")
            params.append(start_date)

        if end_date:
            clauses.append("date <= ?")
            params.append(end_date)

        where_clause = "WHERE " + " AND ".join(clauses) if clauses else ""
        query = f"SELECT * FROM discharge {where_clause} ORDER BY site_id, date"

        df = conn.execute(query, params).fetchdf()

        # Convert to Pydantic models
        records = []
        for _, row in df.iterrows():
            row_dict = row.to_dict()
            # Handle NaN values and ensure proper date parsing
            row_dict = {k: (None if pd.isna(v) else v) for k, v in row_dict.items()}
            if row_dict["date"]:
                row_dict["date"] = pd.to_datetime(row_dict["date"])
            records.append(DischargeRecord(**row_dict))

        self.close_conn()

        return records

    def update_site_statistics(self, site_id: str):
        """
        Calculate and update discharge statistics for a site.

        Args:
            site_id: Site identifier to update statistics for
        """
        conn = self.get_conn(write=True)

        # Calculate statistics from discharge data
        stats_df = conn.execute(
            """
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
        """,
            (site_id,),
        ).fetchdf()

        if stats_df.empty:
            self.close_conn()
            return

        stats_row = stats_df.iloc[0]

        # Update metadata table
        conn.execute(
            """
            UPDATE site_metadata 
            SET 
                min_date = ?,
                max_date = ?,
                min_discharge = ?,
                max_discharge = ?,
                mean_discharge = ?,
                count_discharge = ?,
                last_updated = ?
            WHERE site_id = ?
        """,
            (
                stats_row["min_date"],
                stats_row["max_date"],
                float(stats_row["min_discharge"]),
                float(stats_row["max_discharge"]),
                float(stats_row["mean_discharge"]),
                int(stats_row["count_discharge"]),
                pd.Timestamp.now().date().isoformat(),
                site_id,
            ),
        )

        self.close_conn()
