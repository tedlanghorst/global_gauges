from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any, Union
from enum import Enum

import duckdb
import pandas as pd
from shapely.geometry import Point
from pydantic import BaseModel, Field, validator, ConfigDict


class QualityFlag(str, Enum):
    """Enumeration of possible data quality flags."""
    GOOD = "good" 
    PROVISIONAL = "provisional"
    SUSPECT = "suspect"
    BAD = "bad"


class SiteMetadata(BaseModel):
    """
    Pydantic model representing site metadata in the database.
    
    This model ensures type safety and validation for all site metadata operations.
    All database interactions should use this model for consistency.
    """
    model_config = ConfigDict(
        # Allow extra fields for provider-specific metadata
        extra='allow',
        # Use enum values instead of enum names when serializing
        use_enum_values=True,
        # Validate on assignment to catch errors early
        validate_assignment=True
    )
    
    site_id: str = Field(..., description="Unique site identifier with provider prefix")
    name: str = Field(..., description="Human-readable site/station name")
    area: Optional[float] = Field(None, description="Drainage area in km²", ge=0)
    active: bool = Field(False, description="Whether the site is currently active")
    latitude: float = Field(..., description="Latitude in WGS84", ge=-90, le=90)
    longitude: float = Field(..., description="Longitude in WGS84", ge=-180, le=180)
    
    # Timestamps and data range information
    last_updated: Optional[datetime] = Field(None, description="When data was last fetched")
    min_date: Optional[datetime] = Field(None, description="Earliest data date available")
    max_date: Optional[datetime] = Field(None, description="Latest data date available")
    
    # Discharge statistics
    min_discharge: Optional[float] = Field(None, description="Minimum discharge (m³/s)", ge=0)
    max_discharge: Optional[float] = Field(None, description="Maximum discharge (m³/s)", ge=0)
    mean_discharge: Optional[float] = Field(None, description="Mean discharge (m³/s)", ge=0)
    count_discharge: Optional[int] = Field(None, description="Number of discharge records", ge=0)
    
    @validator('min_discharge', 'max_discharge', 'mean_discharge')
    def validate_positive_discharge(cls, v):
        """Ensure discharge values are non-negative."""
        if v is not None and v < 0:
            raise ValueError("Discharge values must be non-negative")
        return v
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database operations."""
        return self.dict(exclude_none=False)
    
    def get_geometry(self) -> Point:
        """Create a Shapely Point geometry from coordinates."""
        return Point(self.longitude, self.latitude)


class DischargeRecord(BaseModel):
    """
    Pydantic model representing a single discharge measurement.
    
    This model ensures all discharge data follows the same structure
    and provides validation for data quality.
    """
    model_config = ConfigDict(
        use_enum_values=True,
        validate_assignment=True
    )
    
    site_id: str = Field(..., description="Site identifier (with provider prefix)")
    date: datetime = Field(..., description="Date of measurement")
    discharge: float = Field(..., description="Discharge value in m³/s", ge=0)
    quality_flag: Optional[QualityFlag] = Field(
        QualityFlag.UNKNOWN, 
        description="Data quality indicator"
    )
    
    @validator('discharge')
    def validate_discharge_positive(cls, v):
        """Ensure discharge is non-negative."""
        if v < 0:
            raise ValueError("Discharge must be non-negative")
        return v
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database operations."""
        data = self.dict()
        # Ensure date is properly formatted for database
        data['date'] = self.date.strftime('%Y-%m-%d')
        return data


class DatabaseConfig(BaseModel):
    """Configuration for database operations."""
    model_config = ConfigDict(validate_assignment=True)
    
    provider_name: str = Field(..., description="Name of the data provider")
    data_directory: Path = Field(..., description="Base data directory")
    
    @property
    def provider_data_dir(self) -> Path:
        """Get the provider-specific data directory."""
        return self.data_directory / self.provider_name
    
    @property 
    def database_path(self) -> Path:
        """Get the path to the DuckDB database file."""
        return self.provider_data_dir / f"{self.provider_name}.duckdb"



# =============================================================================
# DATABASE OPERATIONS CLASS
# =============================================================================

class DatabaseManager:
    """
    Handles all database operations using Pydantic models.
    
    This class encapsulates all DuckDB operations and ensures
    consistent use of Pydantic models throughout.
    """
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self._connection: Optional[duckdb.DuckDBPyConnection] = None
        
        # Create data directory
        self.config.provider_data_dir.mkdir(parents=True, exist_ok=True)
    
    def get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get database connection, creating if necessary."""
        if self._connection is None:
            self._connection = duckdb.connect(str(self.config.database_path))
            self._initialize_tables()
        return self._connection
    
    def close(self):
        """Close database connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
    
    def __del__(self):
        """Ensure connection is closed when object is destroyed."""
        self.close()
    
    def _initialize_tables(self):
        """Create database tables if they don't exist."""
        conn = self.get_connection()
        
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
                count_discharge BIGINT
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
    
    def store_site_metadata(self, metadata: Union[SiteMetadata, List[SiteMetadata]]):
        """
        Store site metadata in database.
        
        Args:
            metadata: Single SiteMetadata object or list of SiteMetadata objects
        """
        conn = self.get_connection()
        
        if isinstance(metadata, SiteMetadata):
            metadata = [metadata]
        
        # Convert Pydantic models to DataFrames for bulk insert
        data_dicts = [site.to_dict() for site in metadata]
        df = pd.DataFrame(data_dicts)
        
        # Register DataFrame and insert
        conn.register("metadata_temp", df)
        conn.execute("""
            INSERT OR REPLACE INTO site_metadata
            SELECT * FROM metadata_temp
        """)
    
    def get_site_metadata(self, site_ids: Optional[List[str]] = None) -> List[SiteMetadata]:
        """
        Retrieve site metadata from database.
        
        Args:
            site_ids: Optional list of site IDs to filter by
            
        Returns:
            List of SiteMetadata objects
        """
        conn = self.get_connection()
        
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
            metadata_list.append(SiteMetadata(**row_dict))
        
        return metadata_list
    
    def store_discharge_data(self, records: Union[DischargeRecord, List[DischargeRecord]]):
        """
        Store discharge records in database.
        
        Args:
            records: Single DischargeRecord or list of DischargeRecord objects
        """
        conn = self.get_connection()
        
        if isinstance(records, DischargeRecord):
            records = [records]
        
        # Convert to DataFrame for bulk insert
        data_dicts = [record.to_dict() for record in records]
        df = pd.DataFrame(data_dicts)
        
        # Register and insert
        conn.register("discharge_temp", df)
        conn.execute("""
            INSERT OR REPLACE INTO discharge
            SELECT * FROM discharge_temp
        """)
    
    def get_discharge_data(
        self, 
        site_ids: List[str], 
        start_date: Optional[str] = None, 
        end_date: Optional[str] = None
    ) -> List[DischargeRecord]:
        """
        Retrieve discharge data from database.
        
        Args:
            site_ids: List of site IDs to query
            start_date: Optional start date (YYYY-MM-DD)
            end_date: Optional end date (YYYY-MM-DD)
            
        Returns:
            List of DischargeRecord objects
        """
        conn = self.get_connection()
        
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
            if row_dict['date']:
                row_dict['date'] = pd.to_datetime(row_dict['date'])
            records.append(DischargeRecord(**row_dict))
        
        return records
    
    def update_site_statistics(self, site_id: str):
        """
        Calculate and update discharge statistics for a site.
        
        Args:
            site_id: Site identifier to update statistics for
        """
        conn = self.get_connection()
        
        # Calculate statistics from discharge data
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
        
        if stats_df.empty:
            return
        
        stats_row = stats_df.iloc[0]
        
        # Determine if site is active (has data within last 30 days)
        active_cutoff = datetime.now(timezone.utc).date() - timedelta(days=30)
        is_active = pd.to_datetime(stats_row["max_date"]).date() >= active_cutoff
        
        # Update metadata table
        conn.execute("""
            UPDATE site_metadata 
            SET 
                min_date = ?,
                max_date = ?,
                min_discharge = ?,
                max_discharge = ?,
                mean_discharge = ?,
                count_discharge = ?,
                active = ?,
                last_updated = ?
            WHERE site_id = ?
        """, (
            stats_row["min_date"],
            stats_row["max_date"],
            stats_row["min_discharge"],
            stats_row["max_discharge"],
            stats_row["mean_discharge"],
            stats_row["count_discharge"],
            is_active,
            datetime.now(timezone.utc).isoformat(),
            site_id
        ))
