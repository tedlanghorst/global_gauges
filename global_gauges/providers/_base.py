from abc import ABC, abstractmethod
import asyncio
import json
from pathlib import Path
import logging

from tqdm.asyncio import tqdm
import pandas as pd
import polars as pl
import geopandas as gpd
from pydantic import ValidationError

from ..database import QualityFlag, SiteMetadata, ParquetManager

# TODO Would be nice to request multiple sites, especially when updating the database with a short timespan.
# I.e. 5000 single site queries for 7 days of data is kind of silly if we could do 50 queries for 100 sites * 7 days.
# Some providers may not support multiple sites though and we would need to differentiate. Maybe intermediate classes
# for apis that allow different types of queries? Maybe a subclass method that does the batching?

logger = logging.getLogger(__name__)

class BaseProvider(ABC):
    """
    Abstract base class for all data providers using Pydantic models.

    This class now uses Pydantic models for all data operations,
    providing type safety and clear data validation throughout.
    """

    # Subclasses must override these
    name: str = "base"
    desc: str = "Base Provider Class"
    quality_map: dict[str, QualityFlag] = {}

    # Override with True if an API key is needed to pull data.
    requires_key: bool = False

    def __init__(self, data_dir: str | Path):
        """
        Initialize provider with data directory.

        Args:
            data_dir: Base directory for all provider data
        """
        self.db_manager = ParquetManager(Path(data_dir), self.name)

    @classmethod
    def add_provider_prefix(cls, site_id: str | list[str]) -> str | list[str]:
        """Add provider prefix to site ID(s)."""
        prefix = f"{cls.name.upper()}-"

        if isinstance(site_id, list):
            return [f"{prefix}{sid}" for sid in site_id]
        return f"{prefix}{site_id}"

    @classmethod
    def remove_provider_prefix(cls, site_id: str | list[str]) -> str | list[str]:
        """Remove provider prefix from site ID(s)."""
        prefix = f"{cls.name.upper()}-"

        def _remove_prefix(sid: str) -> str:
            return sid[len(prefix) :] if sid.startswith(prefix) else sid

        if isinstance(site_id, list):
            return [_remove_prefix(sid) for sid in site_id]
        return _remove_prefix(site_id)

    def download_station_info(self, force_update: bool = False, api_key: str | None = None):
        """
        Download and store station metadata.

        Args:
            force_update: If True, re-download even if data exists
        """
        if not force_update:
            if not self.db_manager.get_site_metadata().is_empty():
                print(f"Metadata exists for {self.name}. Use force_update=True to refresh.")
                return

        print(f"Downloading station info for {self.name}...")
        raw_df = self._download_station_info(api_key)

        # Vectorized prefix addition
        raw_df["site_id"] = self.add_provider_prefix(raw_df["site_id"].tolist())

        # Validate these records. 
        # Convert to dicts once
        records = raw_df.to_dict("records")
        
        validated_models = []
        invalid_count = 0  
        for record in records:
            try:
                validated_models.append(SiteMetadata.model_validate(record))
            except ValidationError as e:
                invalid_count += 1
                logger.warning(f"Metadata validation failed: {e}")

        if validated_models:
            self.db_manager.store_site_metadata(validated_models)
            print(f"[{self.name}]: Stored {len(validated_models)} stations.")

        if invalid_count:
            print(f"[{self.name}]: Skipped {invalid_count} invalid stations.")

    @abstractmethod
    def _download_station_info(self) -> pd.DataFrame:
        """
        Provider-specific implementation to download station metadata.

        Must return DataFrame with columns:
        - site_id: Raw site identifier (without provider prefix)
        - name: Station name
        - latitude: Latitude in WGS84
        - longitude: Longitude in WGS84
        - area: Optional drainage area in km²
        - active: Optional boolean indicating if site is active
        """
        pass

    def get_station_info(self, site_ids: list[str] | None = None) -> gpd.GeoDataFrame:
        """Returns GeoDataFrame from Pydantic models (Fast enough for metadata)."""
        pl_df = self.db_manager.get_site_metadata(site_ids)

        if pl_df.is_empty():
            return None

        # Convert to Pandas for GeoPandas compatibility
        df = pl_df.to_pandas()

        # Parse provider_misc back to dict if needed, or leave as JSON string.
        df["provider_misc"] = df["provider_misc"].apply(lambda x: json.loads(x) if x else None)

        # Create Geometry
        gdf = gpd.GeoDataFrame(
            df, 
            geometry=gpd.points_from_xy(df.longitude, df.latitude), 
            crs="EPSG:4326"
        )
        
        return gdf.set_index("site_id")

    def _get_sites_to_update(
        self,
        metadata: gpd.GeoDataFrame,
        site_ids: list[str] | None,
        tolerance: int,
        force_update: bool,
    ) -> dict[str : pd.Timestamp]:
        # Use all sites if none specified
        if site_ids is None:
            site_ids = metadata.index.to_list()

        # Determine which sites need updating
        today = pd.Timestamp.now().normalize()
        default_start = pd.Timestamp(1950, 1, 1).normalize()

        to_update = {}
        for site in site_ids:
            # specific check to ensure site exists in metadata
            if site not in metadata.index:
                continue
                
            last = metadata.loc[site, "last_updated"]
            
            if force_update or pd.isna(last):
                to_update[site] = default_start
            elif (today - pd.Timestamp(last)).days > tolerance:
                to_update[site] = pd.Timestamp(last)

        if not to_update: # Pythonic empty check
            print(f"All sites for {self.name.upper()} are up-to-date.")
            return {} # Return empty dict instead of None to avoid TypeErrors later

        return to_update

    async def download_daily_values(
        self,
        site_ids: list[str] | None,
        tolerance: int,
        force_update: bool = False,
        api_key: bool | None = None,
    ):
        """
        Download daily discharge data for specified sites.

        Args:
            site_ids: Optional list of site IDs. If None, downloads for all sites.
        """
        metadata_df = self.get_station_info() # Returns GeoDataFrame
        if metadata_df is None or metadata_df.empty:
            raise ValueError("No metadata found.")

        # Determine sites to update (Logic remains similar)
        sites_to_update = self._get_sites_to_update(metadata_df, site_ids, tolerance, force_update)
        if not sites_to_update:
            return

        # This prevents us from hammering the API if we are already waiting on requests.
        semaphore = asyncio.Semaphore(10) 

        async def dl_and_process(site_id, start_date):
            async with semaphore:
                # Retrieve Pandas DataFrame from API
                df = await self._download_daily_values(
                    self.remove_provider_prefix(site_id),
                    start_date,
                    api_key,
                    metadata_df.loc[site_id].get("provider_misc")
                )

            # Update timestamp regardless of data
            self.db_manager.update_last_fetched(site_id)

            if df.empty:
                return

            try:
                pl_df = pl.from_pandas(df)

                # Add Site ID Column
                pl_df = pl_df.with_columns(pl.lit(site_id).alias("site_id"))

                # Type Enforcement
                pl_df = pl_df.select([
                    pl.col("site_id").cast(pl.Utf8),
                    pl.col("date").cast(pl.Date),
                    pl.col("discharge").cast(pl.Float64),
                    pl.col("quality_flag").cast(pl.Utf8)
                ])

                self.db_manager.store_discharge_dataframe(pl_df)
                self.db_manager.update_site_statistics(site_id)

            except Exception as e:
                logger.error(f"Error processing {site_id}: {e}")

        tasks = [dl_and_process(sid, start) for sid, start in sites_to_update.items()]
        await tqdm.gather(*tasks, desc=f"Downloading {self.name}")


    @abstractmethod
    async def _download_daily_values(self, site_id: str) -> pd.DataFrame:
        """
        Provider-specific implementation to download daily discharge data.

        Args:
            site_id: Raw site identifier (without provider prefix)

        Returns:
            DataFrame with columns:
            - date: Date of measurement
            - discharge: Discharge value in m³/s
            - quality_flag: Optional quality flag (will be mapped using quality_map)
        """
        pass

    def get_daily_data(
        self, site_ids: list[str], start_date: str = None, end_date: str = None
    ) -> pd.DataFrame:
        """
        Retrieve daily discharge data as DataFrame.

        Args:
            site_ids: List of site identifiers
            start_date: Optional start date (YYYY-MM-DD)
            end_date: Optional end date (YYYY-MM-DD)

        Returns:
            DataFrame with discharge data, indexed by (site_id, date)
        """
        pl_df = self.db_manager.get_discharge_data(site_ids, start_date, end_date)

        if pl_df.is_empty():
            return pd.DataFrame()
        
        if self.quality_map:
            pl_df = pl_df.with_columns(
                pl.col("quality_flag")
                .replace(self.quality_map, default="unknown")
            )

        df = pl_df.to_pandas()
        return df.set_index(["site_id", "date"])

    def get_database_age_days(self) -> int:
        """
        Get the age of the database in days since last update.

        Returns:
            Number of days since last update, or -1 if no data exists
        """
        metadata_df = self.get_station_info()
        if metadata_df is None:
            return -1

        most_recent = pd.Timestamp(metadata_df["last_updated"].max())
        if most_recent is pd.NaT:
            return -1
        else:
            age_days = pd.Timestamp.now().date() - most_recent.date()
            return age_days.days
