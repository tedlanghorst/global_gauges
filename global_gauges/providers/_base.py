from abc import ABC, abstractmethod
import asyncio
from pathlib import Path
import logging

from tqdm.asyncio import tqdm
import pandas as pd
import geopandas as gpd
from pydantic import ValidationError

from ..database import QualityFlag, SiteMetadata, DischargeRecord, DatabaseManager

# TODO Would be nice to request multiple sites, especially when updating the database with a short timespan. 
# I.e. 5000 single site queries for 7 days of data is kind of silly if we could do 50 queries for 100 sites * 7 days.
# Some providers may not support multiple sites though and we would need to differentiate. Maybe intermediate classes
# for apis that allow different types of queries. 


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

    def __init__(self, data_dir: str | Path):
        """
        Initialize provider with data directory.

        Args:
            data_dir: Base directory for all provider data
        """
        self.db_manager = DatabaseManager(Path(data_dir), self.name)

    def __del__(self):
        """Ensure database connections are properly closed."""
        self.db_manager.close_conn()

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

    def download_station_info(self, force_update: bool = False):
        """
        Download and store station metadata.

        Args:
            force_update: If True, re-download even if data exists
        """
        # Check if we already have metadata and don't need to update
        if not force_update:
            existing_metadata = self.db_manager.get_site_metadata()
            if existing_metadata:
                print(
                    f"Station metadata already exists for {self.name.upper()}. "
                    "Use force_update=True to re-download."
                )
                return

        print(f"Downloading station information for {self.name.upper()}...")

        # Call provider-specific implementation
        raw_metadata = self._download_station_info()

        # Convert DataFrame to a list of dictionaries for efficient validation
        records_to_validate = raw_metadata.to_dict("records")

        validated_models = []
        invalid_count = 0
        for record in records_to_validate:
            try:
                # Add provider prefix before validation
                record["site_id"] = self.add_provider_prefix(record["site_id"])

                # Validate the dictionary directly into a Pydantic model
                validated_model = SiteMetadata.model_validate(record)
                validated_models.append(validated_model)

            except ValidationError as e:
                # If validation fails, we log the issue and skip this row.
                invalid_count += 1
                site_identifier = record.get("site_id", "N/A")

                # Log all error details
                for error in e.errors():
                    field_path = ".".join(str(loc) for loc in error["loc"])
                    invalid_value = record
                    for loc in error["loc"]:
                        if isinstance(invalid_value, dict) and loc in invalid_value:
                            invalid_value = invalid_value[loc]
                        else:
                            invalid_value = "N/A"
                            break

                    logging.warning(
                        f"Site '{site_identifier}' failed validation on field '{field_path}' "
                        f"with value '{invalid_value}'. Reason: {error['msg']}"
                    )
                continue

        if validated_models:
            self.db_manager.store_site_metadata(validated_models)
            for metadata in validated_models:
                self.db_manager.update_site_statistics(metadata.site_id)
            print(f"Stored metadata for {len(validated_models)} stations.")

        if invalid_count > 0:
            print(
                f"Removed {invalid_count} stations due to validation errors. See log for more info."
            )

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
        """
        Get station metadata as GeoDataFrame.
        """
        metadata_list = self.db_manager.get_site_metadata(site_ids)

        if len(metadata_list) == 0:
            return None

        # Convert to GeoDataFrame
        data_for_gdf = []
        geometries = []

        for metadata in metadata_list:
            data_for_gdf.append(metadata.model_dump())
            geometries.append(metadata.get_geometry())

        df = pd.DataFrame(data_for_gdf)
        gdf = gpd.GeoDataFrame(df, geometry=geometries, crs="EPSG:4326")
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
        sites_to_update = {}

        for site in site_ids:
            if force_update:
                sites_to_update[site] = default_start
            else:
                # Check if site needs updating (no data or not updated today)
                last_updated = pd.Timestamp(metadata.loc[site]["last_updated"])
                if (last_updated is None) or (last_updated is pd.NaT):
                    sites_to_update[site] = default_start
                    continue

                days_since_update = (today - last_updated).days
                if days_since_update > tolerance:
                    sites_to_update[site] = last_updated

        if len(sites_to_update) == 0:
            print(f"All sites for {self.name.upper()} are up-to-date.")
            return

        return sites_to_update

    async def download_daily_values(
        self, site_ids: list[str] | None, tolerance: int, force_update: bool = False
    ):
        """
        Download daily discharge data for specified sites.

        Args:
            site_ids: Optional list of site IDs. If None, downloads for all sites.
        """
        metadata = self.get_station_info()
        if metadata is None:
            raise ValueError("No station metadata available. Run download_station_info() first.")

        sites_to_update = self._get_sites_to_update(metadata, site_ids, tolerance, force_update)
        if sites_to_update is None:
            return

        # This semaphore ensures only ONE _download_daily_values coroutine can run at a time.
        semaphore = asyncio.Semaphore(1)

        async def dl_and_process(site_id, start_date):
            """Worker task for a single site."""
            async with semaphore:
                # Semaphore ensures only one task can enter this block at a time.
                daily_data = await self._download_daily_values(
                    self.remove_provider_prefix(site_id),
                    start_date,
                    metadata.loc[site_id]["provider_misc"],
                )
            # Semaphore is RELEASED here. The event loop can now schedule the next download
            # while we process and insert this data into database.

            # Update last_updated timestamp regardless of whether data was found
            self._update_last_fetched(site_id)

            # We're done with this site if we found nothing.
            assert isinstance(daily_data, pd.DataFrame)  # Helps static type checker
            if daily_data.empty:
                return

            # Apply the provider-specific quality mapping.
            daily_data["quality_flag"] = (
                daily_data["quality_flag"].map(self.quality_map).fillna("unknown")
            )
            # Pre filter discharge to remove most validation errors.
            daily_data = daily_data[daily_data["discharge"] > 0]

            # Convert DataFrame to a list of dictionaries for efficient validation
            records_to_validate = daily_data.to_dict("records")

            discharge_records = []
            invalid_count = 0
            for record in records_to_validate:
                try:
                    # Use the prefixed site_id from the outer loop
                    record["site_id"] = site_id

                    # Validate the dictionary directly into a model
                    validated_record = DischargeRecord.model_validate(record)
                    discharge_records.append(validated_record)

                except ValidationError as e:
                    invalid_count += 1
                    logging.warning(
                        f"Discharge record for site '{site_id}' failed validation. "
                        f"Data: {record}. Reason: {e.errors()[0]['msg']}"
                    )
                    continue

            if discharge_records:
                self.db_manager.store_discharge_data(discharge_records)
                self.db_manager.update_site_statistics(site_id)

            if invalid_count > 0:
                warning_str = (
                    f"Removed {invalid_count} entries from {site_id} due to validation errors."
                )
                logging.warning(warning_str)

        # Create and run all tasks concurrently (again, semaphore prevents crushing the provider API)
        tasks = [dl_and_process(site_id, start) for site_id, start in sites_to_update.items()]
        await tqdm.gather(*tasks, desc=f"{self.name.upper()}: processing sites")

    def _update_last_fetched(self, site_id: str):
        """Update the last_updated timestamp for a site."""
        conn = self.db_manager.get_conn(write=True)
        conn.execute(
            """
            UPDATE site_metadata 
            SET last_updated = ? 
            WHERE site_id = ?
        """,
            (pd.Timestamp.now().date().isoformat(), site_id),
        )
        self.db_manager.close_conn()

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
        records = self.db_manager.get_discharge_data(site_ids, start_date, end_date)

        if not records:
            return pd.DataFrame()

        # Convert to DataFrame
        data_list = []
        for record in records:
            data_list.append(
                {
                    "site_id": record.site_id,
                    "date": record.date,
                    "discharge": record.discharge,
                    "quality_flag": record.quality_flag,
                }
            )

        df = pd.DataFrame(data_list)
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
