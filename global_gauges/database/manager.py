import json
import logging
from pathlib import Path

import polars as pl
import pandas as pd 
from .models import SiteMetadata

logger = logging.getLogger(__name__)


class ParquetManager:
    """
    Handles database operations using Polars and Partitioned Parquet.
    """

    def __init__(self, data_dir: Path, provider_name: str):
        self.base_path = data_dir / provider_name
        self.metadata_path = self.base_path / "site_metadata" / "data.parquet"
        self.discharge_path = self.base_path / "discharge"

        # Ensure directories exist
        self.metadata_path.parent.mkdir(parents=True, exist_ok=True)
        self.discharge_path.mkdir(parents=True, exist_ok=True)

    # ==========================================
    # METADATA OPERATIONS
    # ==========================================

    def store_site_metadata(self, metadata: list[SiteMetadata]):
        """
        Upserts metadata. Reads existing, filters out IDs being updated, appends new, writes back.
        """
        if not metadata:
            return

        # Serialize Pydantic to dicts
        new_dicts = [m.model_dump() for m in metadata]
        
        # Serialize dict/JSON fields to strings for Parquet compatibility
        for d in new_dicts:
            if isinstance(d.get("provider_misc"), dict):
                d["provider_misc"] = json.dumps(d["provider_misc"])
        
        new_df = pl.DataFrame(new_dicts)

        if self.metadata_path.exists():
            existing_df = pl.read_parquet(self.metadata_path)
            updated_ids = new_df["site_id"].to_list()
            # Remove rows that are about to be updated
            existing_df = existing_df.filter(~pl.col("site_id").is_in(updated_ids))

            # If existing_df has Null columns that new_df has types for, cast existing to match.
            for col_name, new_dtype in new_df.schema.items():
                if col_name in existing_df.columns:
                    current_dtype = existing_df.schema[col_name]
                    # If file has Null but we now have data, cast the file's column
                    if current_dtype == pl.Null and new_dtype != pl.Null:
                        existing_df = existing_df.with_columns(
                            pl.col(col_name).cast(new_dtype)
                        )

            # Vertical concat
            final_df = pl.concat([existing_df, new_df], how="vertical")
        else:
            final_df = new_df

        final_df = dedupe_metadata(final_df)

        final_df.write_parquet(self.metadata_path)

    def get_site_metadata(self, site_ids: list[str] | None = None) -> pl.DataFrame:
        """Returns site metadata as polars dataframe"""
        if not self.metadata_path.exists():
            return pl.DataFrame()

        # Eager read required if we might overwrite later, but scan is fine for read-only
        # Using scan for performance
        q = pl.scan_parquet(self.metadata_path)

        if site_ids:
            q = q.filter(pl.col("site_id").is_in(site_ids))

        return q.collect()

    # ==========================================
    # DISCHARGE OPERATIONS (The Big Data)
    # ==========================================

    def store_discharge_dataframe(self, df: pl.DataFrame):
        """
        Writes discharge data using Hive Partitioning.
        Folder structure: discharge/site_id=XYZ/data.parquet
        Performs per-site upsert (Read -> Concat -> Dedup -> Write).
        """
        if df.is_empty():
            return

        # 1. Enforce Schema/Types
        # This prevents schema mismatch errors when appending to existing files
        df = df.select([
            pl.col("site_id").cast(pl.Utf8),
            pl.col("date").cast(pl.Date),
            pl.col("discharge").cast(pl.Float64),
            pl.col("quality_flag").cast(pl.Utf8)
        ])

        # 2. Partition Write Loop
        # We iterate sites because we need to lock/rewrite specific files
        site_ids = df["site_id"].unique().to_list()

        for site in site_ids:
            # Filter just this site's new data
            new_site_data = df.filter(pl.col("site_id") == site)
            
            # Define path: discharge/site_id=XYZ/data.parquet
            site_folder = self.discharge_path / f"site_id={site}"
            site_folder.mkdir(exist_ok=True)
            file_path = site_folder / "data.parquet"

            if file_path.exists():
                # Read existing
                existing_data = pl.read_parquet(file_path)
                
                # Concat
                combined = pl.concat([existing_data, new_site_data])
                
                # Dedup: If we have the same date twice, keep the NEW one (last)
                # Sort by date for read performance later
                combined = combined.unique(subset=["date"], keep="last").sort("date")
                
                combined.write_parquet(file_path)
            else:
                # No existing file, just write sorted data
                new_site_data.sort("date").write_parquet(file_path)
    
    def get_discharge_data(
        self, 
        site_ids: list[str], 
        start_date: str | None = None, 
        end_date: str | None = None
    ) -> pl.DataFrame:
        """
        Eagerly fetches data for specific sites as a Polars DataFrame.
        """
        # Construct specific paths to avoid scanning 20k folders
        # Assumes standard Hive layout: /site_id=VALUE/data.parquet
        file_paths = [
            str(self.discharge_path / f"site_id={sid}" / "data.parquet") 
            for sid in site_ids
        ]

        # Pass the explicit list of files. 
        # hive_partitioning=True ensures the 'site_id' column is inferred from the path.
        q = pl.scan_parquet(file_paths, hive_partitioning=True)

        if start_date:
            q = q.filter(pl.col("date") >= pl.lit(start_date).cast(pl.Date))
        if end_date:
            q = q.filter(pl.col("date") <= pl.lit(end_date).cast(pl.Date))

        return q.collect()

    def update_site_statistics(self, site_id: str):
        """
        Calculates stats from Parquet and updates the Metadata file.
        Refactored to reuse store_site_metadata logic.
        """
        site_file = self.discharge_path / f"site_id={site_id}" / "data.parquet"

        if not site_file.exists():
            logger.warning(f"Tried to update site statistics, but no data file found for {site_id}")
            return

        # Compute stats
        stats = pl.scan_parquet(site_file).select([
            pl.min("date").alias("min_date"),
            pl.max("date").alias("max_date"),
            pl.min("discharge").alias("min_discharge"),
            pl.max("discharge").alias("max_discharge"),
            pl.mean("discharge").alias("mean_discharge"),
            pl.len().alias("count_discharge")
        ]).collect()

        if stats.height == 0:
            return

        row = stats.row(0, named=True)

        # Fetch existing metadata to preserve other fields (like name, coords, etc.)
        current_meta_df = self.get_site_metadata([site_id])
        
        if current_meta_df.height > 0:
            # Convert existing Polars row to Dict
            # We must reconstruct the Pydantic model to merge safely
            # Assuming SiteMetadata handles extra fields or defaults
            meta_dict = current_meta_df.to_dicts()[0]
        else:
            # Create fresh dict if it doesn't exist
            meta_dict = {"site_id": site_id}

        # Update stats fields
        meta_dict.update({
            "min_date": row["min_date"],
            "max_date": row["max_date"],
            "min_discharge": row["min_discharge"],
            "max_discharge": row["max_discharge"],
            "mean_discharge": row["mean_discharge"],
            "count_discharge": row["count_discharge"],
            "last_updated": pd.Timestamp.now()
        })

        # Re-serialize JSON string fields back to dict if needed by Pydantic validation
        # (If your Pydantic model expects a dict but Parquet gave a string)
        if isinstance(meta_dict.get("provider_misc"), str):
             try:
                 meta_dict["provider_misc"] = json.loads(meta_dict["provider_misc"])
             except:
                 meta_dict["provider_misc"] = {}

        # Instantiate Pydantic Model
        try:
            site_obj = SiteMetadata(**meta_dict)
            # Use the existing upsert function
            self.store_site_metadata([site_obj])
        except Exception as e:
            logger.error(f"Failed to update metadata model for {site_id}: {e}")

    def update_last_fetched(self, site_id: str):
        if not self.metadata_path.exists():
            return

        df = pl.read_parquet(self.metadata_path)

        df = df.with_columns(
            pl.when(pl.col("site_id") == site_id)
            .then(pl.lit(pd.Timestamp.now())) 
            .otherwise(pl.col("last_updated"))
            .alias("last_updated")
        )

        df.write_parquet(self.metadata_path)


def dedupe_metadata(df: pl.DataFrame) -> pl.DataFrame:
    """
    Deduplicate metadata in two steps:
      1. Remove exact duplicate rows
      2. Remove duplicate site_id entries (keeping first)

    Logs info/warning messages and logged removed rows.
    """

    # ========== STEP 1 — Exact duplicate removal ==========
    df_exact = df.unique(keep="first")

    # ========== STEP 2 — Duplicate site_id removal ==========
    before_site = df_exact.height
    df_site = df_exact.unique(subset=["site_id"], keep="first")
    site_removed = before_site - df_site.height

    if site_removed > 0:
        logger.warning(
            f"Removed {site_removed} duplicate site_id entries (kept first)."
        )

        dup_ids = (
            df_exact
            .group_by("site_id")
            .count()
            .filter(pl.col("count") > 1)
            .get_column("site_id")
            .to_list()
        )
        logger.warning(
            f"Duplicate site_ids removed: {dup_ids}"
        )

        logger.warning(
            "Rows associated with these duplicate site_ids:\n"
            f"{df_exact.filter(pl.col('site_id').is_in(dup_ids))}"
        )

    return df_site
