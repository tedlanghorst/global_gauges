import requests
import asyncio
import io

import pandas as pd
import aiohttp

from ._base import BaseProvider
from ..database import QualityFlag


class ECCCProvider(BaseProvider):
    """Data provider for Environment and Climate Change Canada (ECCC) hydrologic data"""

    name = "eccc"
    desc = "Environment and Climate Change Canada"
    # https://collaboration.cmc.ec.gc.ca/cmc/hydrometrics/www/Document/WebService_Guidelines_RealtimeData.pdf
    quality_map = {
        "FINAL": QualityFlag.GOOD,
        "PROVISIONAL": QualityFlag.PROVISIONAL,
        "10": QualityFlag.SUSPECT,
        "20": QualityFlag.ESTIMATED,
        "30": QualityFlag.SUSPECT,
        "40": QualityFlag.BAD,
        "-1": QualityFlag.UNKNOWN,
    }

    def _download_station_info(self, api_key: str | None) -> pd.DataFrame:
        """Download and save metadata for all CNHS sites with discharge data."""
        base_url = "https://api.weather.gc.ca/collections/hydrometric-stations/items?limit=10000"
        response = requests.get(base_url)
        response.raise_for_status()

        # Make a df from the flattened json data
        source_df = pd.json_normalize(response.json()["features"])

        def check_for_daily_q(row):
            substr = "Daily Mean of Water Level or Discharge"
            for link in row["links"]:
                if substr in link["title"]:
                    return True
            return False

        has_daily_q = source_df.apply(check_for_daily_q, axis=1)
        source_df = source_df[has_daily_q]

        df = pd.concat(
            [
                source_df["id"].rename("site_id"),
                source_df["properties.STATION_NAME"].rename("name"),
                source_df["properties.DRAINAGE_AREA_GROSS"].rename("area"),
                (source_df["properties.STATUS_EN"] == "Active").rename("active"),
            ],
            axis=1,
        )
        # Coordinates column contains a 2 element list with lat/lon.
        df[["longitude", "latitude"]] = pd.DataFrame(source_df["geometry.coordinates"].tolist())

        return df

    async def _download_historical_values(
        self, session: aiohttp.ClientSession, site_id: str, start: pd.Timestamp, end: pd.Timestamp
    ) -> pd.DataFrame:
        """
        Fetch historical data from the ECCC OGC API (HYDAT equivalent).
        This API returns GeoJSON and requires paging.
        """
        base_url = "https://api.weather.gc.ca/collections/hydrometric-daily-mean/items"
        all_features = []
        limit = 1000
        offset = 0

        datetime_range = (
            f"{start.strftime('%Y-%m-%dT00:00:00Z')}/{end.strftime('%Y-%m-%dT23:59:59Z')}"
        )

        while True:
            params = {
                "STATION_NUMBER": site_id,
                "datetime": datetime_range,
                "limit": limit,
                "offset": offset,
                "sortby": "DATE",
                "f": "json",
            }
            async with session.get(base_url, params=params) as response:
                response.raise_for_status()
                json_data = await response.json()

            features = json_data.get("features", [])
            if not features:
                break

            all_features.extend(features)
            offset += len(features)

            if len(features) < limit:
                break

        if not all_features:
            return pd.DataFrame()

        # Parse the GeoJSON response
        records = []
        for feat in all_features:
            props = feat.get("properties", {})

            # Only include records that have discharge data
            if props.get("DISCHARGE") is not None:
                records.append(
                    {
                        "site_id": props.get("STATION_NUMBER"),
                        "date": props.get("DATE"),
                        "discharge": props.get("DISCHARGE"),
                        "quality_flag": props.get("DISCHARGE_SYMBOL_EN"),
                    }
                )

        if not records:
            return pd.DataFrame()

        df = pd.DataFrame(records)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        return df

    async def _download_realtime_values(
        self, session: aiohttp.ClientSession, site_id: str, start: pd.Timestamp, end: pd.Timestamp
    ) -> pd.DataFrame:
        """
        Fetch recent data from the ECCC real-time CSV service.
        (This is your original function, adapted as a helper)
        """
        base_url = "https://wateroffice.ec.gc.ca/services/real_time_data/csv/inline"
        params = {
            "stations[]": site_id,
            "parameters[]": 47,  # Discharge
            "start_date": start.strftime("%Y-%m-%d") + " 00:00:00",
            "end_date": end.strftime("%Y-%m-%d") + " 23:59:59",
        }
        async with session.get(base_url, params=params) as response:
            response.raise_for_status()
            text_data = await response.text()

            # If there's only one line (the header), there's no data.
            if len(text_data.strip().split("\n")) <= 1:
                return pd.DataFrame()

            df = pd.read_csv(io.StringIO(text_data), low_memory=False)
            df.rename(columns={" ID": "site_id", "Value/Valeur": "discharge"}, inplace=True)
            df["date"] = pd.to_datetime(df["Date"]).dt.date

            combined_qual = df["Qualifier/Qualificatif"].fillna(df["Approval/Approbation"])
            df["quality_flag"] = combined_qual.map(
                lambda x: f"{x:.0f}" if isinstance(x, float) else x
            )

            agg_dict = {"site_id": "first", "discharge": "mean", "quality_flag": "first"}
            daily_df = df.groupby("date").agg(agg_dict).reset_index()

            return daily_df

    async def _download_daily_values(
        self, site_id: str, start: pd.Timestamp, api_key: str | None, misc: dict
    ) -> pd.DataFrame:
        # 1. Define dates
        end = pd.Timestamp.now(tz=start.tz)
        # Real-time data is available for the last 18 months
        cutoff_date = end - pd.DateOffset(months=18)

        async with aiohttp.ClientSession() as session:
            tasks = []

            # 2. Check if historical data is needed
            if start < cutoff_date:
                # We need data from 'start' up to (but not including) the cutoff
                hist_end = cutoff_date - pd.Timedelta(days=1)
                tasks.append(self._download_historical_values(session, site_id, start, hist_end))

            # 3. Check if real-time data is needed
            # This is from the cutoff date OR the start date (whichever is later)
            realtime_start = max(start, cutoff_date)
            if realtime_start < end:
                tasks.append(self._download_realtime_values(session, site_id, realtime_start, end))

            # 4. Run downloads concurrently
            if not tasks:
                return pd.DataFrame()

            results = await asyncio.gather(*tasks)
            dataframes = [df for df in results if not df.empty]

            if not dataframes:
                return pd.DataFrame()

        # 5. Combine and return
        final_df = (
            pd.concat(dataframes)
            .drop_duplicates(subset=["date"], keep="last")
            .sort_values(by="date")
            .reset_index(drop=True)
        )

        return final_df
