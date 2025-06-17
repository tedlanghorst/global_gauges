import requests

import io
import pandas as pd

from ._base import BaseProvider
from ..database import QualityFlag


class ECCCProvider(BaseProvider):
    """Data provider for Environment and Climate Change Canada (ECCC) hydrologic data"""

    name = "eccc"
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

    def _download_station_info(self) -> pd.DataFrame:
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

    def _download_daily_values(self, site_id: str, start: pd.Timestamp, misc: dict) -> pd.DataFrame:
        end = pd.Timestamp.now().date()

        base_url = "https://wateroffice.ec.gc.ca/services/real_time_data/csv/inline"
        params = {
            "stations[]": site_id,
            "parameters[]": 47,  # Discharge
            "start_date": start.strftime("%Y-%m-%d") + " 00:00:00",
            "end_date": end.strftime("%Y-%m-%d") + " 23:59:59",
        }
        response = requests.get(base_url, params=params)
        response.raise_for_status()

        # If there's only one line (the header), there's no data.
        if len(response.text.strip().split("\n")) <= 1:
            return

        df = pd.read_csv(io.StringIO(response.text), low_memory=False)
        # Really annoying bug (?) where the returned field has a space before 'ID'
        df.rename(columns={" ID": "site_id", "Value/Valeur": "discharge"}, inplace=True)
        df["date"] = pd.to_datetime(df["Date"]).dt.date

        # If there is no qualifier tag, use the approval status for quality_flag
        combined_qual = df["Qualifier/Qualificatif"].fillna(df["Approval/Approbation"])
        df["quality_flag"] = combined_qual.map(lambda x: f"{x:.0f}" if isinstance(x, float) else x)

        agg_dict = {"site_id": "first", "discharge": "mean", "quality_flag": "first"}
        daily_df = df.groupby("date").agg(agg_dict).reset_index()

        return daily_df
