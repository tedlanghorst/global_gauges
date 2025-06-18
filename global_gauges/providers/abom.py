import requests
from io import StringIO

import pandas as pd
import aiohttp

from ._base import BaseProvider
from ..database import QualityFlag


class ABoMProvider(BaseProvider):
    """
    Data provider for the Australian Bureau of Meteorology (BOM).
    http://www.bom.gov.au/waterdata/

    Translated and simplified from R package:
    https://github.com/buzacott/bomWater/blob/master/R/bomWater.R
    """

    name = "abom"
    desc = "Australian Bureau of Meteorology"
    # http://www.bom.gov.au/water/hrs/qc_doc.shtml
    quality_map = {
        "A": QualityFlag.GOOD,
        "B": QualityFlag.SUSPECT,
        "C": QualityFlag.ESTIMATED,
        "E": QualityFlag.UNKNOWN,
        "F": QualityFlag.BAD,
    }

    def _download_station_info(self) -> None:
        """
        Downloads station info from the BOM Water Data service.
        It uses the GetStationList service to retrieve a CSV of all stations.
        """
        base_url = "http://www.bom.gov.au/waterdata/services"

        return_fields = ["station_longname", "ts_id", "station_latitude", "station_longitude"]
        params = {
            "service": "kisters",
            "type": "QueryServices",
            "format": "json",
            "request": "getTimeseriesList",
            "parametertype_name": "Water Course Discharge",
            "ts_name": "DMQaQc.Merged.DailyMean.24HR",
            "returnfields": ",".join(return_fields),
        }

        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)

        data = response.json()
        df = pd.DataFrame(data[1:], columns=data[0])

        # Drop rows where latitude or longitude is NaN (missing/invalid coordinates)
        df["latitude"] = pd.to_numeric(df["station_latitude"], errors="coerce")
        df["longitude"] = pd.to_numeric(df["station_longitude"], errors="coerce")
        df.dropna(subset=["latitude", "longitude"], inplace=True)

        df.rename(columns={"ts_id": "site_id", "station_longname": "name"}, inplace=True)

        return df[["site_id", "name", "latitude", "longitude"]]

    async def _download_daily_values(
        self, site_id: str, start: pd.Timestamp, misc: dict
    ) -> pd.DataFrame:
        """Downloads daily discharge data for Australian sites."""
        end = pd.Timestamp.now().date()

        base_url = "http://www.bom.gov.au/waterdata/services"
        params = {
            "service": "kisters",
            "type": "QueryServices",
            "format": "json",
            "request": "getTimeseriesValues",
            "ts_id": site_id,
            "from": start.strftime("%Y-%m-%d"),
            "to": end.strftime("%Y-%m-%d"),
            "returnfields": "Timestamp,Value,Quality Code",
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(base_url, params=params) as response:
                response.raise_for_status()
                json_data = await response.json()

                column_names = json_data[0]["columns"].split(",")
                data_string = "\n".join([",".join(map(str, row)) for row in json_data[0]["data"]])
                df = pd.read_csv(StringIO(data_string), names=column_names)

                # Convert data types
                df["Timestamp"] = pd.to_datetime(df["Timestamp"]).dt.date
                df["Value"] = pd.to_numeric(df["Value"])

                df.rename(
                    columns={
                        "Timestamp": "date",
                        "Value": "discharge",
                        "Quality Code": "quality_flag",
                    },
                    inplace=True,
                )
                df["site_id"] = site_id

                return df
