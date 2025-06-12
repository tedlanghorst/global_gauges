import requests
from datetime import datetime
from io import StringIO

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from tqdm.auto import tqdm

from ._base import BaseProvider


class AustraliaProvider(BaseProvider):
    """
    Data provider for the Australian Bureau of Meteorology (BOM).
    http://www.bom.gov.au/waterdata/

    Translated and simplified from R package:
    https://github.com/buzacott/bomWater/blob/master/R/bomWater.R
    """

    name = "australia"

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
        df["station_latitude"] = pd.to_numeric(df["station_latitude"], errors="coerce")
        df["station_longitude"] = pd.to_numeric(df["station_longitude"], errors="coerce")
        df.dropna(subset=["station_latitude", "station_longitude"], inplace=True)

        geometry = [Point(xy) for xy in zip(df["station_longitude"], df["station_latitude"])]
        sites = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")

        sites.rename(columns={"ts_id": "site_id", "station_longname": "name"}, inplace=True)

        sites = sites[["site_id", "name", "geometry"]]
        sites["area"] = pd.NA
        sites["active"] = pd.NA
        sites["source"] = self.name

        sites.to_file(self.station_path, driver="GeoJSON")

    def _download_daily_values(self, site_ids: list[str]):
        """Downloads daily discharge data for Australian sites."""
        conn = self.connect_to_db()
        end_date = datetime.now()

        for site_id in tqdm(site_ids):
            last_date = self.get_last_entry_date(conn, site_id)
            if last_date:
                if last_date >= end_date:
                    continue  # Already up to date
                else:
                    start_date = last_date + pd.Timedelta(days=1)
            else:
                start_date = pd.to_datetime("1950-01-01")

            data = _download_site_dv(site_id, start_date, end_date)
            data.to_sql("discharge", conn, if_exists="append", index=False)


def _download_site_dv(site_id: str, start_date, end_date):
    """Download daily discharge values for a single site"""

    base_url = "http://www.bom.gov.au/waterdata/services"
    params = {
        "service": "kisters",
        "type": "QueryServices",
        "format": "json",
        "request": "getTimeseriesValues",
        "ts_id": site_id,
        "from": start_date,
        "to": end_date,
        "returnfields": "Timestamp,Value,Quality Code",
    }
    response = requests.get(base_url, params=params)
    response.raise_for_status()

    json_data = response.json()[0]
    column_names = json_data["columns"].split(",")
    data_string = "\n".join([",".join(map(str, row)) for row in json_data["data"]])
    df = pd.read_csv(StringIO(data_string), names=column_names)

    # Convert data types
    df["Timestamp"] = pd.to_datetime(df["Timestamp"]).dt.date
    df["Value"] = pd.to_numeric(df["Value"])

    df.rename(
        columns={"Timestamp": "date", "Value": "discharge", "Quality Code": "quality_flag"},
        inplace=True,
    )
    df["site_id"] = site_id

    return df
