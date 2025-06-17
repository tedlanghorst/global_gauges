import requests
import pandas as pd
import geopandas as gpd
from tqdm.auto import tqdm
from bs4 import BeautifulSoup
from datetime import datetime

from .._base import BaseProvider


class JapanProvider(BaseProvider):
    """
    Data provider for Japan's Ministry of Land, Infrastructure, Transport and Tourism (MLIT).
    http://www1.river.go.jp/
    """

    name = "japan"

    def _download_station_info(self) -> None:
        """
        Placeholder for downloading station information.
        The R scripts rely on a pre-compiled list, and a public inventory is not readily available.
        A real implementation would require finding an official station list or scraping one.
        """
        print(
            "Warning: Station info download for Japan is not implemented due to lack of a public inventory API."
        )
        print(
            "This provider will only work if a station file is manually created or if site_ids are provided directly to download_daily_values."
        )
        # Create an empty file to prevent errors on subsequent calls
        gpd.GeoDataFrame(
            columns=["site_id", "name", "area", "source", "active", "geometry"]
        ).to_file(self.station_path, driver="GeoJSON")

    def _download_daily_values(self, site_ids: list[str]):
        """
        Downloads daily discharge data by scraping the MLIT website.
        Replicates the iterative approach from the R script.
        """
        conn = self.connect_to_db()

        for site_id in tqdm(site_ids, desc=f"Downloading {self.name.upper()} data"):
            all_site_data = []
            # The R script iterates by year. We'll do the same.
            start_year = 1980  # A reasonable starting point
            end_year = datetime.now().year

            for year in range(start_year, end_year + 1):
                try:
                    # Construct URL as in the R script
                    url = f"http://www1.river.go.jp/cgi-bin/DspWaterData.exe?KIND=6&ID={site_id}&BGNDATE={year}0101&ENDDATE={year}1231"
                    response = requests.get(url)
                    response.raise_for_status()

                    soup = BeautifulSoup(response.content, "html.parser")

                    # Find the main data table
                    tables = soup.find_all("table")
                    if len(tables) < 3:
                        continue

                    # The data is in the third table on the page.
                    # The first row is headers, data starts from the second.
                    rows = tables[2].find_all("tr")[1:]
                    if not rows:
                        continue

                    page_data = []
                    for row in rows:
                        cols = row.find_all("td")
                        if not cols:
                            continue

                        date_str = cols[0].text.strip()
                        # Data columns are from index 1 to 24 (for 24 hours)
                        hourly_values = [
                            pd.to_numeric(c.text.strip(), errors="coerce") for c in cols[1:25]
                        ]

                        # The R script calculates the daily mean from hourly values
                        daily_mean = pd.Series(hourly_values).mean(skipna=True)

                        if pd.notna(daily_mean):
                            page_data.append(
                                {
                                    "date": pd.to_datetime(date_str, format="%Y/%m/%d"),
                                    "discharge": daily_mean,
                                }
                            )

                    if page_data:
                        all_site_data.extend(page_data)

                except requests.exceptions.RequestException as e:
                    # Log error but continue to next year
                    print(f"Could not fetch data for site {site_id}, year {year}: {e}")

            if all_site_data:
                df = pd.DataFrame(all_site_data)
                df["site_id"] = site_id
                df[["site_id", "date", "discharge"]].to_sql(
                    "discharge", conn, if_exists="append", index=False
                )
