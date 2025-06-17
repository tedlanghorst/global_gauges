import requests
import pandas as pd
import geopandas as gpd
from tqdm.auto import tqdm
from bs4 import BeautifulSoup
import re
from datetime import datetime

from .._base import BaseProvider


class SouthAfricaProvider(BaseProvider):
    """
    Data provider for South Africa's Department of Water and Sanitation (DWA).
    """

    name = "south_africa"

    def _download_station_info(self) -> None:
        """
        Placeholder for downloading station information for South Africa.
        The R scripts rely on a pre-compiled list, and a public inventory is not readily available for direct download.
        A real implementation would require finding an official station list or scraping one from the DWA website.
        """
        print(
            "Warning: Station info download for South Africa is not implemented due to lack of a public inventory API."
        )
        print(
            "This provider will only work if a station file is manually created or if site_ids are provided directly."
        )
        # Create an empty file to prevent errors on subsequent calls
        gpd.GeoDataFrame(
            columns=["site_id", "name", "area", "source", "active", "geometry"]
        ).to_file(self.station_path, driver="GeoJSON")

    def _download_daily_values(self, site_ids: list[str]):
        """
        Downloads daily discharge data by scraping the DWA Hydrology website.
        This is a direct Python translation of the complex scraping logic in the allRIGGS.R file.
        """
        conn = self.connect_to_db()

        for site_id in tqdm(site_ids, desc=f"Downloading {self.name.upper()} data"):
            all_site_data = []

            # The R script logic chunks downloads into 20-year periods to avoid timeouts.
            start_year = 1900
            end_year = datetime.now().year

            for year in range(start_year, end_year + 1, 20):
                chunk_start_date = f"{year}-01-01"
                chunk_end_date = f"{min(year + 19, end_year)}-12-31"

                # Construct the endpoint URL as done in the R script.
                url = (
                    f"https://www.dws.gov.za/Hydrology/Verified/HyData.aspx?"
                    f"Station={site_id}100.00&DataType=Daily"
                    f"&StartDT={chunk_start_date}&EndDT={chunk_end_date}&SiteType=RIV"
                )

                try:
                    response = requests.get(url)
                    response.raise_for_status()

                    soup = BeautifulSoup(response.content, "html.parser")

                    # The R script gets the text from the body and splits by newline.
                    body_text = soup.body.get_text(separator="\n")
                    lines = body_text.splitlines()

                    # Find the header row to know where the data starts
                    header_index = -1
                    for i, line in enumerate(lines):
                        if line.strip().startswith("DATE"):
                            header_index = i
                            break

                    if header_index == -1:
                        continue

                    # Process only the data lines
                    data_lines = lines[header_index + 1 :]
                    chunk_data = []
                    for line in data_lines:
                        # The R script splits by one or more spaces (' +').
                        parts = re.split(r"\s+", line.strip())
                        if len(parts) >= 2:
                            # Headers are "DATE", "D_AVG_FR", "QUAL". We need the first two.
                            date_str, q_str = parts[0], parts[1]
                            chunk_data.append({"date": date_str, "discharge": q_str})

                    if chunk_data:
                        all_site_data.extend(chunk_data)

                except requests.exceptions.RequestException as e:
                    print(f"Failed to fetch data for site {site_id}, chunk starting {year}: {e}")

            if all_site_data:
                df = pd.DataFrame(all_site_data)
                df["site_id"] = site_id
                df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce")
                df["discharge"] = pd.to_numeric(df["discharge"], errors="coerce")
                df.dropna(inplace=True)

                df[["site_id", "date", "discharge"]].to_sql(
                    "discharge", conn, if_exists="append", index=False
                )
