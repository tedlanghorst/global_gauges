import requests

import xml.etree.ElementTree as ET
import pandas as pd
import aiohttp

from ._base import BaseProvider
from ..database import QualityFlag


class BrANAProvider(BaseProvider):
    """
    Data provider for the Brazilian National Water Agency (Agência Nacional de Águas - ANA).

    Provides access to flow/stage data from hydrological stations across Brazil.
    API reference: http://telemetriaws1.ana.gov.br/

    TODO: This is API endpoint is set to be deprecated in June 2026. I believe the new API
    requires a key which is more difficult to access.

    This code is based off hydrobr package by Wallisson Moreira de Carvalho
    https://github.com/wallissoncarvalho/hydrobr/blob/refactor/hydrobr/get_data.py
    """

    name = "brana"
    desc = "Brazilian National Water Agency (Agência Nacional de Águas)"
    quality_map = {
        "0": QualityFlag.PROVISIONAL,  # Raw data
        "1": QualityFlag.BAD,  # Bruto
        "2": QualityFlag.GOOD,  # Consistido
    }

    def _download_station_info(self, api_key: str | None) -> pd.DataFrame:
        """
        Downloads metadata for all flow stations from ANA's ANAF database.
        """
        params = {
            "codEstDE": "",
            "codEstATE": "",
            "tpEst": "1",
            "nmEst": "",
            "nmRio": "",
            "codSubBacia": "",
            "codBacia": "",
            "nmMunicipio": "",
            "nmEstado": "",
            "sgResp": "ANA",
            "sgOper": "",
            "telemetrica": "",
        }

        base_url = "http://telemetriaws1.ana.gov.br/ServiceANA.asmx/HidroInventario"
        response = requests.get(base_url, params, timeout=120.0)

        tree = ET.ElementTree(ET.fromstring(response.content))
        root = tree.getroot()

        station_info = []
        for station in root.iter("Table"):
            if station.find("ResponsavelSigla").text != "ANA":
                # Even though request 'ANA' as responsible,
                # the filter allows partial matches like 'Guiana' makes it through.
                continue

            data_dict = {
                "site_id": station.find("Codigo").text,
                "name": station.find("Nome").text,
                "latitude": float(station.find("Latitude").text),
                "longitude": float(station.find("Longitude").text),
                "area": station.find("AreaDrenagem").text,
            }
            station_info.append(data_dict)

        return pd.DataFrame(station_info)

    async def _download_daily_values(
        self, site_id: str, start: pd.Timestamp, api_key: str | None, misc: dict
    ) -> pd.DataFrame:
        """
        Downloads daily flow data for a specific station.

        Args:
            site_id: Station code (8 digits)
            start: Start date for data retrieval
            misc: Additional metadata (unused for ANA)

        Returns:
            DataFrame with columns: site_id, date, discharge, quality_flag
        """
        base_url = "http://telemetriaws1.ana.gov.br/ServiceANA.asmx/HidroSerieHistorica"
        params = {
            "codEstacao": site_id,
            "dataInicio": "",  # Empty means get all data
            "dataFim": "",
            "tipoDados": "3",  # 3 = Flow (Vazao), 1 = Stage (Cota), 2 = Precipitation (Chuva)
            "nivelConsistencia": "",  # Empty means get all consistency levels
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(
                base_url, params=params, timeout=aiohttp.ClientTimeout(total=120)
            ) as response:
                if response.status != 200:
                    return pd.DataFrame()

                content = await response.read()
                tree = ET.ElementTree(ET.fromstring(content))
                root = tree.getroot()

        # Parse XML data
        all_records = []

        for month in root.iter("SerieHistorica"):
            code = month.find("EstacaoCodigo").text
            code = f"{int(code):08}"
            consist = month.find("NivelConsistencia").text

            date_str = month.find("DataHora").text
            date = pd.to_datetime(date_str, dayfirst=False)
            # Get first day of month
            month_start = date.to_period("M").to_timestamp()

            # Get number of days in this month using pandas
            next_month = month_start + pd.offsets.MonthBegin(1)
            last_day = (next_month - pd.Timedelta(days=1)).day

            # Extract daily values for the month
            for day in range(1, last_day + 1):
                value_tag = f"Vazao{day:02d}"
                value_elem = month.find(value_tag)

                if value_elem is not None and value_elem.text is not None:
                    try:
                        discharge = float(value_elem.text)
                        record_date = month_start + pd.Timedelta(days=day - 1)

                        # Only include data from start date onwards
                        if record_date >= start:
                            all_records.append(
                                {
                                    "site_id": code,
                                    "date": record_date.strftime("%Y-%m-%d"),
                                    "discharge": discharge,
                                    "quality_flag": str(consist),
                                }
                            )
                    except (ValueError, TypeError):
                        continue

        if not all_records:
            return pd.DataFrame()

        df = pd.DataFrame(all_records)
        return df[["site_id", "date", "discharge", "quality_flag"]]
