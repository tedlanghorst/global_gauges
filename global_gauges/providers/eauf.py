import requests

import pandas as pd
import aiohttp

from ._base import BaseProvider
from ..database import QualityFlag


class EauFProvider(BaseProvider):
    """
    Data provider for Hub'eau (France).
    https://hubeau.eaufrance.fr/page/api-hydrometrie

    """

    name = "eauf"
    desc = "French water (eau) information service"
    quality_map = {
        "Bonne": QualityFlag.GOOD,
        "Douteuse": QualityFlag.SUSPECT,
    }  # Good or Dubious lol

    def _download_station_info(self) -> None:
        """Downloads and saves metadata for all hydrometric stations."""

        base_url = "https://hubeau.eaufrance.fr/api/v2/hydrometrie/referentiel/stations"

        response = requests.get(base_url, {"size": 10000})
        response.raise_for_status()

        if response.status_code == 200:
            # All results were returned
            df = pd.DataFrame(response.json()["data"])
        elif response.status_code == 206:
            # Max records returned but there are more.
            rjson = response.json()  # parse the json once.
            df_list = [pd.DataFrame(rjson["data"])]
            while rjson["next"] is not None:
                response = requests.get(rjson["next"])
                response.raise_for_status()
                df_list.append(pd.DataFrame(rjson["data"]))
            df = pd.concat(df_list, ignore_index=True)
        else:
            raise RuntimeError(
                "Unhandled response from Hubeau API.\n"
                f"response: {response.status_code}\n"
                f"content: {response.text}"
            )

        # Filter stations to these types. I belive others do not have Q.
        df = df[df["type_station"].isin(["STD", "DEB"])]

        # TODO Cannot find a watershed area column.
        name_map = {
            "code_site": "site_id",
            "libelle_site": "name",
            "en_service": "active",
            "latitude_station": "latitude",
            "longitude_station": "longitude",
        }
        df = df[name_map.keys()].rename(columns=name_map)

        return df

    async def _download_daily_values(
        self, site_id: str, start: pd.Timestamp, misc: dict
    ) -> pd.DataFrame:
        """Downloads daily discharge data for the given site_id."""

        base_url = "https://hubeau.eaufrance.fr/api/v2/hydrometrie/obs_elab"
        params = {
            "format": "json",
            "code_entite": site_id,
            "grandeur_hydro_elab": "QmnJ",
            "date_debut_obs_elab": start.strftime("%Y-%m-%d"),
            "date_fin_obs_elab": pd.Timestamp.now().strftime("%Y-%m-%d"),
            "size": 20000,  # Max
        }
        async with aiohttp.ClientSession() as session:
            df_list = []
            next_url = str(aiohttp.client.URL(base_url).with_query(params))
            while next_url:
                async with session.get(next_url) as response:
                    response.raise_for_status()
                    rjson = await response.json()
                    df_list.append(pd.DataFrame(rjson["data"]))
                    next_url = rjson.get("next")
            df = pd.concat(df_list, ignore_index=True)

        name_map = {
            "code_site": "site_id",
            "date_obs_elab": "date",
            "resultat_obs_elab": "discharge",
            "libelle_qualification": "quality_flag",
        }

        if df.empty:
            return pd.DataFrame()
        else:
            df = df.rename(columns=name_map)
            df["discharge"] /= 1000  # l/s to m3/s
            return df[["site_id", "date", "discharge", "quality_flag"]]


def fetch_paginated_data(base_url, params):
    """We will make an initial request for this site then page through."""

    response = requests.get(base_url, params)
    response.raise_for_status()

    if response.status_code == 200:
        # All results were returned
        df = pd.DataFrame(response.json()["data"])
    elif response.status_code == 206:
        # Max records returned but there are more.
        rjson = response.json()  # parse the json once.
        df_list = [pd.DataFrame(rjson["data"])]
        while rjson["next"] is not None:
            response = requests.get(rjson["next"])
            response.raise_for_status()
            df_list.append(pd.DataFrame(rjson["data"]))
        df = pd.concat(df_list, ignore_index=True)
    else:
        raise RuntimeError(
            "Unhandled response from Hubeau API.\n"
            f"response: {response.status_code}\n"
            f"content: {response.text}"
        )

    return df
