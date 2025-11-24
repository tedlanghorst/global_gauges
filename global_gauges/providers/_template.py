import pandas as pd

from ._base import BaseProvider
from ..database import QualityFlag  # noqa: F401


class WEAProvider(BaseProvider):
    """
    Template for a new data provider. Replace the properties and fill in the methods below.
    """

    name = "wea"  # Unique string identifier for your provider
    desc = "Wakandan Environmental Agency"  # Short description of source.
    quality_map = {
        # Optional, define a quality_map if your data source uses quality flags.
        # Any unmapped flags will be assigned to QualityFlag.UNKNOWN
        # Example:
        # "Good": QualityFlag.GOOD,
        # "P": QualityFlag.PROVISIONAL,
        # "Est": QualityFlag.ESTIMATED,
        # "ice": QualityFlag.SUSPECT,
        # "Err": QualityFlag.BAD,
    }
    # Override with True if an API key is needed to pull data.
    requires_key: bool = False

    def _download_station_info(self, api_key: str | None) -> pd.DataFrame:
        """
        Download and return metadata for all sites with discharge data.
        Returns a DataFrame with columns: [site_id, name, area, active, latitude, longitude].
        `area` and `active` can be excluded if they are not given by the provider.
        The meaning of `active` is defined by the provider. If you want to know how recent the data are,
        there is be a metadata property `max_date` that is set whenever we download daily data.
        """
        raise NotImplementedError("Implement station info download for your provider.")

    async def _download_daily_values(
        self, site_id: str, start: pd.Timestamp, api_key: str | None, misc: dict
    ) -> pd.DataFrame:
        """
        Download daily discharge data for the given site_id(s).
        Returns a DataFrame with columns: [site_id, date, discharge, quality_flag]
        Make sure the discharge is in cubic meters per second.

        This method must be implemented as an async operation. The BaseProvider class
        will ensure that only 1 request to the provider API is made at a time (to prevent hammering
        the provider's server), and only uses the async feature to do some database manipulation with
        the dataframe we return here. There are two general ways I have implemented the async calls,
        although there are many more.

        1. Use the package aiohttp to make asynchronous http requests
            async with aiohttp.ClientSession() as session:
            async with session.get(base_url, params=params) as response:
                response.raise_for_status()
                json_data = await response.json()
                df = pd.DataFrame(json_data)

        2. Wrap an asynchronous function in asyncio.to_thread()
            def some_synchronous_fn(site_id, start_date):
                ...
                return pd.DataFrame(data)
            df = await asyncio.to_thread(some_synchronous_fn, site_id, start_date)

        Option 2 is useful if you're using a library, like nwis for the USGS, that does not have a
        non-blocking data retrieval. Otherwise, Option 1 with aiohttp is very easy to imlement.

        """
        raise NotImplementedError("Implement async daily value download for your provider.")
