import fire

from global_gauges import ConfigManager, GaugeDataFacade


class Config:
    @staticmethod
    def set_data_dir(path: str):
        """Set the default data directory for downloads."""
        config = ConfigManager()
        data_dir = config.set_default_data_dir(path)
        print(f"Default data directory set to: {data_dir}")

    @staticmethod
    def set_key(provider: str, key: str):
        config = ConfigManager()
        config.set_provider_key(provider, key)


class Download:
    @staticmethod
    def all(providers=None, force_update=False, workers=1):
        """Download all data (station info and timeseries)."""
        downloader = GaugeDataFacade(providers=providers)
        downloader.download(force_update=force_update, workers=workers)

    @staticmethod
    def stations(providers=None, force_update=False, workers=1):
        """Download only station info."""
        downloader = GaugeDataFacade(providers=providers)
        downloader.download_station_info(force_update=force_update, workers=workers)

    @staticmethod
    def timeseries(providers=None, tolerance=1, force_update=False, workers=1):
        """Download only timeseries data."""
        downloader = GaugeDataFacade(providers=providers)
        downloader.download_daily_values(
            tolerance=tolerance, force_update=force_update, workers=workers
        )


if __name__ == "__main__":
    fire.Fire(
        {
            "config": Config,
            "download": Download,
        }
    )
