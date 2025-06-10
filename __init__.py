from .global_gauges import (
    download_providers,
    download_station_info,
    download_daily_values,
    get_database_age,
    get_station_info,
    get_active_stations,
    get_daily_values,
)

__all__ = [
    "download_providers",
    "download_station_info",
    "download_daily_values",
    "get_database_age",
    "get_station_info",
    "get_active_stations",
    "get_daily_values",
]

age_days = get_database_age()
for name, age in age_days.items():
    if age > 30:
        print(f"Warning: {name.upper()} database is {age} days old. Consider updating.")
