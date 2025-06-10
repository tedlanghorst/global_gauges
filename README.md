# Global Gauges Python Package
An interface for downloading, updating, and querying river gauge data from multiple providers. This package is focused on mantaining a local database of up-to-date river discharge from global providers. Currently, only the USGS and HYDAT are implemented, but other agencies or organizations with API access to current data can be easily added using the BaseProviders class. 

## Features
- Download and update station metadata and daily discharge data from supported providers
- Query and combine data from multiple sources with a consistent API
- Easily filter for active stations and select by site or date range


## Installation
You can install the package using one of the following methods:

### Using uv
```bash
uv venv .venv
source .venv/bin/activate
uv pip install .
```

### Using conda
```bash
conda create -n global-gauges python=3.10
conda activate global-gauges
pip install .
```

## Example Use
Import the package and use the high-level API:

```python
import global_gauges as gg

# Download all data for all providers
gg.download_all_data()

# Update the local database from provider APIs
gg.download_all_data(update=True)

# Download only station info for USGS
gg.download_station_info('usgs')

# Download daily values for specific sites
gg.download_daily_values(providers='hydat', sites=['01AB002', '02BC003'])

# Query station metadata
df = gg.get_station_info(['usgs', 'hydat'])

# Query daily values for a date range
dv = gg.get_daily_values(providers='usgs', sites='06892350', start_date='2020-01-01', end_date='2020-12-31')

# Get only active stations
df_active = gg.get_active_stations('hydat')

# Check database age
ages = gg.get_database_age()
```

## Command Line Usage

You can also download data directly from the command line after installing the package:


Example:

```bash
python -m global_gauges download_all_data
```
```bash
python -m global_gauges download_all_data --update --provider usgs
```


## License
This project is licensed under the GNU General Public License v3.0 or later (GPLv3+). See the LICENSE file for details.
