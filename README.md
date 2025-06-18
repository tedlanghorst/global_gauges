# Global Gauges Python Package
An interface for downloading, updating, and querying river gauge data from multiple providers. This package is focused on mantaining a local database of up-to-date river discharge from global providers. Currently, providers are implemented for United States, Canada, France, United Kingdom, and Australian services are implemented, but other agencies or organizations with API access to current data can be easily added using the BaseProviders class. 

## Features
- Download and update station metadata and daily discharge data from supported providers
- Query and combine data from multiple sources with a consistent API
- Easily filter for active stations and select by site or date range


## Installation
### Using uv
```bash
uv venv
source .venv/bin/activate
uv pip install .
```
 
### Or, using conda
```bash
conda create -n global-gauges python=3.10
conda activate global-gauges
pip install .
```

## Python interface

### Getting started
The first time you use global_gauges you must tell the package where to store your data. This path will be saved to config file. If you want to change this path, just call the function again.
```python
import global_gauges as gg

# Only needs to run your first time.
gg.set_default_data_dir("/path/to/data/dir")

# Next you need to create a GlobalGaugesFacade object.
facade = gg.GaugeDataFacade()
```

The GaugeDataFacade bundles all the providers together and allows us to interact with each provider without worrying about their specific details. The `download` method first calls `download_station_info` on each provider, and then calls `download_daily_values` on each provider.

```python
# Download methods allow a number of workers, which can download multiple providers in parallel.
facade.download(workers=4)

# 
gdf = facade.get_station_info()

# facade.get_daily_values() will return a pandas dataframe for any site(s) in the database.
df = facade.get_daily_values(['USGS-01010070','USGS-01010500'])

# You can also query a shorter timeseries by specifying start and/or end dates.
df = facade.get_daily_values('USGS-01010070', '2020-01-01', '2020-12-31')

```

### Updating
global_gauges is designed to make it easy to maintain an up-to-date database GaugeDataFacade() will automatically warn you if any of your databases are >30 days old. You can also easily check how many days it has been since each database was modified, or the date that each site was modified. 

```python
# You can easily update your databases by just calling download again.
facade.download_daily_values(workers=4)

# Returns a dict of {provider: age}
provider_ages = facade.get_database_ages()

# 'get_station_info' returns a geopandas GeoDataFrame, which contains the age of each site, among other things.
station_ages = facade.get_station_info()['last_updated']
```

For more details on the high-level interaction, see the code and docstrings in `global_gauges.py`.

## Command Line Interface
There are also a few commands you can call from a CLI to download data. This interface could be useful if you want to schedule a bash script to periodically update your databases. 

Just like the python interface, we need to set the data directory if you have not already done so. 
```bash
python global_gauges.py config set_data_dir /path/to/data
```
The same three download methods are exposed to the CLI. Note you would never actually call these three in a row, as `download all` just combines `stations` and `timeseries`. 
```bash
python global_gauges.py download all
python global_gauges.py download stations
python global_gauges.py download timeseries
```
Or, with some arguments:
```bash
python global_gauges.py download all --providers "usgs,eccc" --workers 2
python global_gauges.py download stations --providers "ukea" --force_update True
python global_gauges.py download timeseries --tolerance 30 # only update sites >30 days old.
```

## License
This project is licensed under the GNU General Public License v3.0 or later (GPLv3+). See the LICENSE file for details.
