from .usgs import UsgsProvider
from .hydat import HydatProvider
from .ukea import UKEnvironmentAgencyProvider

# Map provider names to their classes (or instances)
PROVIDER_MAP = {
    "usgs": UsgsProvider(),
    "hydat": HydatProvider(),
    "ukea": UKEnvironmentAgencyProvider(),
}
