from .usgs import UsgsProvider
from .hydat import HydatProvider

# Map provider names to their classes (or instances)
PROVIDER_MAP = {
    "usgs": UsgsProvider(),
    "hydat": HydatProvider(),
}
