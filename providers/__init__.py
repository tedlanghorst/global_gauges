from .abom import ABOMProvider
from .eccc import ECCCProvider
from .ukea import UKEAProvider
from .usgs import USGSProvider

# Map provider names to their classes
PROVIDER_MAP = {
    "abom": ABOMProvider,
    "eccc": ECCCProvider,
    "ukea": UKEAProvider,
    "usgs": USGSProvider,
}
