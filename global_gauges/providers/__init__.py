from ._base import BaseProvider
from .abom import ABoMProvider
from .eccc import ECCCProvider
from .eauf import EauFProvider
from .ukea import UKEAProvider
from .usgs import USGSProvider

# Map provider names to their classes
PROVIDER_MAP = {
    "abom": ABoMProvider,
    "eccc": ECCCProvider,
    "eauf": EauFProvider,
    "ukea": UKEAProvider,
    "usgs": USGSProvider,
}

__all__ = ["PROVIDER_MAP", "BaseProvider"]
