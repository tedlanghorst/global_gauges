from ._base import BaseProvider  # exposed for type hinting.
from .abom import ABoMProvider
from .eccc import ECCCProvider
from .eauf import EauFProvider
from .ukea import UKEAProvider
from .usgs import USGSProvider
from .brana import BrANAProvider
from .krwamis import KrWAMISProvider

# Map provider names to their classes
PROVIDER_MAP = {
    "abom": ABoMProvider,
    "eccc": ECCCProvider,
    "eauf": EauFProvider,
    "ukea": UKEAProvider,
    "usgs": USGSProvider,
    "brana": BrANAProvider,
    "krwamis": KrWAMISProvider,
}

__all__ = ["PROVIDER_MAP", "BaseProvider"]
