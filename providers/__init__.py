from .australia import AustraliaProvider

# from .defunct.brazil import BrazilProvider
from .canada import CanadaProvider

# from .defunct.chile import ChileProvider
# from .defunct.france import FranceProvider
# from .defunct.japan import JapanProvider
# from .defunct.sa import SouthAfricaProvider
from .uk import UKProvider
from .usa import USAProvider

# Map provider names to their classes
PROVIDER_MAP = {
    "australia": AustraliaProvider,
    # "brazil": BrazilProvider,
    "canada": CanadaProvider,
    # "chile": ChileProvider,
    # "france": FranceProvider,
    # "japan": JapanProvider,
    # "sa": SouthAfricaProvider,
    "uk": UKProvider,
    "usa": USAProvider,
}
