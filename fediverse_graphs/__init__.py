from .main import GraphLoader
from pkg_resources import get_distribution

__version__ = get_distribution("fediverse-graphs").version
__license__ = "GPLv3"
