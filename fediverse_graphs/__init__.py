from .main import get_graph, get_graph_metadata
from pkg_resources import get_distribution

__version__ = get_distribution("fediverse-graphs").version
__license__ = "GPLv3"
