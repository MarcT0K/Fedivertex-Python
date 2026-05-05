import pytest

from fedivertex import GraphLoader
from fedivertex.exceptions import InteractionError


def test_list_error():
    loader = GraphLoader()

    with pytest.raises(InteractionError):
        loader.list_graph_types("NON-EXISTING SOFTWARE")


def test_index_selection_error():
    loader = GraphLoader()

    with pytest.raises(InteractionError):
        loader._fetch_date_index("peertube", "follow", 10000000000000000000000000)


def test_get_graph_errors():
    loader = GraphLoader()

    with pytest.raises(InteractionError):
        loader.get_graph("NON-EXISTING", "federation")

    with pytest.raises(InteractionError):
        loader.get_graph("peertube", "NON-EXISTING")

    with pytest.raises(InteractionError):
        loader.get_graph("peertube", "follow", date="20250203", index=3)


def test_get_temporal_graph_errors():
    loader = GraphLoader()

    with pytest.raises(InteractionError):
        loader.get_temporal_graph("NON-EXISTING", "federation")

    with pytest.raises(InteractionError):
        loader.get_temporal_graph("peertube", "NON-EXISTING")

    with pytest.raises(InteractionError):
        loader.get_temporal_graph(
            "peertube", "follow", date=("20250203", "20250217"), index=(3, 7)
        )

    with pytest.raises(InteractionError):
        loader.get_temporal_graph("peertube", "follow", index=(-1, 7))

    with pytest.raises(InteractionError):
        loader.get_temporal_graph("peertube", "follow", index=(3, 70000000000))

    with pytest.raises(InteractionError):
        loader.get_temporal_graph("peertube", "follow", date=("20210203", "20210217"))
