from pathlib import Path
from turtle import clear

import pytest

from fedivertex import GraphLoader
from fedivertex.cache import DEFAULT_CACHE_DIR, clear_default_cache
from fedivertex.exceptions import CacheError, InteractionError


def test_list_error():
    loader = GraphLoader()

    with pytest.raises(InteractionError):
        loader.list_graph_types("NON-EXISTING SOFTWARE")


def test_cache_only_errors():
    cache_path = Path(DEFAULT_CACHE_DIR)
    assert cache_path.exists()
    loader = GraphLoader(cache_only=True)
    # No error because the cache exists

    # Cache corruption
    update_file_path = loader.DATASET_INFO.dataset_dir / "last_update.txt"
    update_file_path.unlink()
    with open(update_file_path, "w") as update_file:
        update_file.write("INVALID DATA")

    del loader

    assert cache_path.exists()
    with pytest.raises(CacheError):  # Corrupted cache
        _loader = GraphLoader(cache_only=True)

    clear_default_cache()

    assert not cache_path.exists()
    with pytest.raises(CacheError):  # Missing cache
        _loader = GraphLoader(cache_only=True)


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
