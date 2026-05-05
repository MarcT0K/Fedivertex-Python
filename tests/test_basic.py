from fedivertex import GraphLoader


def test_basic_lists():
    software_list = [
        "bookwyrm",
        "friendica",
        "lemmy",
        "mastodon",
        "misskey",
        "peertube",
        "pleroma",
    ]

    loader = GraphLoader()
    assert loader.list_all_software() == software_list

    for software in software_list:
        assert loader.list_graph_types(software) == loader.VALID_GRAPH_TYPES[software]


def test_available_dates():
    loader = GraphLoader()
    peertube_dates = loader.list_available_dates("peertube", "follow")
    assert set(peertube_dates).issuperset(
        {
            "20250203",
            "20250210",
            "20250217",
            "20250224",
            "20250303",
            "20250311",
            "20250317",
            "20250324",
        }
    )

    peertube_dates.sort()
    assert loader._fetch_latest_date("peertube", "follow") == peertube_dates[-1]


def test_get_temporal_graph():
    loader = GraphLoader()

    temporal_graph = loader.get_temporal_graph(
        "peertube", "follow", date=("20250203", "20250617")
    )
    assert len(temporal_graph.temporal_nodes()) == 1157
    assert len(temporal_graph.temporal_edges()) == 310695
    assert temporal_graph.number_of_snapshots() == 20

    temporal_graph = loader.get_temporal_graph("peertube", "follow", index=(0, 7))
    assert len(temporal_graph.temporal_nodes()) == 991
    assert len(temporal_graph.temporal_edges()) == 133852
    assert temporal_graph.number_of_snapshots() == 8
