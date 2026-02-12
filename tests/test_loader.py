import pytest

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

    with pytest.raises(ValueError):
        loader.list_graph_types("NON-EXISTING SOFTWARE")


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


def test_index_selection():
    loader = GraphLoader()

    with pytest.raises(ValueError):
        loader._fetch_date_index("peertube", "follow", 10000000000000000000000000)

    assert loader._fetch_date_index("peertube", "follow", 0) == "20250203"

    latest_date = loader._fetch_latest_date("peertube", "follow")
    assert loader._fetch_date_index("peertube", "follow", -1) == latest_date


def test_get_graph():
    loader = GraphLoader()

    with pytest.raises(ValueError):
        loader.get_graph("NON-EXISTING", "federation")

    with pytest.raises(ValueError):
        loader.get_graph("peertube", "NON-EXISTING")

    with pytest.raises(ValueError):
        loader.get_graph("peertube", "follow", date="20250203", index=3)

    # No error with latest date
    for software, graph_type_list in loader.VALID_GRAPH_TYPES.items():
        if software == "mastodon":  # To avoid parsing the massive mastodon graph
            continue

        for graph_type in graph_type_list:
            date = loader._fetch_latest_date(software, graph_type)

            # Test date selection
            graph1 = loader.get_graph(software, graph_type, date=date)

            if not graph_type == "federation":  # Because Federation is undirected
                csv_file = f"{software}/{graph_type}/{date}/interactions.csv"
                records = loader.dataset.records(csv_file)

                assert graph1.number_of_edges() == len(list(records))

            # Test index selection
            graph2 = loader.get_graph(software, graph_type, index=-1)
            assert graph1.number_of_edges() == graph2.number_of_edges()

            ########### FURTHER TESTS ###############
            available_dates = loader.list_available_dates(software, graph_type)
            date = available_dates[0]
            graph3 = loader.get_graph(software, graph_type, date=date)

            if not graph_type == "federation":  # Because Federation is undirected
                csv_file = f"{software}/{graph_type}/{date}/interactions.csv"
                records = loader.dataset.records(csv_file)

                assert graph3.number_of_edges() == len(list(records))

            # Test index selection
            graph4 = loader.get_graph(software, graph_type, index=0)
            assert graph3.number_of_edges() == graph4.number_of_edges()

    # Check graph consistency
    peertube_graph = loader.get_graph("peertube", "follow", date="20250324")
    assert peertube_graph.number_of_edges() == 19171
    assert peertube_graph.number_of_nodes() == 883

    # Check node attributes
    assert peertube_graph.nodes["aperi[DOT]tube"] == {
        "domain": "tube",
        "totalUsers": 39,
        "totalDailyActiveUsers": 0.0,
        "totalWeeklyActiveUsers": 4.0,
        "totalMonthlyActiveUsers": 8.0,
        "totalLocalVideos": 638,
        "totalVideos": 1287,
        "totalLocalPlaylists": 26.0,
        "totalVideoComments": 4632,
        "totalLocalVideoComments": 44,
        "totalLocalVideoViews": 106216,
        "serverVersion": "7.1.0",
    }

    # Check largest component consistency
    peertube_graph = loader.get_graph(  # DIRECTED GRAPH
        "peertube", "follow", date="20250324", only_largest_component=True
    )
    assert peertube_graph.number_of_edges() == 7450
    assert peertube_graph.number_of_nodes() == 264

    bookwyrm_graph = loader.get_graph(
        "bookwyrm", "federation", date="20250324", only_largest_component=True
    )
    assert bookwyrm_graph.number_of_nodes() == 70
    assert bookwyrm_graph.number_of_edges() == 1827


def test_get_temporal_graph():
    loader = GraphLoader()

    with pytest.raises(ValueError):
        loader.get_temporal_graph("NON-EXISTING", "federation")

    with pytest.raises(ValueError):
        loader.get_temporal_graph("peertube", "NON-EXISTING")

    with pytest.raises(ValueError):
        loader.get_temporal_graph(
            "peertube", "follow", date=("20250203", "20250217"), index=(3, 7)
        )

    with pytest.raises(ValueError):
        loader.get_temporal_graph("peertube", "follow", index=(-1, 7))

    with pytest.raises(ValueError):
        loader.get_temporal_graph("peertube", "follow", index=(3, 70000000000))

    with pytest.raises(ValueError):
        loader.get_temporal_graph("peertube", "follow", date=("20210203", "20210217"))

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
