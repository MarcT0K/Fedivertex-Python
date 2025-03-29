import pytest

from fediverse_graphs import GraphLoader


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
        assert loader.list_valid_graphs(software) == loader.VALID_GRAPH_TYPES[software]

    with pytest.raises(ValueError):
        loader.list_valid_graphs("NON-EXISTING SOFTWARE")


def test_available_dates():
    loader = GraphLoader()
    peertube_dates = loader.list_available_dates("peertube", "federation")
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
    assert loader._fetch_latest_date("peertube", "federation") == peertube_dates[-1]


def test_get_graph():
    loader = GraphLoader()

    with pytest.raises(ValueError):
        loader.get_graph("NON-EXISTING", "federation")

    with pytest.raises(ValueError):
        loader.get_graph("peertube", "NON-EXISTING")

    # No error with latest date
    for software, graph_type_list in loader.VALID_GRAPH_TYPES.items():
        if software == "mastodon":  # To avoid parsing the massive mastodon graph
            continue

        for graph_type in graph_type_list:
            date = loader._fetch_latest_date(software, graph_type)

            graph = loader.get_graph(software, graph_type)

            csv_file = f"{software}/{graph_type}/{date}/interactions.csv"
            records = loader.dataset.records(csv_file)

            assert graph.number_of_edges() == len(list(records))

    # Check graph consistency TODO
    # peertube_graph = loader.get_graph("peertube", "federation", "20250324")
    # assert peertube_graph.number_of_edges() == 33019
    # assert peertube_graph.number_of_nodes() == 883


def test_get_graph_metadata():
    loader = GraphLoader()

    with pytest.raises(ValueError):
        loader.get_graph_metadata("NON-EXISTING", "federation")

    with pytest.raises(ValueError):
        loader.get_graph_metadata("peertube", "NON-EXISTING")

    # No error with latest date
    peertube_graph_metadata = loader.get_graph_metadata("peertube", "federation")

    # Check graph consistency
    peertube_graph_metadata = loader.get_graph_metadata(
        "peertube", "federation", "20250324"
    )
    assert peertube_graph_metadata.shape[0] == 883
