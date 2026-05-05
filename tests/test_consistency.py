import pytest

from fedivertex import GraphLoader


def test_index_selection():
    loader = GraphLoader()

    assert loader._fetch_date_index("peertube", "follow", 0) == "20250203"

    latest_date = loader._fetch_latest_date("peertube", "follow")
    assert loader._fetch_date_index("peertube", "follow", -1) == latest_date


def _iter_software_graph():
    loader = GraphLoader(
        cache_only=True
    )  # Avoids to fetch the metadata again and again
    for software, graph_types in loader.VALID_GRAPH_TYPES.items():
        for graph_type in graph_types:
            if software == "mastodon" and graph_type == "federation":
                continue
            yield software, graph_type


@pytest.mark.parametrize("software,graph_type", list(_iter_software_graph()))
def test_get_graph_selection(software, graph_type):
    loader = GraphLoader(cache_only=True)

    date = loader._fetch_latest_date(software, graph_type)

    # Test date selection
    graph1 = loader.get_graph(software, graph_type, date=date)

    if not graph_type == "federation":  # Because Federation is undirected
        csv_file = (
            loader.DATASET_INFO.dataset_dir
            / software
            / graph_type
            / date
            / "interactions.csv"
        )

        with open(csv_file, "r", encoding="utf-8") as f:
            line_count = sum(1 for _ in f)
            line_count -= 1  # Remove the header from the count

        assert graph1.number_of_edges() == line_count

    # Test index selection
    graph2 = loader.get_graph(software, graph_type, index=-1)
    assert graph1.number_of_edges() == graph2.number_of_edges()

    available_dates = loader.list_available_dates(software, graph_type)
    date = available_dates[0]
    graph3 = loader.get_graph(software, graph_type, date=date)

    graph4 = loader.get_graph(software, graph_type, index=0)
    assert graph3.number_of_edges() == graph4.number_of_edges()


def _iter_software_graph_date():
    loader = GraphLoader(cache_only=True)
    for software, graph_types in loader.VALID_GRAPH_TYPES.items():
        for graph_type in graph_types:
            if graph_type == "federation":  # Because we want directed graphs
                continue
            for date in loader.list_available_dates(software, graph_type):
                yield software, graph_type, date


@pytest.mark.parametrize("software,graph_type,date", list(_iter_software_graph_date()))
def test_get_graph_sizes(software, graph_type, date):
    loader = GraphLoader(cache_only=True)

    graph = loader.get_graph(software, graph_type, date=date)
    csv_file = (
        loader.DATASET_INFO.dataset_dir
        / software
        / graph_type
        / date
        / "interactions.csv"
    )

    with open(csv_file, "r", encoding="utf-8") as f:
        line_count = sum(1 for _ in f)
        line_count -= 1  # Remove the header from the count

    assert graph.number_of_edges() == line_count  # Verify that we load all the edges
    # NB: an error can also occur in case of data cleaning issue in the dataset


def test_graph_consistency():
    loader = GraphLoader(cache_only=True)

    # Check graph consistency
    peertube_graph = loader.get_graph("peertube", "follow", date="20250324")
    assert peertube_graph.number_of_edges() == 19171
    assert peertube_graph.number_of_nodes() == 883

    # Check node attributes
    assert peertube_graph.nodes["aperi[DOT]tube"] == {
        "domain": "tube",
        "totalUsers": "39",
        "totalDailyActiveUsers": "0.0",
        "totalWeeklyActiveUsers": "4.0",
        "totalMonthlyActiveUsers": "8.0",
        "totalLocalVideos": "638",
        "totalVideos": "1287",
        "totalLocalPlaylists": "26.0",
        "totalVideoComments": "4632",
        "totalLocalVideoComments": "44",
        "totalLocalVideoViews": "106216",
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
