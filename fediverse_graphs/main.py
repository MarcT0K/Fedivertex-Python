from types import NoneType
import mlcroissant as mlc
import networkx as nx
import pandas as pd

CROISSANT_URL = "https://www.kaggle.com/datasets/marcdamie/fediverse-graph-dataset/croissant/download"
VALID_GRAPH_TYPES = {
    "bookwyrm": ["federation"],
    "friendica": ["federation"],
    "lemmy": ["federation", "cross_instance", "intra_instance"],
    "mastodon": ["federation", "active_users"],
    "misskey": ["federation", "active_users"],
    "peertube": ["federation"],
    "pleroma": ["federation", "active_users"],
}
DATASET_INTERFACE = mlc.Dataset(jsonld=CROISSANT_URL)


def _check_input(software: str, graph_type: str) -> NoneType:
    if software not in VALID_GRAPH_TYPES.keys():
        raise ValueError(
            f"Invalid software! Valid software: {list(VALID_GRAPH_TYPES.keys())}"
        )

    if graph_type not in VALID_GRAPH_TYPES[software]:
        raise ValueError(
            f"{graph_type} is not a valid graph type for {software}. Valid types: {VALID_GRAPH_TYPES[software]}"
        )


def _fetch_latest_date(software: str, graph_type: str) -> str:
    # ds.metadata.record_sets
    raise NotImplementedError  # TODO


def get_graph(software: str, graph_type: str, date: str = "latest") -> nx.Graph:
    _check_input(software, graph_type)

    if date == "latest":
        date = _fetch_latest_date(software, graph_type)

    csv_file = f"{software}/{graph_type}/{date}/interactions.csv"
    records = DATASET_INTERFACE.records(csv_file)

    G = nx.Graph()

    for record in records:
        source = record[csv_file + "/Source"].decode()
        target = record[csv_file + "/Target"].decode()
        weight = record[csv_file + "/Weight"]
        G.add_edge(source, target, weight=weight)

    return G


def get_graph_metadata(
    software: str, graph_type: str, date: str = "latest"
) -> pd.Dataframe:
    _check_input(software, graph_type)

    if date == "latest":
        date = _fetch_latest_date(software, graph_type)

    csv_file = f"{software}/{graph_type}/{date}/interactions.csv"
    records = DATASET_INTERFACE.records(csv_file)

    df = pd.DataFrame(records)

    # Sanitize the column name
    df = df.rename(columns={col: col.split("/")[-1] for col in df.columns})
    return df
