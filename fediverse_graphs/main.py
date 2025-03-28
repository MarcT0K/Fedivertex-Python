import mlcroissant as mlc
import networkx as nx
import pandas as pd

CROISSANT_URL = "https://www.kaggle.com/datasets/marcdamie/fediverse-graph-dataset/croissant/download"
GRAPH_TYPES = {
    "bookwyrm": ["federation"],
    "friendica": ["federation"],
    "lemmy": ["federation", "cross_instance", "intra_instance"],
    "mastodon": ["federation", "active_users"],
    "misskey": ["federation", "active_users"],
    "peertube": ["federation"],
    "pleroma": ["federation", "active_users"],
}


def get_graph(
    software: str, type: str, date: str = "latest", format: str = "networkx"
) -> nx.Graph: ...


def get_graph_metadata(
    software: str, type: str, date: str = "latest"
) -> pd.Dataframe: ...


# Useful functions
# ds.metadata.record_sets
# ds.records("bookwyrm/federation/20250203/interactions.csv")
