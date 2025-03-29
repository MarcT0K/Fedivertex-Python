import json
from types import NoneType
from typing import List
import mlcroissant as mlc
import networkx as nx
import pandas as pd
from tqdm import tqdm


class GraphLoader:
    VALID_GRAPH_TYPES = {
        "bookwyrm": ["federation"],
        "friendica": ["federation"],
        "lemmy": ["federation", "cross_instance", "intra_instance"],
        "mastodon": ["federation", "active_user"],
        "misskey": ["federation", "active_user"],
        "peertube": ["federation"],
        "pleroma": ["federation", "active_user"],
    }

    def __init__(
        self,
        url="https://www.kaggle.com/datasets/marcdamie/fediverse-graph-dataset/croissant/download",
    ):
        try:
            self.dataset = mlc.Dataset(jsonld=url)
        except json.JSONDecodeError as err:
            raise SystemError(
                "Unexpected error from Croissant (try to empty Croissant's cache in ~/.cache/croissant)"
            ) from err

    def _check_input(self, software: str, graph_type: str) -> NoneType:
        if software not in self.VALID_GRAPH_TYPES.keys():
            raise ValueError(
                f"Invalid software! Valid software: {list(self.VALID_GRAPH_TYPES.keys())}"
            )

        if graph_type not in self.VALID_GRAPH_TYPES[software]:
            raise ValueError(
                f"{graph_type} is not a valid graph type for {software}. Valid types: {self.VALID_GRAPH_TYPES[software]}"
            )

    def _fetch_latest_date(self, software: str, graph_type: str) -> str:
        dates = self.list_available_dates(software, graph_type)

        if len(dates) == 0:
            raise ValueError(f"No graph available for {software}+{graph_type}")

        return dates[-1]

    def list_all_software(self) -> List[str]:
        return list(self.VALID_GRAPH_TYPES.keys())

    def list_graph_types(self, software: str) -> List[str]:
        if software not in self.VALID_GRAPH_TYPES.keys():
            raise ValueError(
                f"Invalid software! Valid software: {list(self.VALID_GRAPH_TYPES.keys())}"
            )

        return self.VALID_GRAPH_TYPES[software]

    def list_available_dates(self, software: str, graph_type: str) -> List[str]:
        self._check_input(software, graph_type)

        record_sets = list(self.dataset.metadata.record_sets)
        dates = []
        for record_set in record_sets:
            if "interactions.csv" not in record_set.uuid:
                continue

            software_i, graph_type_i, date_i, _file = record_set.uuid.split("/")
            if software_i == software and graph_type_i == graph_type:
                dates.append(date_i)

        dates.sort()
        return dates

    def get_graph(
        self, software: str, graph_type: str, date: str = "latest"
    ) -> nx.Graph:
        self._check_input(software, graph_type)

        if date == "latest":
            date = self._fetch_latest_date(software, graph_type)

        csv_file = f"{software}/{graph_type}/{date}/interactions.csv"
        records = self.dataset.records(csv_file)

        graph = nx.DiGraph()

        for record in tqdm(records, desc="Building the graph"):
            source = record[csv_file + "/Source"].decode()
            target = record[csv_file + "/Target"].decode()
            weight = record[csv_file + "/Weight"]
            graph.add_edge(source, target, weight=weight)

        return graph

    def get_graph_metadata(
        self, software: str, graph_type: str, date: str = "latest"
    ) -> pd.DataFrame:
        self._check_input(software, graph_type)

        if date == "latest":
            date = self._fetch_latest_date(software, graph_type)

        csv_file = f"{software}/{graph_type}/{date}/instances.csv"
        records = self.dataset.records(csv_file)

        df = pd.DataFrame(records)

        # Sanitize the column name
        df = df.rename(columns={col: col.split("/")[-1] for col in df.columns})
        return df
