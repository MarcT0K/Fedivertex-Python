import json
from types import NoneType
from typing import List, Optional
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
    UNDIRECTED_GRAPHS = ["federation"]

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
        """Verify that (software,graph type) combination exists

        :param software: software name
        :type software: str
        :param graph_type: graph type
        :type graph_type: str
        :raises ValueError: if the software does not exist in the dataset
        :raises ValueError: if the graph type does not exist for a given software
        :return: Nothing
        :rtype: NoneType
        """
        if software not in self.VALID_GRAPH_TYPES.keys():
            raise ValueError(
                f"Invalid software! Valid software: {list(self.VALID_GRAPH_TYPES.keys())}"
            )

        if graph_type not in self.VALID_GRAPH_TYPES[software]:
            raise ValueError(
                f"{graph_type} is not a valid graph type for {software}. Valid types: {self.VALID_GRAPH_TYPES[software]}"
            )

    def _fetch_date_index(self, software: str, graph_type: str, index: int) -> str:
        """Returns the i-th date available for a given graph type.
        The dates are sorted increasingly.

        :param software:
        :type software: str
        :param graph_type:
        :type graph_type: str
        :param index:
        :type index: int
        :raises ValueError: if there is no graph available of the given type.
        :raises ValueError: if the index is invalid
        :return: date
        :rtype: str
        """
        dates = self.list_available_dates(software, graph_type)

        if len(dates) == 0:
            raise ValueError(f"No graph available for {software}+{graph_type}")

        try:
            return dates[index]
        except Exception as err:
            raise ValueError("Invalid index: " + str(index)) from err

    def _fetch_latest_date(self, software: str, graph_type: str) -> str:
        """Returns the latest date available for a given graph.

        :param software:
        :type software: str
        :param graph_type:
        :type graph_type: str
        :raises ValueError: if there is no graph available of the given type.
        :return: date
        :rtype: str
        """
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
        """List all the dates available for a given software and graph type.

        :param software:
        :type software: str
        :param graph_type:
        :type graph_type: str
        :return: list of dates
        :rtype: List[str]
        """
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
        self,
        software: str,
        graph_type: str,
        index: Optional[int] = None,
        date: Optional[str] = None,
        only_largest_component: bool = False,
    ) -> nx.Graph:
        """Provide a graph for a given software and graph type.
        By default, we provide the latest graph but it can also be selected using the date or index.

        :param software:
        :type software: str
        :param graph_type:
        :type graph_type: str
        :param index: index of the graph, defaults to None
        :type index: Optional[int], optional
        :param date: date of the graph, defaults to None
        :type date: Optional[str], optional
        :param only_largest_component: only returns the largest connected component, defaults to False
        :type only_largest_component: bool, optional
        :raises ValueError: if both a date and an index are provided.
        :return: a graph in the NetworkX format
        :rtype: nx.Graph
        """
        self._check_input(software, graph_type)

        if index and date:
            raise ValueError(
                "You must provide either the date or the index of the graph, not both."
            )

        if index:
            date = self._fetch_date_index(software, graph_type, index)

        if index is None and date is None:
            # Fetch latest graph
            date = self._fetch_latest_date(software, graph_type)

        csv_file = f"{software}/{graph_type}/{date}/interactions.csv"
        records = self.dataset.records(csv_file)

        if graph_type in self.UNDIRECTED_GRAPHS:
            graph = nx.Graph()
        else:
            graph = nx.DiGraph()

        for record in tqdm(records, desc="Building the graph"):
            source = record[csv_file + "/Source"].decode()
            target = record[csv_file + "/Target"].decode()
            weight = record[csv_file + "/Weight"]
            graph.add_edge(source, target, weight=weight)

        if only_largest_component:
            largest_cc = max(nx.connected_components(graph), key=len)
            graph = graph.subgraph(largest_cc)

        return graph

    def get_graph_metadata(
        self, software: str, graph_type: str, date: str = "latest"
    ) -> pd.DataFrame:
        """Provide metadata (e.g., number of registered users) about the nodes in a graph.

        :param software:
        :type software: str
        :param graph_type:
        :type graph_type: str
        :param date: date of the graph, defaults to "latest"
        :type date: str, optional
        :return: Pandas dataframe containing all the metadata available
        :rtype: pd.DataFrame
        """
        self._check_input(software, graph_type)

        if date == "latest":
            date = self._fetch_latest_date(software, graph_type)

        csv_file = f"{software}/{graph_type}/{date}/instances.csv"
        records = self.dataset.records(csv_file)

        df = pd.DataFrame(records)

        # Sanitize the column name
        df = df.rename(columns={col: col.split("/")[-1] for col in df.columns})
        return df
