# Python API to interact with Fedivertex, the Fediverse Graph Dataset

This Python package provides a simple interface to interact with Fedivertex: https://www.kaggle.com/datasets/marcdamie/fediverse-graph-dataset/data.
Our package automatically downloads the dataset from Kaggle and loads graphs in a usable format (i.e., NetworkX).

The Fediverse Graph dataset provides graphs for different decentralized social media.
These graphs represents the interactions between servers in these decentralized social media.
The graph type corresponds to the type of interactions modelled by the graph.
Finally, the dataset provides the graphs obtained on different dates, so the users can analyze the evolution of the interactions.

Refer to this [repository](https://github.com/MarcT0K/Franck) to discover more about the data acquisition.

## Extracting a graph 

Three pieces of information are necessary to select a graph in the datatset: the software/social media, the graph type, and the date.

We provide graphs using the [NetworkX](https://networkx.org/) format.

**Example**:

```python3
    from fedivertex import GraphLoader

    loader = GraphLoader()
    graph = loader.get_graph(software="peertube", graph_type="follow", date="20250324")
    graph = loader.get_graph(software="peertube", graph_type="follow") # Loads the most recent graph
```


In each graph, we also provide metadata in the attributes of the graph nodes.

## Utility functions

Finally, we provide a few utility functions:

```python3
    from fedivertex import GraphLoader

    loader = GraphLoader()
    loader.list_all_software()
    loader.list_graph_types("peertube")
    loader.list_available_dates("peertube", "follow")
```
