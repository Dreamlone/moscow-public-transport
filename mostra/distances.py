import pandas as pd
import osmnx as ox
import networkx as nx


class DistanceCalculator:
    """
    Class for calculating distances between two points (stops) using OSM roads
    graph

    :param df_with_stops: dataframe with stops names and WGS84 coordinates
    """

    def __init__(self, df_with_stops: pd.DataFrame):
        self.df_with_stops = df_with_stops
        self.df_with_stops = self.df_with_stops.reset_index()

        self.radius = 1000
        self.network_type = 'all'
        self.default_distance = 200
        self.distances = []

    def get_cumulative_distance(self):
        """ Return distance in meters """
        cumulative_distance = 0
        for i, row in self.df_with_stops.iterrows():
            if i == 0:
                self.distances.append(0)
                yield 0, row
            else:
                # Launch distance calculation
                prev_row = self.df_with_stops.iloc[i - 1]
                point = (prev_row['lat'], prev_row['lon'])
                streets_graph = ox.graph_from_point(point, dist=self.radius,
                                                    network_type=self.network_type)
                origin_node = ox.nearest_nodes(streets_graph, prev_row['lon'], prev_row['lat'])
                current_node = ox.nearest_nodes(streets_graph, row['lon'], row['lat'])

                try:
                    path_length = nx.shortest_path_length(streets_graph,
                                                          origin_node,
                                                          current_node,
                                                          weight='length')
                except nx.NetworkXNoPath:
                    # There is no networkx path between this nodes
                    path_length = self.default_distance

                if path_length < self.default_distance:
                    path_length = self.default_distance
                cumulative_distance += path_length
                print(f'Process stop number {i}. Distance: {path_length}')
                self.distances.append(cumulative_distance)
                yield cumulative_distance, row
