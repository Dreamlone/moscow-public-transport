from pathlib import Path
import matplotlib.pyplot as plt
import contextily as cx

import pandas as pd

from mostra.convert import prepare_points_layer
from mostra.paths import get_data_path


def show_stops():
    """
    Rus
    Показывает на карте Москвы как расположен остановки, попавшие в выборку
    """
    stops = pd.read_csv(Path(get_data_path(), 'stops.csv'),
                        names=['unknown_0', 'unknown_1', 'unknown_2',
                               'unknown_3', "stop_name", 'lon', 'lat'])
    stops = pd.read_csv(Path(get_data_path(), 'stop_from_repo.csv'))
    stops = stops[['stop_id', 'lat', 'lon']]
    stops = stops.drop_duplicates()

    # Convert pandas dataframe into geopandas GeoDataFrame
    stops = prepare_points_layer(stops)

    stops = stops.to_crs(3857)
    ax = stops.plot(color='red', alpha=0.2, markersize=10)
    cx.add_basemap(ax)
    plt.suptitle('Transport stops')
    plt.show()


if __name__ == '__main__':
    show_stops()
