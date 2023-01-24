from pathlib import Path
from typing import Union

import pandas as pd
import numpy as np

from tqdm import tqdm

from mostra.paths import create_folder, get_data_path

import warnings

from mostra.plots import create_plot_with_stops, COLORS_PER_TRANSPORT, \
    create_plot_with_stops_and_transport

warnings.filterwarnings('ignore')


class TransportDataExplorer:
    """
    Class for exploring transport data, creating visualizations and
    perform preprocessing
    """

    def __init__(self, dataframe: pd.DataFrame):
        self.dataframe = dataframe

    def prepare_plots_stops_per_route(self, folder_to_save: Union[Path, str]):
        """
        Rus
        Генерирует графики где приезд транспортных средств упорядочен по времени для
        выбранных остановок. Остановки также упорядочены снизу вверх в порядке
        движения транспорта по маршруту

        TODO Выяснить есть ли более надежный способ задавать порядок остановок. В
            production решении они должны определяться однозначно из базы данных
        """
        folder_to_save = create_folder(folder_to_save)
        stop_names, routes_names = self.load_stops_info()

        route_path_ids = list(self.dataframe['route_path_id'].unique())
        route_path_ids.sort()

        for route_name, df_vis, tm_id_df, stops_order, transport_i in self._preprocess_dataframe_for_vis(route_path_ids,
                                                                                                         stop_names,
                                                                                                         routes_names):
            # Generate plot
            create_plot_with_stops(route_name, stops_order, df_vis,
                                   tm_id_df, folder_to_save, transport_i)

    def prepare_plots_track_transport(self, folder_to_save: Union[Path, str]):
        folder_to_save = create_folder(folder_to_save)
        stop_names, routes_names = self.load_stops_info()

        route_path_ids = list(self.dataframe['route_path_id'].unique())
        route_path_ids.sort()

        for route_name, df_vis, tm_id_df, stops_order, transport_i in self._preprocess_dataframe_for_vis(route_path_ids,
                                                                                                         stop_names,
                                                                                                         routes_names):
            if len(list(df_vis['tmId'].unique())) > len(COLORS_PER_TRANSPORT):
                continue

            create_plot_with_stops_and_transport(route_name, stops_order,
                                                 df_vis, folder_to_save)

    def _preprocess_dataframe_for_vis(self, route_path_ids: list,
                                      stop_names: pd.DataFrame,
                                      routes_names: pd.DataFrame):
        for route_path_id in route_path_ids:
            print(f'Create plot for route {route_path_id}')

            route_df = self.dataframe[self.dataframe['route_path_id'] == route_path_id]

            # Prepare dataframe for visualization
            try:
                df_vis, route_path_name = enrich_with_route_stop_name(route_df,
                                                                      route_path_id,
                                                                      stop_names,
                                                                      routes_names)
            except Exception as ex:
                # Skip incorrect cases
                print(f'Skip {route_path_id} due to {ex}')
                continue

            grouped_by_transport = df_vis.groupby('tmId').agg({'stop_name': 'count'})
            grouped_by_transport = grouped_by_transport.reset_index()
            grouped_by_transport['tmId'] = grouped_by_transport['tmId'].replace({0: np.nan})
            grouped_by_transport = grouped_by_transport.dropna()
            grouped_by_transport = grouped_by_transport.reset_index()

            if len(grouped_by_transport) < 2:
                continue

            max_id = np.argmax(np.array(grouped_by_transport['stop_name']))
            transport_to_check = grouped_by_transport['tmId'].iloc[max_id]

            ######################################
            # Search for appropriate stops order #
            ######################################
            tm_id_df = df_vis[df_vis['tmId'] == transport_to_check]
            tm_id_df = tm_id_df[tm_id_df['byTelemetry'] == 0]
            if len(tm_id_df['stop_name'].unique()) != len(
                    df_vis['stop_name'].unique()):
                print(f'We can miss several stops during analysis - skip current route_path_id')
                continue
            tm_id_df = tm_id_df.sort_values(by='forecast_time_datetime')
            stops_order = list(tm_id_df['stop_name'].unique())

            yield route_path_name, df_vis, tm_id_df, stops_order, transport_to_check

    @staticmethod
    def load_stops_info():
        """ Load and return dataframe with information about stops """
        stop_from_repo = pd.read_csv(Path(get_data_path(), 'stop_from_repo.csv'))
        stop_names = stop_from_repo[['stop_id', 'name']]
        stop_names = stop_names.drop_duplicates()

        routes_names = stop_from_repo[['route_path_id', 'transport_type', 'number']]
        routes_names = routes_names.drop_duplicates()

        return stop_names, routes_names


def enrich_with_route_stop_name(route_df: pd.DataFrame, route_path_id,
                                stop_names, routes_names):
    """
    Rus
    Дополняет данные названиями остановок и названиями маршрутов
    """
    # Iterate through stops
    stop_ids = list(route_df['stop_id'].unique())
    stop_ids.sort()

    pbar = tqdm(stop_ids, colour='blue')

    df_vis = []
    route_path_name = 'default'
    for stop_id in pbar:
        pbar.set_description(f'Enrich {route_path_id} with {stop_id}')

        stop_df = route_df[route_df['stop_id'] == stop_id]
        if len(stop_df) < 1:
            continue

        stop_names_local = stop_names[stop_names['stop_id'] == stop_id]
        stop_name = stop_names_local['name'].iloc[0]

        # Add columns for convenient debug checking
        stop_df['forecast_time_datetime'] = pd.to_datetime(stop_df['forecast_time'], unit='s')
        stop_df['request_time_datetime'] = pd.to_datetime(stop_df['request_time'], unit='s')
        stop_df = stop_df.sort_values(by=['request_time_datetime', 'forecast_time_datetime'])

        stop_df = stop_df.merge(routes_names, on='route_path_id')
        stop_df = stop_df.drop_duplicates()
        transport_type = stop_df['transport_type'].iloc[0]
        number = stop_df['number'].iloc[0]
        route_path_name = f'{transport_type} {number}'

        stop_df['stop_name'] = [stop_name] * len(stop_df)

        df_vis.append(stop_df)

    df_vis = pd.concat(df_vis)
    route_path_name = route_path_name.replace('bus', 'Автобус')
    route_path_name = route_path_name.replace('tram', 'Трамвай')

    return df_vis, route_path_name
