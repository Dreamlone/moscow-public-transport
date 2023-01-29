import datetime
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


MINIMUM_NUMBER_OF_STOPS_PER_ROUTE_TO_SHOW = 5


class TransportDataExplorer:
    """
    Class for exploring transport data, creating visualizations and
    perform preprocessing
    """

    def __init__(self, dataframe: pd.DataFrame):
        self.dataframe = dataframe

    def prepare_plots_stops_per_route(self, folder_to_save: Union[Path, str],
                                      route_name_to_show: str = None):
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

        for route_name, df_vis, tm_id_df, transport_i, stops_order in self._preprocess_dataframe_for_vis(route_path_ids,
                                                                                                         stop_names,
                                                                                                         routes_names):
            # Generate plot
            if len(df_vis['stop_id'].unique()) < MINIMUM_NUMBER_OF_STOPS_PER_ROUTE_TO_SHOW:
                # Too few stops for visualizations
                continue
            if route_name_to_show is not None and route_name_to_show != route_name:
                continue

            create_plot_with_stops(route_name, stops_order, df_vis,
                                   tm_id_df, folder_to_save, transport_i)

    def prepare_plots_track_transport(self, folder_to_save: Union[Path, str]):
        folder_to_save = create_folder(folder_to_save)
        stop_names, routes_names = self.load_stops_info()

        route_path_ids = list(self.dataframe['route_path_id'].unique())
        route_path_ids.sort()

        for route_name, df_vis, tm_id_df, transport_i, stops_order in self._preprocess_dataframe_for_vis(route_path_ids,
                                                                                                         stop_names,
                                                                                                         routes_names):
            if len(list(df_vis['tmId'].unique())) > len(COLORS_PER_TRANSPORT):
                # Too many transports for visualization
                continue
            if len(df_vis['stop_id'].unique()) < MINIMUM_NUMBER_OF_STOPS_PER_ROUTE_TO_SHOW:
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

            if route_path_name == 'Трамвай 21':
                min_x = datetime.datetime.strptime("24072022T13:30",
                                                   "%d%m%YT%H:%M")
                max_x = datetime.datetime.strptime("24072022T14:30",
                                                   "%d%m%YT%H:%M")

                # Clip dataframes for further visualizations
                df_vis = df_vis[df_vis['forecast_time_datetime'] <= max_x]
                df_vis = df_vis[df_vis['forecast_time_datetime'] >= min_x]

            grouped_by_transport = df_vis.groupby('tmId').agg({'stop_id': 'count'})
            grouped_by_transport = grouped_by_transport.reset_index()
            grouped_by_transport['tmId'] = grouped_by_transport['tmId'].replace({0: np.nan})
            grouped_by_transport = grouped_by_transport.dropna()
            grouped_by_transport = grouped_by_transport.reset_index()

            if len(grouped_by_transport) < 2:
                continue

            max_id = np.argmax(np.array(grouped_by_transport['stop_id']))
            transport_to_check = grouped_by_transport['tmId'].iloc[max_id]

            ######################################
            # Search for appropriate stops order #
            ######################################
            tm_id_df = df_vis[df_vis['tmId'] == transport_to_check]
            tm_id_df = tm_id_df[tm_id_df['byTelemetry'] == 0]

            if len(tm_id_df) < 1:
                continue

            tm_id_df = tm_id_df.sort_values(by='forecast_time_datetime')
            stops_order_with_coordinates = get_stops_order(route_path_name, tm_id_df)

            yield route_path_name, df_vis, tm_id_df, transport_to_check, stops_order_with_coordinates

    @staticmethod
    def load_stops_info():
        """ Load and return dataframe with information about stops """
        stop_from_repo = pd.read_csv(Path(get_data_path(), 'stop_from_repo.csv'))
        stop_names = stop_from_repo[['stop_id', 'name', 'lon', 'lat']]
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
        lon = stop_names_local['lon'].iloc[0]
        lat = stop_names_local['lat'].iloc[0]

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
        stop_df['lon'] = [lon] * len(stop_df)
        stop_df['lat'] = [lat] * len(stop_df)
        df_vis.append(stop_df)

    df_vis = pd.concat(df_vis)
    route_path_name = route_path_name.replace('bus', 'Автобус')
    route_path_name = route_path_name.replace('tram', 'Трамвай')

    return df_vis, route_path_name


def get_stops_order(route_path_name: str, tm_id_df: pd.DataFrame):
    stops_order_with_coordinates = tm_id_df.groupby(['stop_id']).agg(
        {'forecast_time_datetime': 'min',
         'stop_name': 'first',
         'lon': 'first',
         'lat': 'first'})

    stops_order_with_coordinates = stops_order_with_coordinates.reset_index()

    if route_path_name == 'Трамвай 21':
        stops_order = ['3add7f03-88a9-4b18-8e98-3f8a1e320395',
                       '87458dd6-d9b0-4a0b-bec7-e477670e30b4',
                       'cce490df-6768-4d5b-a4a2-3e4bfa048423',
                       'bccae0bc-0afc-4529-be61-bcd5200b2b27',
                       '3aa51b3f-b632-4769-9faf-b4f58da3c81f',
                       '9de022e2-7c37-4761-a197-057a9bc0228e',
                       '4c5233a2-e6de-41b4-b147-2e1a6220b3d3',
                       'f163ffd5-c52a-4547-92ff-357a99fe1f14',
                       '19b57055-62e4-4ce1-8dbd-0af52328681c',
                       '1ccf60a5-ea97-448f-b5a3-2dd30958af09',
                       'b853633d-5028-413b-ba72-d0d371bee6bd',
                       'd2c961a2-2b6f-4738-967d-1e80e6bd05d3',
                       'ebe8630e-3493-44cf-8a56-88284831bba8',
                       'a97cfd4e-0c67-40b9-a6c2-090155384554',
                       '9e531bb4-ff64-425a-b3ae-f63f9f25199d',
                       '562e1d18-90a1-4864-833f-7dd3baf8dc31',
                       '60a80664-9b63-47ed-bbab-56d53fe9c5a6',
                       '17d85ad7-b6ae-4795-ac5b-90272bef5731',
                       'c6e42455-12b0-41b6-8022-7debd8fd016e']
        stops_order_with_coordinates['stop_id'] = pd.Categorical(stops_order_with_coordinates['stop_id'],
                                                                 stops_order)
        stops_order_with_coordinates = stops_order_with_coordinates.sort_values(by='stop_id')
        stops_order_with_coordinates = stops_order_with_coordinates.drop_duplicates()
        return stops_order_with_coordinates

    stops_order_with_coordinates = stops_order_with_coordinates.sort_values(by='forecast_time_datetime')
    stops_order_with_coordinates = stops_order_with_coordinates.drop_duplicates()

    return stops_order_with_coordinates
