import datetime
from pathlib import Path
from typing import Union, Any

import pandas as pd
import numpy as np

from tqdm import tqdm
import matplotlib.pyplot as plt

from mostra.data_structure import COLUMN_NAMES
from mostra.paths import get_data_path

import warnings
warnings.filterwarnings('ignore')


def prepare_df_for_visualization(route_df: pd.DataFrame, route_path_id,
                                 stop_names, stop_from_repo):
    # Iterate through stops
    stop_ids = list(route_df['stop_id'].unique())
    stop_ids.sort()

    pbar = tqdm(stop_ids, colour='blue')

    # Generate plot for each stop in the route - collect information about
    # stops and stations
    df_vis = []
    route_path_name = 'default'
    for stop_id in pbar:
        pbar.set_description(f'Check {route_path_id} with {stop_id}')

        stop_df = route_df[route_df['stop_id'] == stop_id]
        if len(stop_df) < 1:
            continue

        stop_names_local = stop_names[stop_names['stop_id'] == stop_id]
        stop_name = stop_names_local['name'].iloc[0]

        # Add columns for convenient debug checking
        stop_df['forecast_time_datetime'] = pd.to_datetime(stop_df['forecast_time'], unit='s')
        stop_df['request_time_datetime'] = pd.to_datetime(stop_df['request_time'], unit='s')
        stop_df = stop_df.sort_values(by=['request_time_datetime', 'forecast_time_datetime'])

        stop_df = stop_df.merge(stop_from_repo, on='route_path_id')
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


def create_plot_with_stops(route_path_name: str,
                           stops_order: list,
                           df_vis: pd.DataFrame,
                           tm_id_df: pd.DataFrame,
                           folder_to_save: Path,
                           transport_to_check: Any):
    fig_size = (15, 10.0)
    fig, ax = plt.subplots(1, figsize=fig_size)
    for i, stop in enumerate(stops_order):
        stop_df = df_vis[df_vis['stop_name'] == stop]
        stop_df = stop_df.sort_values(by='forecast_time_datetime')

        tel_data = stop_df[stop_df['byTelemetry'] == 1]
        scheduled_data = stop_df[stop_df['byTelemetry'] == 0]

        if len(tel_data) > 1:
            ax.scatter(tel_data['forecast_time_datetime'], [i] * len(tel_data),
                       c='red', alpha=0.8, s=50, edgecolors={'#FFBAAC'})
        if len(scheduled_data) > 1:
            ax.scatter(scheduled_data['forecast_time_datetime'],
                       [i] * len(scheduled_data),
                       c='blue', alpha=0.8, s=50, edgecolors={'#BCE7FF'})

    # Add line for desired (main) transport
    agg = tm_id_df.groupby('stop_name').agg({'forecast_time_datetime': 'first'})
    agg = agg.reset_index()
    agg['stop_name'] = pd.Categorical(agg['stop_name'], stops_order)
    agg = agg.sort_values(by='stop_name')
    ax.plot(agg['forecast_time_datetime'], agg['stop_name'], '--',
            c='black',
            label=f'Отдельно выбранное транспортное средство id: {transport_to_check}')

    if route_path_name == 'Трамвай 21':
        min_x = datetime.datetime.strptime("24072022T13:30",
                                           "%d%m%YT%H:%M")

        max_x = datetime.datetime.strptime("24072022T14:30",
                                           "%d%m%YT%H:%M")

        ax.set_xlim(min_x, max_x)

    ax.grid()
    ax.set_yticklabels(stops_order)
    plt.yticks(range(len(stops_order)))
    ax.legend(fontsize=16)
    ax.set_xlabel('Дата')
    ax.set_ylabel('Остановки на маршруте')
    fig.suptitle(f'Маршрут {route_path_name}', fontsize=16)
    plot_name = f'{route_path_name.replace(".", "_")}.png'
    fig.savefig(Path(folder_to_save, plot_name),
                dpi=300, bbox_inches='tight')
    plt.close()


def prepare_plots_for_route(df: pd.DataFrame,
                            folder_to_save: Union[Path, str]):
    """
    Rus
    Генерирует графики где приезд транспортных средств упорядочен по времени для
    выбранных остановок. Остановки также упорядочены снизу вверхе в порядке
    маршрута

    TODO Выяснить есть ли более надежный способ задавать порядок остановок. В
        production решении они должны определяться однозначно из базы данных
    """
    if isinstance(folder_to_save, str):
        folder_to_save = Path(folder_to_save)
    folder_to_save = folder_to_save.resolve()
    folder_to_save.mkdir(parents=True, exist_ok=True)

    route_path_ids = list(df['route_path_id'].unique())
    route_path_ids.sort()

    stop_from_repo = pd.read_csv(Path(get_data_path(), 'stop_from_repo.csv'))
    stop_names = stop_from_repo[['stop_id', 'name']]
    stop_from_repo = stop_from_repo[['route_path_id', 'transport_type', 'number']]
    stop_from_repo = stop_from_repo.drop_duplicates()
    stop_names = stop_names.drop_duplicates()

    for route_path_id in route_path_ids:
        print(f'Create plot for route {route_path_id}')

        route_df = df[df['route_path_id'] == route_path_id]

        # Prepare dataframe for visualization
        try:
            df_vis, route_path_name = prepare_df_for_visualization(route_df, route_path_id,
                                                                   stop_names, stop_from_repo)
        except Exception:
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
        if len(tm_id_df['stop_name'].unique()) != len(df_vis['stop_name'].unique()):
            print(f'We can miss several stops')
            continue
        tm_id_df = tm_id_df.sort_values(by='forecast_time_datetime')
        stops_order = list(tm_id_df['stop_name'].unique())

        ##########################
        # Create plot with stops #
        ##########################
        create_plot_with_stops(route_path_name, stops_order, df_vis,
                               tm_id_df, folder_to_save, transport_to_check)

    return df
