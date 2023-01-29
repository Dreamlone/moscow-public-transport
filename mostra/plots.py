import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
import warnings

from mostra.data_structure import MIN_FORECAST_HORIZON_SECONDS
from mostra.distances import DistanceCalculator

warnings.filterwarnings('ignore')

# TODO refactor color related logic
COLORS_PER_TRANSPORT = ['red', 'orange', 'green', 'blue', 'black',
                        'purple', '#FFB4B4', '#FF3131', '#FFC0A0', '#FF8547',
                        '#FFDEA3', '#FFB531', '#FFF295', '#FFE527', '#EEFF8F',
                        '#DEFF27', '#CFFF99', '#9BFF2B', '#A9FFA0', '#3DFF28',
                        '#90FFB7', '#2AFF75', '#9DFFD8', '#2AFFAA', '#8BFFF8',
                        '#27FFF2', '#90D5FF', '#2CAFFF', '#A0BAFF', '#1857FF']


def create_plot_with_stops(route_path_name: str,
                           stops_order_df: pd.DataFrame,
                           df_vis: pd.DataFrame,
                           tm_id_df: pd.DataFrame,
                           folder_to_save: Path,
                           transport_to_check: Any):

    stops_order = list(['stop_id'])

    fig_size = (17, 10.0)
    fig, ax = plt.subplots(1, figsize=fig_size)

    # Calculate distances between stops
    calculator = DistanceCalculator(stops_order_df)
    distances = []
    for i, row in calculator.get_cumulative_distance():
        distances.append([row.stop_id, i])

        stop_df = df_vis[df_vis['stop_id'] == row.stop_id]
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
    distances = pd.DataFrame(distances, columns=['stop_id', 'distance'])

    # Add line for desired (main) transport
    agg = tm_id_df.groupby('stop_id').agg({'forecast_time_datetime': 'mean',
                                           'stop_name': 'first'})
    agg = agg.reset_index()
    agg = agg.merge(distances, on='stop_id')
    agg = agg.reset_index()
    agg['stop_id'] = pd.Categorical(agg['stop_id'], stops_order)
    agg = agg.sort_values(by='stop_id')
    if route_path_name == 'Трамвай 21':
        ax.plot(agg['forecast_time_datetime'][2:], agg['distance'][2:], '--', c='black',
                label=f'Движение транспортного средства по данным расписания: {transport_to_check}')
    else:
        ax.plot(agg['forecast_time_datetime'], agg['distance'], '--', c='black',
                label=f'Движение транспортного средства по данным расписания: {transport_to_check}')

    if route_path_name == 'Трамвай 21':
        min_x = datetime.datetime.strptime("24072022T13:30",
                                           "%d%m%YT%H:%M")

        max_x = datetime.datetime.strptime("24072022T14:30",
                                           "%d%m%YT%H:%M")

        ax.set_xlim(min_x, max_x)

    ax.grid()
    ax.set_yticklabels(list(agg['stop_name']))
    plt.yticks(agg['distance'])

    direct_ids, back_ids = get_indices_direct_and_back(list(agg['stop_name']))
    for direct_id in direct_ids:
        plt.gca().get_yticklabels()[direct_id].set_color("#84BEFF")
    for back_id in back_ids:
        plt.gca().get_yticklabels()[back_id].set_color("#FFDA84")

    ax.legend(fontsize=16)
    ax.set_xlabel('Дата')
    ax.set_ylabel('Остановки на маршруте')
    fig.suptitle(f'Маршрут {route_path_name}', fontsize=16)
    plot_name = f'{route_path_name.replace(".", "_")}.png'
    fig.savefig(Path(folder_to_save, plot_name), dpi=300, bbox_inches='tight')
    plt.close()


def create_plot_with_stops_and_transport(route_path_name: str,
                                         stops_order_df: pd.DataFrame,
                                         df_vis: pd.DataFrame,
                                         folder_to_save: Path):
    stops_order = list(stops_order_df['stop_id'])

    fig_size = (15, 10.0)
    fig, ax = plt.subplots(1, figsize=fig_size)
    for transport_id, transport in enumerate(list(df_vis['tmId'].unique())):
        transport_df = df_vis[df_vis['tmId'] == transport]
        color = COLORS_PER_TRANSPORT[transport_id]

        calculator = DistanceCalculator(stops_order_df)
        distances = []
        for i, row in calculator.get_cumulative_distance():
            distances.append([row.stop_id, i])
            stop_df = transport_df[transport_df['stop_id'] == row.stop_id]
            stop_df = stop_df.sort_values(by='forecast_time_datetime')

            tel_data = stop_df[stop_df['byTelemetry'] == 1]
            scheduled_data = stop_df[stop_df['byTelemetry'] == 0]

            if len(tel_data) >= 1:
                # Remove all unreliable telemetry data (use threshold for that)
                if 'id' in list(tel_data.columns):
                    tel_data = tel_data.drop(columns=['id'])
                tel_data['diff'] = tel_data['forecast_time'] - tel_data['request_time']
                tel_data['diff'] = np.abs(np.array(tel_data['diff']))
                tel_data['diff'][tel_data['diff'] > MIN_FORECAST_HORIZON_SECONDS] = np.nan
                tel_data = tel_data.dropna()

                if len(tel_data) >= 1:
                    ax.scatter(tel_data['forecast_time_datetime'],
                               [i] * len(tel_data),
                               c=color, alpha=1.0, marker="^", s=110)
            if len(scheduled_data) >= 1:
                ax.scatter(scheduled_data['forecast_time_datetime'],
                           [i] * len(scheduled_data),
                           c=color, alpha=0.5, s=110)

        distances = pd.DataFrame(distances, columns=['stop_id', 'distance'])

    if route_path_name == 'Трамвай 21':
        min_x = datetime.datetime.strptime("24072022T13:30", "%d%m%YT%H:%M")

        max_x = datetime.datetime.strptime("24072022T14:30", "%d%m%YT%H:%M")

        ax.set_xlim(min_x, max_x)

    ax.grid()
    ax.set_yticklabels(stops_order)
    plt.yticks(range(len(stops_order)))
    ax.set_xlabel('Дата')
    ax.set_ylabel('Остановки на маршруте')
    fig.suptitle(f'Маршрут {route_path_name}', fontsize=16)
    plot_name = f'{route_path_name.replace(".", "_")}.png'
    fig.savefig(Path(folder_to_save, plot_name),
                dpi=300, bbox_inches='tight')
    plt.close()


def get_indices_direct_and_back(stops_names_order: list):
    """ Split stops by direct and backward directions """
    visited_stops = []
    visited_direct = []
    visited_back = []
    move_direct = True

    direct_ids = []
    back_ids = []
    for i, stop in enumerate(stops_names_order):
        if stop in visited_stops:
            if stop in visited_direct and move_direct is True:
                move_direct = False
            elif stop in visited_back and move_direct is False:
                move_direct = True

        if move_direct is True:
            direct_ids.append(i)
            visited_direct.append(stop)
        else:
            back_ids.append(i)
            visited_back.append(stop)
        visited_stops.append(stop)

    return direct_ids, back_ids
