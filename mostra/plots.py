import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
import warnings

from mostra.data_structure import MIN_FORECAST_HORIZON_SECONDS

warnings.filterwarnings('ignore')

# TODO refactor color related logic
COLORS_PER_TRANSPORT = ['red', 'orange', 'green', 'blue', 'black',
                        'purple', '#FFDB6C', '#EFFF6C', '#EFFF6C', '#59EBAD',
                        '#5EBDF7', 'purple', 'purple', 'purple', 'purple',
                        'purple', 'purple', 'purple', 'purple', 'purple',
                        'purple', 'purple', 'purple', 'purple', 'purple',
                        'purple', 'purple', 'purple', 'purple', 'purple']


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


def create_plot_with_stops_and_transport(route_path_name: str,
                                         stops_order: list,
                                         df_vis: pd.DataFrame,
                                         folder_to_save: Path):
    fig_size = (15, 10.0)
    fig, ax = plt.subplots(1, figsize=fig_size)
    for transport_id, transport in enumerate(list(df_vis['tmId'].unique())):
        transport_df = df_vis[df_vis['tmId'] == transport]
        color = COLORS_PER_TRANSPORT[transport_id]

        for i, stop in enumerate(stops_order):
            stop_df = transport_df[transport_df['stop_name'] == stop]
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

    if route_path_name == 'Трамвай 21':
        min_x = datetime.datetime.strptime("24072022T13:30",
                                           "%d%m%YT%H:%M")

        max_x = datetime.datetime.strptime("24072022T14:30",
                                           "%d%m%YT%H:%M")

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
