import datetime
from pathlib import Path
from typing import Any

import pandas as pd

import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')


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
