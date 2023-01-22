from pathlib import Path
from typing import Union
import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from mostra.data_structure import COLUMN_NAMES
from mostra.paths import get_data_path
from mostra.routes.routes_with_stops import prepare_plots_for_route, \
    prepare_df_for_visualization

import warnings
warnings.filterwarnings('ignore')

COLORS_PER_TRANSPORT = ['red', 'orange', 'green', 'blue', 'black',
                        'purple', 'purple', 'purple', 'purple', 'purple',
                        'purple', 'purple', 'purple', 'purple', 'purple']

MIN_FORECAST_HORIZON_SECONDS = 120


def create_plots(folder_to_save: Union[Path, str]):
    if isinstance(folder_to_save, str):
        folder_to_save = Path(folder_to_save)
    folder_to_save = folder_to_save.resolve()
    folder_to_save.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(Path(get_data_path(), 'pred_data_preprocessed.csv'),
                     parse_dates=['forecast_time_datetime', 'request_time_datetime'])

    stop_from_repo = pd.read_csv(Path(get_data_path(), 'stop_from_repo.csv'))
    stop_names = stop_from_repo[['stop_id', 'name']]
    stop_from_repo = stop_from_repo[['route_path_id', 'transport_type', 'number']]
    stop_from_repo = stop_from_repo.drop_duplicates()
    stop_names = stop_names.drop_duplicates()

    route_path_ids = list(df['route_path_id'].unique())
    route_path_ids.sort()
    for i, route_path_id in enumerate(route_path_ids):
        if i < 27:
            continue
        print(f'Process path {i} from {len(route_path_ids)}')
        route_path_df = df[df['route_path_id'] == route_path_id]

        try:
            df_vis, route_path_name = prepare_df_for_visualization(
                route_path_df,
                route_path_id,
                stop_names,
                stop_from_repo)
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

        if len(list(df_vis['tmId'].unique())) > len(COLORS_PER_TRANSPORT):
            continue

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


if __name__ == '__main__':
    # Prepare not the whole dataframe for visualization but small part
    create_plots('./routes_preprocessed_with_telemetry')
