from pathlib import Path

import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
import seaborn as sns

from tqdm import tqdm

from mostra.data_structure import HOUR_INTO_DAYTIME
from mostra.paths import get_data_path

import warnings
warnings.filterwarnings('ignore')


def visualize_actual_arrival_time(aggregate: bool = False):
    """
    Rus
    Поиск остановки и транспортного средства с наибольшим количеством информации
    для наглядной визуализации вида "Насколько сильно опаздывает автобус N
    на остановке A в зависимости от даты и времени суток"
    """
    actual = pd.read_csv(Path(get_data_path(), 'actual_vs_forecasted.csv'),
                         parse_dates=['forecast_time_datetime',
                                      'request_time_datetime'])
    actual['arrival_time_datetime'] = pd.to_datetime(actual['arrival_time'], unit='s')

    # Collect information - TODO vectorize this algorithm
    stop_id_to_show = None
    path_id_to_show = None
    max_cases = None
    route_path_ids = list(actual['route_path_id'].unique())
    pbar = tqdm(route_path_ids, colour='blue')
    for route_path_id in route_path_ids:
        pbar.set_description(f'Processing route path with id {route_path_id}')

        route_path_df = actual[actual['route_path_id'] == route_path_id]
        for stop in list(route_path_df['stop_id'].unique()):
            stop_df = route_path_df[route_path_df['stop_id'] == stop]

            schedule_data = stop_df[stop_df['byTelemetry'] == 0]
            if len(schedule_data) == 0:
                continue

            if max_cases is None:
                # First iteration
                max_cases = len(schedule_data['case'].unique())
                stop_id_to_show = stop
                path_id_to_show = route_path_id
            elif len(schedule_data['case'].unique()) > max_cases:
                # New more appropriate case to show
                max_cases = len(schedule_data['case'].unique())
                stop_id_to_show = stop
                path_id_to_show = route_path_id

    route_path_df = actual[actual['route_path_id'] == path_id_to_show]
    stop_df = route_path_df[route_path_df['stop_id'] == stop_id_to_show]
    schedule_data = stop_df[stop_df['byTelemetry'] == 0]
    schedule_data = schedule_data.sort_values(by='forecast_time_datetime')

    # Enrich with additional data
    stop_from_repo = pd.read_csv(Path(get_data_path(), 'stop_from_repo.csv'))
    stop_from_repo = stop_from_repo[stop_from_repo['route_path_id'] == path_id_to_show]
    stop_from_repo = stop_from_repo[['stop_id', 'transport_type', 'number', 'name']]
    stop_from_repo = stop_from_repo.drop_duplicates()
    df_for_visualization = schedule_data.merge(stop_from_repo, on='stop_id')
    df_for_visualization['Отклонение от расписания, мин'] = df_for_visualization['arrival_time'] - df_for_visualization['forecast_time']
    # Convert into minutes
    df_for_visualization['Отклонение от расписания, мин'] = df_for_visualization['Отклонение от расписания, мин'] / 60
    stop_from_repo = None

    df_for_visualization = df_for_visualization.rename(columns={'arrival_time_datetime': 'Дата и время прибытия на остановку'})
    transport = df_for_visualization["transport_type"].iloc[0]
    transport_number = df_for_visualization["number"].iloc[0]
    stop_name = df_for_visualization["name"].iloc[0]
    # Prepare visualization
    with sns.axes_style('darkgrid'):
        print(f'Тип {transport}, номер {transport_number},'
              f'название {stop_name}')
        print(f'Всего кейсов: {max_cases}')
        sns.relplot(data=df_for_visualization, kind="line",
                    x="Дата и время прибытия на остановку",
                    y="Отклонение от расписания, мин",
                    color='purple')
        plt.show()

    df_for_visualization.to_csv(Path(get_data_path(), f'{transport}_{transport_number}_{stop_name}.csv'),
                                index=False)

    # Aggregate information per cases and group by day time
    if aggregate:
        df_for_visualization = df_for_visualization.groupby('case').agg({'Дата и время прибытия на остановку': 'first',
                                                                         'Отклонение от расписания, мин': 'mean'})
        df_for_visualization = df_for_visualization.reset_index()
    df_for_visualization['Время суток'] = df_for_visualization['Дата и время прибытия на остановку'].dt.hour
    df_for_visualization['Время суток'] = df_for_visualization['Время суток'].replace(HOUR_INTO_DAYTIME)

    with sns.axes_style('darkgrid'):
        sns.catplot(data=df_for_visualization,
                    x="Время суток", y="Отклонение от расписания, мин",
                    palette='crest', hue='Время суток',
                    order=['утро', 'день', 'вечер', 'ночь'],
                    hue_order=['утро', 'день', 'вечер', 'ночь'])
        plt.show()


if __name__ == '__main__':
    visualize_actual_arrival_time(aggregate=False)
