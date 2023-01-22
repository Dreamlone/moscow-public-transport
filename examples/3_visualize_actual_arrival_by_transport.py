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


def visualize_actual_arrival_time_by_transport(per_daytime: bool = False):
    """
    Rus
    Подговка попарных сравнений (или n-мерных сравнений) между траснпортными
    средствами по времени отклонения от расписания
    """
    actual = pd.read_csv(Path(get_data_path(), 'actual_vs_forecasted.csv'),
                         parse_dates=['forecast_time_datetime',
                                      'request_time_datetime'])
    actual['arrival_time_datetime'] = pd.to_datetime(actual['arrival_time'], unit='s')

    # Remain only scheduled data
    actual['Отклонение от расписания, мин'] = actual['arrival_time'] - actual['forecast_time']
    actual['Отклонение от расписания, мин'] = actual['Отклонение от расписания, мин'] / 60
    a = actual[actual['Отклонение от расписания, мин'] >= 50]
    a['id'] = np.arange(len(a))
    actual = actual[actual['byTelemetry'] == 0]
    actual['Отклонение от расписания, мин'] = actual['arrival_time'] - actual['forecast_time']
    actual['Отклонение от расписания, мин'] = actual['Отклонение от расписания, мин'] / 60

    df_for_visualization = actual.groupby(['stop_id', 'route_path_id', 'tmId', 'case']).agg(
        {'arrival_time_datetime': 'first',
         'Отклонение от расписания, мин': 'mean'})
    df_for_visualization = df_for_visualization.reset_index()

    # Enrich with additional data
    stop_from_repo = pd.read_csv(Path(get_data_path(), 'stop_from_repo.csv'))
    stop_from_repo = stop_from_repo[['route_path_id', 'transport_type', 'number']]
    stop_from_repo = stop_from_repo.drop_duplicates()

    updated_transport_types = []
    # TODO vectorize
    for row_id, row in stop_from_repo.iterrows():
        if row.transport_type == 'tram':
            updated_transport_types.append('трамвай')
        else:
            if 'К' in row.number or 'к' in row.number:
                if row.number[0] != 'т':
                    updated_transport_types.append('маршрутка')
                else:
                    updated_transport_types.append('автобус')
            else:
                updated_transport_types.append('автобус')
    stop_from_repo['transport_type'] = updated_transport_types

    df_for_visualization = df_for_visualization.merge(stop_from_repo, on='route_path_id')
    df_for_visualization = df_for_visualization.drop_duplicates()
    df_for_visualization = df_for_visualization.rename(columns={'transport_type': 'Транспорт'})

    df_for_visualization['Время суток'] = df_for_visualization['arrival_time_datetime'].dt.hour
    df_for_visualization['Время суток'] = df_for_visualization['Время суток'].replace(HOUR_INTO_DAYTIME)

    with sns.axes_style('darkgrid'):
        sns.catplot(data=df_for_visualization,
                    x="Транспорт", y="Отклонение от расписания, мин",
                    palette='rainbow', kind="box", width=0.2, row='Время суток')
        plt.show()


if __name__ == '__main__':
    visualize_actual_arrival_time_by_transport(per_daytime=True)
