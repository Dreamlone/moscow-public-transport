from pathlib import Path

import pandas as pd

from mostra.data_structure import COLUMN_NAMES
from mostra.paths import get_data_path


def show_desired_item(stop_id: str, route_path_id: str, tm_id: int = None):
    """
    Rus
    Формирует таблицу для требуемого транспортного средства, маршрута (с учетом
    направления) и остановки. Запрос формируется для исходной таблицы
    """
    df = pd.read_csv(Path(get_data_path(), 'pred_data.csv'), names=COLUMN_NAMES)
    df = df[df['stop_id'] == stop_id]
    df = df[df['route_path_id'] == route_path_id]
    if tm_id is not None:
        df = df[df['tmId'] == tm_id]

    # Add columns for convenient debug checking
    df['forecast_time_datetime'] = pd.to_datetime(df['forecast_time'], unit='s')
    df['request_time_datetime'] = pd.to_datetime(df['request_time'], unit='s')
    df = df.sort_values(by=['request_time_datetime', 'forecast_time_datetime'])

    debug_df = df[['request_time_datetime', 'forecast_time_datetime', 'byTelemetry']]
    return df


if __name__ == '__main__':
    show_desired_item('fe3e2f59-78bf-4599-8f5b-5745a9c94e67',
                      '1d40a752-2894-40b1-aeea-3b3bf18f8d68',
                      55830)
