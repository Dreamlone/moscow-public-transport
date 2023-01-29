from pathlib import Path

import pandas as pd

from mostra.data_structure import COLUMN_NAMES
from mostra.main import TransportDataExplorer
from mostra.paths import get_data_path
from mostra.preprocessing import aggregate_schedule_items

import warnings
warnings.filterwarnings('ignore')


def preprocess_schedule_data_and_show():
    """
    Rus
    Скрипт сначала агрегирует прогнозные времена прибытия по расписанию по
    каждой остановке и затем строит графики для каждого маршрута

    Нотация генерируемых изображений:
        - ось X: названия остановок, расположенные в порядке следования
        транспорта по маршарута
        - ось Y: предсказанное время повяления транспорта на остановке
        - цвет: обозначает были ли прогнозные значения приезда транспорта
        получены при помощи телеметрии или это данные расписания.
    """
    # Load data
    df = pd.read_csv(Path(get_data_path(), 'pred_data.csv'), names=COLUMN_NAMES)

    # Aggregate and save for further steps
    final_df = aggregate_schedule_items(df)
    final_df.to_csv(Path(get_data_path(), 'pred_data_preprocessed.csv'), index=False)

    # And generate visualizations per routes
    explorer = TransportDataExplorer(final_df)
    explorer.prepare_plots_stops_per_route('./routes_preprocessed',
                                           route_name_to_show='Трамвай 21')


if __name__ == '__main__':
    preprocess_schedule_data_and_show()
