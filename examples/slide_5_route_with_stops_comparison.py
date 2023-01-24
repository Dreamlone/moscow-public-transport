from pathlib import Path
from typing import Union
import pandas as pd

from mostra.main import TransportDataExplorer
from mostra.paths import get_data_path

import warnings
warnings.filterwarnings('ignore')


def create_transport_and_stops_plots(folder_to_save: Union[Path, str]):
    """
    Rus
    Скрипт сначала агрегирует прогнозные времена прибытия по расписанию по
    каждой остановке и затем строит графики для каждого маршрута

    Нотация генерируемых изображений:
        - ось X: названия остановок, расположенные в порядке следования
        транспорта по маршарута
        - ось Y: предсказанное время повяления транспорта на остановке
        - цвет: обозначает id траспорта (tmId)
        - форма: обозначает были ли прогнозные значения приезда транспорта
        получены при помощи телеметрии или это данные расписания. Треугольник
        значит - телеметрия, кружок - данные расписания
    """
    df = pd.read_csv(Path(get_data_path(), 'pred_data_preprocessed.csv'),
                     parse_dates=['forecast_time_datetime', 'request_time_datetime'])

    explorer = TransportDataExplorer(df)
    explorer.prepare_plots_track_transport(folder_to_save)


if __name__ == '__main__':
    create_transport_and_stops_plots('./routes_preprocessed_with_telemetry')
