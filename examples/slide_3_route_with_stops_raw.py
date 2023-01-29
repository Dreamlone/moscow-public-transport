from pathlib import Path
import pandas as pd

from mostra.data_structure import COLUMN_NAMES
from mostra.main import TransportDataExplorer
from mostra.paths import get_data_path

import warnings
warnings.filterwarnings('ignore')


def generate_raw_route_plots(new_path: str):
    """
    Rus
    Скрипт создает папку и генерирует (и сохраняет в неё) картинки. Для каждого
    маршрута генерируется своя картинка. Для визуализации используется исходный
    датасет

    Нотация генерируемых изображений:
        - ось X: названия остановок, расположенные в порядке следования
        транспорта по маршарута
        - ось Y: предсказанное время повяления транспорта на остановке.
        Расстояния между названиями остановок пропорциональны дистанции между
        остановками
        - цвет: обозначает были ли прогнозные значения приезда транспорта
        получены при помощи телеметрии или это данные расписания.
    """
    df = pd.read_csv(Path(get_data_path(), 'pred_data.csv'), names=COLUMN_NAMES)

    explorer = TransportDataExplorer(df)
    explorer.prepare_plots_stops_per_route(new_path)


if __name__ == '__main__':
    generate_raw_route_plots(new_path='./raw_routes')
