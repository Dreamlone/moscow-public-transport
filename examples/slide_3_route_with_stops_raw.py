from pathlib import Path
from typing import Union

import pandas as pd

from mostra.data_structure import COLUMN_NAMES
from mostra.paths import get_data_path
from mostra.routes.routes_with_stops import prepare_plots_for_route

import warnings
warnings.filterwarnings('ignore')


def load_raw_dataframe():
    df = pd.read_csv(Path(get_data_path(), 'pred_data.csv'),
                     names=COLUMN_NAMES,
                     nrows=300000)

    return df


if __name__ == '__main__':
    df = load_raw_dataframe()
    prepare_plots_for_route(df, './routes')
