from pathlib import Path
from typing import Union

import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
import seaborn as sns

from tqdm import tqdm

from mostra.data_structure import HOUR_INTO_DAYTIME
from mostra.paths import get_data_path, create_folder

import warnings
warnings.filterwarnings('ignore')


def visualize_actual_arrival_time(folder_to_save: Union[Path, str] = '.'):
    actual = pd.read_csv(Path(get_data_path(), 'actual_vs_forecasted.csv'),
                         parse_dates=['forecast_time_datetime',
                                      'request_time_datetime',
                                      'arrival_time_datetime'])

    folder_to_save = create_folder(folder_to_save)

    route_path_ids = list(actual['route_path_id'].unique())
    pbar = tqdm(route_path_ids, colour='blue')
    for route_path_id in pbar:
        pbar.set_description(f'Processing route path with id {route_path_id}')

        stop_df = actual[actual['route_path_id'] == route_path_id]
        stop_df = stop_df.sort_values('arrival_time')

        stop_df['Отклонение от расписания, мин'] = stop_df['arrival_time'] - stop_df['forecast_time']
        stop_df['Отклонение от расписания, мин'] = stop_df['Отклонение от расписания, мин'] / 60

        stop_df = stop_df.sort_values(by='forecast_time_datetime')
        stop_df = stop_df.rename(columns={'arrival_time_datetime': 'Дата и время прибытия на остановку'})

        transport = stop_df["transport_type"].iloc[0]
        transport_number = stop_df["number"].iloc[0]
        plot_name = f'{transport}_{transport_number}'
        plot_name = plot_name.replace('"', '')
        plot_name = plot_name.replace('.', ',')

        fig_size = (15, 6.0)
        fig, ax = plt.subplots(1, figsize=fig_size)

        late_df = stop_df[stop_df['Отклонение от расписания, мин'] > 0]
        early_df = stop_df[stop_df['Отклонение от расписания, мин'] <= 0]

        if len(late_df) > 0:
            ax.scatter(late_df['Дата и время прибытия на остановку'],
                       late_df['Отклонение от расписания, мин'], color='red')
        if len(early_df) > 0:
            ax.scatter(early_df['Дата и время прибытия на остановку'],
                       early_df['Отклонение от расписания, мин'], color='blue')
        ax.grid()
        ax.set_xlabel('Дата и время прибытия на остановку')
        ax.set_ylabel('Отклонение от расписания, мин')
        title = f'{transport} {transport_number}'
        title = title.replace('bus', 'Автобус')
        title = title.replace('tram', 'Трамвай')
        fig.suptitle(title, fontsize=16)
        plt.savefig(Path(folder_to_save, f'{plot_name}.png'),
                    dpi=300, bbox_inches='tight')
        plt.close()

        stop_df['Время суток'] = stop_df['Дата и время прибытия на остановку'].dt.hour
        stop_df['Время суток'] = stop_df['Время суток'].replace(HOUR_INTO_DAYTIME)

        with sns.axes_style('darkgrid'):
            sns.catplot(data=stop_df,
                        x="Время суток", y="Отклонение от расписания, мин",
                        palette='crest', hue='Время суток',
                        order=['утр. час пик', 'утро', 'день', 'веч. час пик', 'вечер', 'ночь'],
                        hue_order=['утр. час пик', 'утро', 'день', 'веч. час пик', 'вечер', 'ночь'])
            plt.savefig(Path(folder_to_save, f'{plot_name}_boxplot.png'),
                        dpi=300, bbox_inches='tight')
            plt.close()


if __name__ == '__main__':
    visualize_actual_arrival_time(folder_to_save='./arrival')
