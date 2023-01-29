from pathlib import Path
from typing import Union

import geopandas
import pandas as pd
import numpy as np
import contextily as cx

import matplotlib.pyplot as plt
import seaborn as sns
from geopandas import GeoDataFrame
from geopy import Point
from shapely import LineString

from tqdm import tqdm

from mostra.data_structure import HOUR_INTO_DAYTIME
from mostra.paths import get_data_path

import warnings
warnings.filterwarnings('ignore')

TH_MINUTES = 1


def maps_with_statistics():
    actual = pd.read_csv(Path(get_data_path(), 'actual_vs_forecasted.csv'),
                         parse_dates=['forecast_time_datetime',
                                      'request_time_datetime',
                                      'arrival_time_datetime'])

    route_path_ids = list(actual['route_path_id'].unique())
    pbar = tqdm(route_path_ids, colour='blue')
    report = []
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

        late_df = stop_df[stop_df['Отклонение от расписания, мин'] > 0]
        early_df = stop_df[stop_df['Отклонение от расписания, мин'] <= 0]

        if len(late_df) < 1 or len(early_df) < 1:
            continue

        # Start calculating only lates
        delta = np.array(late_df['Отклонение от расписания, мин'])
        delta = np.abs(delta)
        true_late_ids = np.argwhere(delta >= TH_MINUTES)

        true_late_number = len(true_late_ids)
        if true_late_number == 0:
            report.append([route_path_id, transport, transport_number, 0, 100, 0])
            continue

        true_late_ratio = (true_late_number / len(stop_df)) * 100
        true_late_ratio = round(true_late_ratio, 0)
        report.append([route_path_id, transport, transport_number,
                       true_late_ratio,
                       100 - true_late_ratio,
                       np.mean(delta[true_late_ids])])

    report = pd.DataFrame(report, columns=['route_path_id', 'transport_type',
                                           'number', 'Доля опозданий',
                                           'Доля прибытий по расписанию',
                                           'Среднее время опозданий, мин'])

    # Create line spatial geometries with attributes
    stop_from_repo = pd.read_csv(Path(get_data_path(), 'stop_from_repo.csv'))
    stop_from_repo = stop_from_repo[['route_path_id', 'stop_id', 'lon', 'lat']]
    stop_from_repo = stop_from_repo.drop_duplicates()

    paths = []
    for row_id, row in report.iterrows():
        route_stops = stop_from_repo[stop_from_repo['route_path_id'] == row.route_path_id]

        if len(route_stops) < 1:
            continue

        for _, node in route_stops.iterrows():
            point_df = GeoDataFrame(pd.DataFrame({'transport_type': [row['transport_type']],
                                                  'number': [row['number']],
                                                  'Доля опозданий': [row['Доля опозданий']],
                                                  'Доля прибытий по расписанию': [row['Доля прибытий по расписанию']],
                                                  'Среднее время опозданий, мин': [row['Среднее время опозданий, мин']],
                                                  'stop_id': [node['stop_id']]}),
                                    geometry=geopandas.points_from_xy([node['lon']], [node['lat']]),
                                    crs=4326)
            paths.append(point_df)

    paths = pd.concat(paths)
    paths = paths.to_crs(3857)

    paths_vis = paths.groupby('stop_id').agg({'Доля опозданий': 'mean',
                                              'Доля прибытий по расписанию': 'mean',
                                              'Среднее время опозданий, мин': 'mean',
                                              'geometry': 'first'})
    paths_vis = paths_vis.reset_index()
    paths_vis = GeoDataFrame(paths_vis, geometry=paths_vis['geometry'])

    # Среднее время опозданий
    markersize = 12
    ax = paths_vis.plot(column='Среднее время опозданий, мин',
                    alpha=0.6, legend=True,
                    cmap='Reds',
                    vmin=0, vmax=8,
                    figsize=(11, 7),
                    legend_kwds={'label': "Среднее время опозданий, мин"},
                    zorder=1, markersize=markersize)
    cx.add_basemap(ax)
    ax.set_xlim(4.16 * 1e6, 4.22 * 1e6)
    ax.set_ylim(7.49 * 1e6, 7.54 * 1e6)
    plt.show()

    # Среднее время опозданий
    ax = paths_vis.plot(column='Доля опозданий',
                    alpha=0.6, legend=True,
                    cmap='Oranges',
                    vmin=0, vmax=100,
                    figsize=(11, 7),
                    legend_kwds={'label': "Доля опозданий, %"},
                    zorder=1, markersize=markersize)
    cx.add_basemap(ax)
    ax.set_xlim(4.16 * 1e6, 4.22 * 1e6)
    ax.set_ylim(7.49 * 1e6, 7.54 * 1e6)
    plt.show()

    bus_df = paths[paths['transport_type'] == 'bus']
    tram_df = paths[paths['transport_type'] == 'tram']

    # Take common points for buses and trams
    common_stops = bus_df.merge(tram_df, on='stop_id', suffixes=(' автобус', ' трамвай'))
    common_stops = common_stops.groupby('stop_id').agg({'Доля опозданий автобус': 'mean',
                                                        'Доля опозданий трамвай': 'mean',
                                                        'Доля прибытий по расписанию автобус': 'mean',
                                                        'Доля прибытий по расписанию трамвай': 'mean',
                                                        'Среднее время опозданий, мин автобус': 'mean',
                                                        'Среднее время опозданий, мин трамвай': 'mean',
                                                        'geometry автобус': 'first'})

    common_stops = common_stops.reset_index()
    common_stops = GeoDataFrame(common_stops, geometry=common_stops['geometry автобус'])

    markersize = 22
    ax = common_stops.plot(column='Доля опозданий автобус', alpha=0.6, legend=True,
                           cmap='Oranges', vmin=0, vmax=100, figsize=(15, 7),
                           legend_kwds={'label': "Доля опозданий, %"},
                           zorder=1, markersize=markersize)
    cx.add_basemap(ax, reset_extent=False)
    ax.set_xlim(4.16 * 1e6, 4.215 * 1e6)
    ax.set_ylim(7.49 * 1e6, 7.533 * 1e6)
    ax.set_title('Автобусные остановки, пересекающиеся с сегментами трамвайных маршрутов')
    plt.show()
    ax = common_stops.plot(column='Доля опозданий трамвай', alpha=0.6,
                           legend=True,
                           cmap='Oranges', vmin=0, vmax=100, figsize=(15, 7),
                           legend_kwds={'label': "Доля опозданий, %"},
                           zorder=1, markersize=markersize)
    cx.add_basemap(ax, reset_extent=False)
    ax.set_xlim(4.16 * 1e6, 4.215 * 1e6)
    ax.set_ylim(7.49 * 1e6, 7.533 * 1e6)
    ax.set_title('Остановки трамваев, пересекающиеся с сегментами автобусных маршрутов')
    plt.show()


if __name__ == '__main__':
    maps_with_statistics()
