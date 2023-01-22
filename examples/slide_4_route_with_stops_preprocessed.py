from pathlib import Path
from typing import Union

import pandas as pd
import numpy as np

from mostra.data_structure import COLUMN_NAMES
from mostra.paths import get_data_path
from mostra.routes.routes_with_stops import prepare_plots_for_route

import warnings
warnings.filterwarnings('ignore')


SINGLE_CASE_SECONDS_THRESHOLD = 10 * 60
BELONG_CASE_SECONDS_TEL_THRESHOLD = 20 * 60
MIN_FORECAST_HORIZON_SECONDS = 120


def load_preprocessed_dataframe():
    df = pd.read_csv(Path(get_data_path(), 'pred_data.csv'),
                     names=COLUMN_NAMES,
                     nrows=300000)

    # Assign datetime labels for convenient debugging process
    df['forecast_time_datetime'] = pd.to_datetime(df['forecast_time'], unit='s')
    df['request_time_datetime'] = pd.to_datetime(df['request_time'], unit='s')

    final_df = []
    route_path_ids = list(df['route_path_id'].unique())
    route_path_ids.sort()
    for i, path_id in enumerate(route_path_ids):
        print(f'Process path {i} from {len(route_path_ids)}')
        route_path_df = df[df['route_path_id'] == path_id]

        stops_list = list(route_path_df['stop_id'].unique())
        for stop in stops_list:
            stop_df = route_path_df[route_path_df['stop_id'] == stop]

            for transport_id in list(stop_df['tmId'].unique()):
                transport_df = stop_df[stop_df['tmId'] == transport_id]

                transponder_data = transport_df[transport_df['byTelemetry'] == 1]
                if len(transponder_data) > 0:
                    transponder_data = transponder_data.drop(columns=['id'])

                scheduled_data = transport_df[transport_df['byTelemetry'] == 0]
                if len(scheduled_data) < 1:
                    continue
                elif len(scheduled_data) == 1:
                    final_df.extend([transponder_data, scheduled_data])

                # Assign single arrival labels to
                scheduled_data = scheduled_data.sort_values(by='forecast_time')
                row_id = 0
                current_time_batch = 0
                time_batches = []
                for _, row in scheduled_data.iterrows():
                    if row_id == 0:
                        # Skip first index
                        row_id += 1
                        time_batches.append(current_time_batch)
                        continue

                    prev_row = scheduled_data.iloc[row_id - 1]
                    time_diff = row.forecast_time - prev_row.forecast_time
                    # The time delta must not exceed 20 minutes
                    if time_diff >= SINGLE_CASE_SECONDS_THRESHOLD:
                        # Otherwise - it is new case
                        current_time_batch += 1
                    time_batches.append(current_time_batch)
                    row_id += 1

                scheduled_data['case'] = time_batches
                scheduled_data = scheduled_data.groupby('case').agg({'stop_id': 'first',
                                                                     'route_path_id': 'first',
                                                                     'forecast_time': 'mean',
                                                                     'byTelemetry': 'first',
                                                                     'tmId': 'first',
                                                                     'routePathId': 'first',
                                                                     'request_time': 'mean',
                                                                     'forecast_time_datetime': 'mean',
                                                                     'request_time_datetime': 'mean'})
                scheduled_data = scheduled_data.reset_index()
                scheduled_data = scheduled_data.drop(columns=['case'])

                final_df.extend([transponder_data, scheduled_data])
    final_df = pd.concat(final_df)

    final_df.to_csv(Path(get_data_path(), 'pred_data_preprocessed.csv'), index=False)
    return final_df


if __name__ == '__main__':
    # Prepare not the whole dataframe for visualization but small part
    df = load_preprocessed_dataframe()
    prepare_plots_for_route(df, './routes_preprocessed')
