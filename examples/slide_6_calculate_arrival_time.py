from pathlib import Path
import pandas as pd
import numpy as np

from mostra.paths import get_data_path
from mostra.routes.routes_with_stops import prepare_df_for_visualization

import warnings
warnings.filterwarnings('ignore')

MIN_FORECAST_HORIZON_SECONDS = 120
BELONG_CASE_SECONDS_TEL_THRESHOLD = 10 * 60


def calculate_arrival_time():
    """
    Rus
    Расчет фактического времени прибытия для каждой пары
    "остановка - транспорт id"
    """
    df = pd.read_csv(Path(get_data_path(), 'pred_data_preprocessed.csv'),
                     parse_dates=['forecast_time_datetime',
                                  'request_time_datetime'])

    stop_from_repo = pd.read_csv(Path(get_data_path(), 'stop_from_repo.csv'))
    stop_names = stop_from_repo[['stop_id', 'name']]
    stop_from_repo = stop_from_repo[['route_path_id', 'transport_type', 'number']]
    stop_from_repo = stop_from_repo.drop_duplicates()
    stop_names = stop_names.drop_duplicates()

    final_df = []
    route_path_ids = list(df['route_path_id'].unique())
    route_path_ids.sort()
    for i, route_path_id in enumerate(route_path_ids):
        print(f'Process path {i} from {len(route_path_ids)}')

        route_path_df = df[df['route_path_id'] == route_path_id]

        try:
            df_vis, route_path_name = prepare_df_for_visualization(
                route_path_df,
                route_path_id,
                stop_names,
                stop_from_repo)
        except Exception:
            continue

        for transport in list(df_vis['tmId'].unique()):
            transport_df = df_vis[df_vis['tmId'] == transport]

            for stop in list(transport_df['stop_name'].unique()):
                stop_df = transport_df[transport_df['stop_name'] == stop]
                if 'id' in list(stop_df.columns):
                    stop_df = stop_df.drop(columns=['id'])

                tel_data = stop_df[stop_df['byTelemetry'] == 1]
                scheduled_data = stop_df[stop_df['byTelemetry'] == 0]
                if len(tel_data) < 1 or len(scheduled_data) < 1:
                    continue

                # Remain only "reliable" data
                tel_data['diff'] = tel_data['forecast_time'] - tel_data['request_time']
                tel_data['diff'] = np.abs(np.array(tel_data['diff']))
                tel_data['diff'][tel_data['diff'] > MIN_FORECAST_HORIZON_SECONDS] = np.nan
                tel_data = tel_data.dropna()

                if len(tel_data) < 1:
                    continue

                # Assign telemetry data to scheduled item
                scheduled_data = scheduled_data.sort_values(by='forecast_time')
                tel_data = tel_data.sort_values(by='forecast_time')
                tel_data = tel_data.reset_index()

                arrival_time = []
                for row_id, row in scheduled_data.iterrows():
                    # Search for appropriate telemetry data
                    tel_data['schedule_forecast_time'] = [row.forecast_time] * len(tel_data)
                    delta = np.array(tel_data['schedule_forecast_time'] - tel_data['forecast_time'])
                    delta = np.abs(delta)

                    expected_arrival_time = np.nan
                    for tel_item_id, tel_point in enumerate(delta):
                        if tel_point < BELONG_CASE_SECONDS_TEL_THRESHOLD:
                            # We can assign arrival time for current case
                            expected_arrival_time = tel_data['forecast_time'].iloc[tel_item_id]
                            break

                    arrival_time.append(expected_arrival_time)

                scheduled_data['arrival_time'] = arrival_time
                final_df.append(scheduled_data)

    final_df = pd.concat(final_df)
    final_df = final_df.dropna()

    final_df['arrival_time_datetime'] = pd.to_datetime(final_df['arrival_time'], unit='s')
    final_df.to_csv(Path(get_data_path(), 'actual_vs_forecasted.csv'),
                    index=False)


if __name__ == '__main__':
    calculate_arrival_time()
