from pathlib import Path
from tqdm import tqdm

import pandas as pd
import numpy as np

from mostra.data_structure import COLUMN_NAMES
from mostra.paths import get_data_path

import warnings
warnings.filterwarnings('ignore')


SINGLE_CASE_SECONDS_THRESHOLD = 10 * 60
BELONG_CASE_SECONDS_TEL_THRESHOLD = 20 * 60
MIN_FORECAST_HORIZON_SECONDS = 120


def _find_scheduled_row(dataframe: pd.DataFrame):
    """
    Приоритет при выделении кейсов всегда отдаем данным расписания - если
    между данными расписания большой промежуток времени, то это, очень вероятно,
    следующая итерация данного транспортного средства на маршруте
    """
    if len(dataframe) == 1:
        # Single entry
        current_row = dataframe.iloc[0]
        return current_row

    # Iterate to the past untill get scheduled data
    for i in range(len(dataframe) - 1, -1, -1):
        if dataframe.iloc[i].byTelemetry == 0:
            return dataframe.iloc[i]

    return dataframe.iloc[-1]


def _enrich_data_with_cases_and_horizon(stop_vehicle_df: pd.DataFrame):
    """
    Rus
    Назначает каждой записи в таблице идентефикатор кейса и рассчитвает
    заблаговременность прогноза (время ожидания до приезда автобуса)
    """
    stop_vehicle_df = stop_vehicle_df.sort_values(by='forecast_time')
    tel_df = stop_vehicle_df[stop_vehicle_df['byTelemetry'] == 1]
    # Take only scheduled data
    scheduled_df = stop_vehicle_df[stop_vehicle_df['byTelemetry'] == 0]

    row_id = 0
    current_time_batch = 0
    time_batches = []
    for _, row in scheduled_df.iterrows():
        if row_id == 0:
            # Skip first index
            row_id += 1
            time_batches.append(current_time_batch)
            continue

        prev_row = scheduled_df.iloc[row_id - 1]
        time_diff = row.forecast_time - prev_row.forecast_time
        # The time delta must not exceed 20 minutes
        if time_diff >= SINGLE_CASE_SECONDS_THRESHOLD:
            # Otherwise - it is new case
            current_time_batch += 1
        time_batches.append(current_time_batch)
        row_id += 1

    scheduled_df['case'] = time_batches

    # Assign labels for telemetry data
    cases_for_tel = []
    scheduled_df = scheduled_df.reset_index()
    for _, row in tel_df.iterrows():
        scheduled_df['forecast_time_for_tel'] = [row.forecast_time] * len(scheduled_df)
        diff = np.array(scheduled_df['forecast_time_for_tel'] - scheduled_df['forecast_time'])
        diff = np.abs(diff)

        if len(scheduled_df) == 1:
            # We only can assign to current case or skip
            if diff[0] < BELONG_CASE_SECONDS_TEL_THRESHOLD:
                cases_for_tel.append(scheduled_df.iloc[0].case)
            else:
                cases_for_tel.append(np.nan)
            continue

        # Find indices of n minimum elements - nearest time stamps to tel observation
        minimum_elements_ids = diff.argsort()[:2]

        left_nearest_schedule = scheduled_df.iloc[minimum_elements_ids[0]]
        left_case = left_nearest_schedule.case
        right_nearest_schedule = scheduled_df.iloc[minimum_elements_ids[1]]
        right_case = right_nearest_schedule.case

        # Get seconds between timestamps
        left_distance = diff[minimum_elements_ids[0]]
        right_distance = diff[minimum_elements_ids[1]]

        if left_case == right_case:
            # It is the same batch

            if left_distance < BELONG_CASE_SECONDS_TEL_THRESHOLD:
                case_to_insert = left_case
            elif right_distance < BELONG_CASE_SECONDS_TEL_THRESHOLD:
                case_to_insert = right_case
            else:
                case_to_insert = np.nan

            cases_for_tel.append(case_to_insert)
            continue

        # Cases are different - we need to choose which to assign
        if left_distance <= right_distance and left_distance < BELONG_CASE_SECONDS_TEL_THRESHOLD:
            cases_for_tel.append(left_case)
            continue
        if left_distance > right_distance and right_distance < BELONG_CASE_SECONDS_TEL_THRESHOLD:
            cases_for_tel.append(right_distance)
            continue

        cases_for_tel.append(np.nan)

    scheduled_df = scheduled_df.drop(columns=['index', 'forecast_time_for_tel'])

    # Insert telemetric data
    tel_df['case'] = cases_for_tel
    stop_vehicle_df = pd.concat([scheduled_df, tel_df])
    stop_vehicle_df = stop_vehicle_df.sort_values(by=['forecast_time', 'request_time'])

    # Remove un appropriate telemetry observations
    stop_vehicle_df = stop_vehicle_df.dropna()
    stop_vehicle_df['forecast_horizon'] = stop_vehicle_df['forecast_time'] - stop_vehicle_df['request_time']
    return stop_vehicle_df


def define_actual_time(stop_vehicle_df: pd.DataFrame):
    """
    Rus
    Для выбранного события (конкретный транспорт на выбранной остановке)
    производится присваивание времени прибытия транспорта.

    Сначала выделяются кейсы: кейс - это когда ты стоишь на остановке и смотришь
    в приложении когда следующий автобус приедет. Смотреть можешь несколько раз
    подряд (будет несколько request_time), при этом каждый раз будет получать
    разное прогнозное время (будет несколько forecast_time). Но автобус ждёшь
    один и тот же, - и когда он приезжает на остановку - кейс закрывается.
    """
    # Get rid of duplicates
    stop_vehicle_df = stop_vehicle_df.drop(columns=['id']).drop_duplicates()

    # Define cases and forecast horizon (in seconds)
    stop_vehicle_df = _enrich_data_with_cases_and_horizon(stop_vehicle_df)

    # Start process each case - assign appropriate actual arriving time
    # Always one case - one arrival time
    dataframe_with_actual_arrival_time = []
    for case in list(stop_vehicle_df['case'].unique()):
        case_df = stop_vehicle_df[stop_vehicle_df['case'] == case]
        if len(case_df['request_time'].unique()) != len(case_df) and \
                len(case_df['forecast_time'].unique()) > len(case_df['request_time'].unique()):
            # Collision when we have requests in the same time and got different
            # forecasted times - skip it
            continue

        if 1 in list(case_df['byTelemetry'].unique()):
            # Telemetry data can be used
            telemetry_data = case_df[case_df['byTelemetry'] == 1]

            min_forecast_horizon = min(telemetry_data['forecast_horizon'])
            if min_forecast_horizon < MIN_FORECAST_HORIZON_SECONDS:
                # For current case it is possible to estimate actual time
                min_df = telemetry_data[telemetry_data['forecast_horizon'] == min_forecast_horizon]
                case_df['forecast_horizon'] = case_df['forecast_horizon'].replace({min_forecast_horizon: np.nan})
                case_df = case_df.dropna()
                arrival_time = list(min_df['forecast_time']) * len(case_df)
                case_df['arrival_time'] = arrival_time

                dataframe_with_actual_arrival_time.append(case_df)

    if len(dataframe_with_actual_arrival_time) > 0:
        dataframe_with_actual_arrival_time = pd.concat(dataframe_with_actual_arrival_time)
        dataframe_with_actual_arrival_time = dataframe_with_actual_arrival_time.drop(columns=['forecast_horizon'])

        return dataframe_with_actual_arrival_time
    else:
        return None


def assign_actual_arrival_time():
    """
    Rus
    На основании исходной таблицы для некоторых пар "транспорт - остановка"
    используются телеметрические данные, чтобы примерно оценить фактическое
    время прибытия транспортного средства на остановку.

    Ключевое предположение: если прогнозируемое время прибытия транспорта
    не превышает две минуты с момента запроса по телеметрическим данным,
    то с большой вероятностью этот автобус / трамвай действительно приедет
    в предсказанное время.
    © "Телеметрические данные достаточно точны, чтобы не ошибаться за две
    минуты до прибытия"️

    Конфигурацию алгоритма можно изменить - см. MIN_FORECAST_HORIZON_SECONDS
    переменную. По умолчанию она установлена как 120 секунд (упомянутые выше
    2 минуты между запросом и прогнозируемым временем прибытия)
    """
    # Load all data and add coordinates to stops
    stop_from_repo = pd.read_csv(Path(get_data_path(), 'stop_from_repo.csv'))
    stop_from_repo = stop_from_repo[['stop_id', 'lat', 'lon']]
    stop_from_repo = stop_from_repo.drop_duplicates()

    df = pd.read_csv(Path(get_data_path(), 'pred_data.csv'), names=COLUMN_NAMES)

    # Assign datetime labels for convenient debugging process
    df['forecast_time_datetime'] = pd.to_datetime(df['forecast_time'], unit='s')
    df['request_time_datetime'] = pd.to_datetime(df['request_time'], unit='s')

    vehicles = list(df['tmId'].unique())
    vehicles.sort()

    final_df = []
    pbar = tqdm(vehicles, colour='blue')
    for vehicle in pbar:
        pbar.set_description(f'Processing vehicle with tmId {vehicle}')
        if vehicle == 0:
            # Skip non reliable data (we don't know exactly the calculation
            # algorithm for rows with label)
            continue

        # Process each vehicle (particular bus, for example)
        vehicle_df = df[df['tmId'] == vehicle]
        for path_id in list(vehicle_df['route_path_id'].unique()):

            # Each route (direction is taken into account)
            route_path_df = vehicle_df[vehicle_df['route_path_id'] == path_id]

            stops_list = list(route_path_df['stop_id'].unique())
            for stop in stops_list:
                try:
                    # Process each stop
                    stop_df = route_path_df[route_path_df['stop_id'] == stop]
                    stop_df = stop_df.merge(stop_from_repo, on='stop_id')

                    # Start actual arrival time calculation only if trans
                    transponder_data = stop_df[stop_df['byTelemetry'] == 1]
                    forecasted_data = stop_df[stop_df['byTelemetry'] == 0]
                    if len(transponder_data) > 0 and len(forecasted_data) > 0:
                        df_arrival_time = define_actual_time(stop_df)
                        if df_arrival_time is not None:
                            final_df.append(df_arrival_time)
                except Exception as ex:
                    print(f'Raised exception {ex}. Skip stop {stop} for vehicle {vehicle}')

    final_df = pd.concat(final_df)
    final_df.to_csv(Path(get_data_path(), 'actual_vs_forecasted.csv'), index=False)


if __name__ == '__main__':
    assign_actual_arrival_time()
