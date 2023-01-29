import sqlite3
from pathlib import Path
import pandas as pd

from mostra.paths import get_data_path

import warnings
warnings.filterwarnings('ignore')


def check_table():
    """
    Rus
    Исследование файла БД. Доступные сущности:
    'snapshot_list_routes', 'routes', 'stops', 'route_stop',
    'route_stop_times', 'timetable', 'route_timetable_date'
    """
    path_to_file = Path(get_data_path(), 'mosgortrans_20220115.sqlite')

    con = sqlite3.connect(path_to_file)
    cur = con.cursor()
    table_list = [a for a in cur.execute("SELECT name FROM sqlite_master WHERE type = 'table'")]

    for table_name in table_list:
        table_name = table_name[0]
        table_cursor = cur.execute(f"SELECT * FROM {table_name} LIMIT 500")
        columns = [desc[0] for desc in table_cursor.description]
        table = table_cursor.fetchall()
        dataframe = pd.DataFrame(table, columns=columns)

        print(f'\nРассматриваемая таблица: {table_name}')
        print(dataframe)

    con.close()


if __name__ == '__main__':
    check_table()
