from pathlib import Path


def get_project_path() -> Path:
    return Path(__file__).parent.parent


def get_data_path() -> Path:
    """ Return path for folder with csv files """
    return Path(get_project_path(), 'data')
