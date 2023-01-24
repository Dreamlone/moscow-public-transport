from pathlib import Path
from typing import Union


def get_project_path() -> Path:
    return Path(__file__).parent.parent


def get_data_path() -> Path:
    """ Return path for folder with csv files """
    return Path(get_project_path(), 'data')


def create_folder(folder: Union[str, Path]):
    """ Create desired folder """
    if isinstance(folder, str):
        folder = Path(folder)
    folder = folder.resolve()
    folder.mkdir(parents=True, exist_ok=True)

    return folder
