import os
from typing import Tuple

from envinorma.data_fetcher import DataFetcher

from shared_config import get_config_variable

LEGIFRANCE_CLIENT_SECRET = get_config_variable('legifrance', 'client_secret')
LEGIFRANCE_CLIENT_ID = get_config_variable('legifrance', 'client_id')
AM_DATA_FOLDER = get_config_variable('storage', 'am_data_folder')
PSQL_DSN = get_config_variable('storage', 'psql_dsn')
SEED_FOLDER = get_config_variable('storage', 'seed_folder')
SECRET_DATA_FOLDER = get_config_variable('storage', 'secret_data_folder')
AM_SLACK_URL = get_config_variable('slack', 'am_channel')
DATA_FETCHER = DataFetcher(PSQL_DSN)

if not os.path.exists(AM_DATA_FOLDER):
    os.mkdir(AM_DATA_FOLDER)


def get_parametric_ams_folder(am_id: str) -> str:
    return f'{AM_DATA_FOLDER}/{am_id}'


def generate_parametric_descriptor(version_descriptor: Tuple[str, ...]) -> str:
    if not version_descriptor:
        return 'no_date_version'
    return '_AND_'.join(version_descriptor).replace(' ', '_')


def create_folder_and_generate_parametric_filename(am_id: str, version_descriptor: Tuple[str, ...]) -> str:
    folder_name = get_parametric_ams_folder(am_id)
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    return get_parametric_ams_folder(am_id) + '/' + generate_parametric_descriptor(version_descriptor) + '.json'
