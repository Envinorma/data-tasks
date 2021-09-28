from typing import Tuple

from tasks.common.config import DATA_FETCHER, get_config_variable

LEGIFRANCE_CLIENT_SECRET = get_config_variable('legifrance', 'client_secret')
LEGIFRANCE_CLIENT_ID = get_config_variable('legifrance', 'client_id')
SEED_FOLDER = get_config_variable('storage', 'seed_folder')
SECRET_DATA_FOLDER = get_config_variable('storage', 'secret_data_folder')
GEORISQUES_DATA_FOLDER = get_config_variable('storage', 'georisques_data_folder')
GEORISQUES_DUMP_URL = get_config_variable('georisques', 'data_url')
AM_REPOSITORY_FOLDER = get_config_variable('storage', 'am_repository_folder')
AM_SLACK_URL = get_config_variable('slack', 'am_channel')
DATABASE_NAME = DATA_FETCHER.psql_dsn.split('/')[-1]


def generate_parametric_descriptor(version_descriptor: Tuple[str, ...]) -> str:
    if not version_descriptor:
        return 'no_date_version'
    return '_AND_'.join(sorted(version_descriptor, reverse=True)).replace(' ', '_')
