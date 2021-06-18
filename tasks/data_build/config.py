from typing import Tuple

from envinorma.data_fetcher import DataFetcher

from ..common.config import get_config_variable

LEGIFRANCE_CLIENT_SECRET = get_config_variable('legifrance', 'client_secret')
LEGIFRANCE_CLIENT_ID = get_config_variable('legifrance', 'client_id')
PSQL_DSN = get_config_variable('storage', 'psql_dsn')
SEED_FOLDER = get_config_variable('storage', 'seed_folder')
SECRET_DATA_FOLDER = get_config_variable('storage', 'secret_data_folder')
AM_REPOSITORY_FOLDER = get_config_variable('storage', 'am_repository_folder')
AM_SLACK_URL = get_config_variable('slack', 'am_channel')
DATA_FETCHER = DataFetcher(PSQL_DSN)
DATABASE_NAME = PSQL_DSN.split('/')[-1]


def generate_parametric_descriptor(version_descriptor: Tuple[str, ...]) -> str:
    if not version_descriptor:
        return 'no_date_version'
    return '_AND_'.join(sorted(version_descriptor, reverse=True)).replace(' ', '_')
