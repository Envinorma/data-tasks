import os
from configparser import ConfigParser
from functools import lru_cache
from typing import Tuple

from envinorma.data_fetcher import DataFetcher


class _ConfigError(Exception):
    pass


def _get_var(section: str, varname: str) -> str:
    env_key = f'{section}_{varname}'.upper()
    if env_key in os.environ:
        return os.environ[env_key]
    config = load_config()
    try:
        return config[section][varname]
    except KeyError:
        raise _ConfigError(f'Variable {varname} must either be defined in config.ini or in environment.')


@lru_cache
def load_config() -> ConfigParser:
    parser = ConfigParser()
    parser.read('config.ini')
    return parser


AIDA_URL = _get_var('aida', 'base_url')
LEGIFRANCE_CLIENT_SECRET = _get_var('legifrance', 'client_secret')
LEGIFRANCE_CLIENT_ID = _get_var('legifrance', 'client_id')
AM_DATA_FOLDER = _get_var('storage', 'am_data_folder')
PSQL_DSN = _get_var('storage', 'psql_dsn')
SEED_FOLDER = _get_var('storage', 'seed_folder')
SECRET_DATA_FOLDER = _get_var('storage', 'secret_data_folder')


def get_parametric_ams_folder(am_id: str) -> str:
    return f'{AM_DATA_FOLDER}/parametric_texts/{am_id}'


def generate_parametric_descriptor(version_descriptor: Tuple[str, ...]) -> str:
    if not version_descriptor:
        return 'no_date_version'
    return '_AND_'.join(version_descriptor).replace(' ', '_')


def create_folder_and_generate_parametric_filename(am_id: str, version_descriptor: Tuple[str, ...]) -> str:
    folder_name = get_parametric_ams_folder(am_id)
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    return get_parametric_ams_folder(am_id) + '/' + generate_parametric_descriptor(version_descriptor) + '.json'


DATA_FETCHER = DataFetcher(PSQL_DSN)
