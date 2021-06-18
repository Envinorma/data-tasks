import os
from configparser import ConfigParser
from functools import lru_cache


class _ConfigError(Exception):
    pass


def get_config_variable(section: str, varname: str) -> str:
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


AIDA_URL = 'https://aida.ineris.fr/consultation_document/'
HEROKU_API_KEY = get_config_variable('heroku', 'api_key')
os.environ['HEROKU_API_KEY'] = HEROKU_API_KEY

_OVH_VARIABLE_NAMES = [
    'os_auth_url',
    'os_identity_api_version',
    'os_user_domain_name',
    'os_project_domain_name',
    'os_tenant_id',
    'os_tenant_name',
    'os_username',
    'os_password',
    'os_region_name',
]
for variable_name in _OVH_VARIABLE_NAMES:
    os.environ[variable_name.upper()] = get_config_variable('ovh', variable_name)
