from functools import lru_cache

from leginorma import LegifranceClient

from ..common.config import get_config_variable

LEGIFRANCE_CLIENT_SECRET = get_config_variable('legifrance', 'client_secret')
LEGIFRANCE_CLIENT_ID = get_config_variable('legifrance', 'client_id')
AM_SLACK_URL = get_config_variable('slack', 'am_channel')


@lru_cache
def get_legifrance_client() -> LegifranceClient:
    return LegifranceClient(LEGIFRANCE_CLIENT_ID, LEGIFRANCE_CLIENT_SECRET)
