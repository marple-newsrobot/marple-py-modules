# encoding: utf-8

from marple.postgrest import Api
from data.config import POSTGREST_URL, POSTGREST_TABLES

def _get_api():
    return Api(POSTGREST_URL)


def test_basic_get():
    """ Test a basic get request to marple api
    """
    api = _get_api()
    for table in POSTGREST_TABLES:
        r = api.get(table).select("id").request()
        assert r.status_code == 200

