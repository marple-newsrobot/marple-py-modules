# encoding: utf-8

from marple.postgrest import Api
from data.config import POSTGREST_URL, POSTGREST_TABLES
import pytest


@pytest.fixture(scope="session")
def get_api():
    return Api(POSTGREST_URL)


def test_basic_get(get_api):
    """ Test a basic get request to marple api
    """
    api = get_api
    for table in POSTGREST_TABLES:
        r = api.get(table).select("id").request()
        assert r.status_code == 200


def test_in_query(get_api):
    """ Make .is_in() query
    """
    api = get_api
    r = api.get("dataset")\
        .select("id")\
        .is_in("id",[
            u"brå-reported_crime_by_crime_type-monthly-count-bilstölder (stöldbrott)",
            u"brå-reported_crime_by_crime_type-monthly-count-alkohol och narkotikabrott"
            ])\
        .request()

    assert r.status_code == 200
    assert len(r.json()) == 2
