# encoding: utf-8

from marple.connection import LocalConnection, DatabaseSchemaConnection
from data.config import POSTGREST_URL

def test_get_with_local_connection():
    """ Should open all files in data/connection and verify count
    """
    connection = LocalConnection("tests/data/connection")
    files = [x for x in connection.get()]
    assert len(files) == 2
    assert files[0] == { "id": "file1" } 

def test_get_by_id_with_local_connection():
    """ Open file1.json and verify that content is correct
    """
    connection = LocalConnection("tests/data/connection")
    file_content = connection.get_by_id("file1")
    assert file_content == { "id": "file1" } 


def test_list_schemas_from_api():
    """ Make sure that listing schemas from database works
    """
    connection = DatabaseSchemaConnection(POSTGREST_URL)
    schemas = connection.get()
    assert len(schemas) > 0
    assert isinstance(schemas[0], unicode)


def test_get_schema_from_api():
    """ Make sure that listing schemas from database works
    """
    connection = DatabaseSchemaConnection(POSTGREST_URL)
    resp = connection.get_by_id("marple-dataset.json")
    assert "name" in resp
    assert "schema" in resp
    assert "git_hash" in resp
    assert isinstance(resp["schema"],dict)
