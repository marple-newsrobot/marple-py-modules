# encoding: utf-8
import pytest
import json
from copy import deepcopy
from marple.connection import (LocalConnection, DatabaseSchemaConnection,
    DatabaseDatasetConnection, DatabaseConnection, DatabaseRecipeConnection,
    AWSConnection)    
from marple.dataset import Dataset
from data.config import (POSTGREST_URL, POSTGREST_JWT_TOKEN, POSTGREST_ROLE,
    AWS_ACCESS_ID, AWS_ACCESS_KEY)


def test_get_with_local_connection():
    """ Should open all files in data/connection and verify count
    """
    connection = LocalConnection("tests/data/connection")
    files = [x for x in connection.get()]
    expected_number_of_files_in_folder = 2
    assert len(files) == expected_number_of_files_in_folder

def test_get_by_id_with_local_connection():
    """ Open file1.json and verify that content is correct
    """
    connection = LocalConnection("tests/data/connection")
    file_content = connection.get_by_id("file1")
    assert file_content == { "id": "file1" } 


def test_missing_file_should_not_exist():
    connection = LocalConnection("tests/data/connection")
    assert connection.exists(id="missing_file") == False

def test_get_files_with_invalid_query():
    connection = LocalConnection("tests/data/connection")

    with pytest.raises(ValueError):
        connection.get(missing_key="foo")


def test_list_schemas_from_api():
    """ Make sure that listing schemas from database works
    """
    connection = DatabaseSchemaConnection(POSTGREST_URL)
    schemas = connection.get()
    assert len(schemas) > 0
    assert isinstance(schemas[0], unicode)

def test_list_recipes_from_api():
    """ Try to get every recipe one by one
        This mote of a test for the api
    """
    connection = DatabaseRecipeConnection(POSTGREST_URL)
    recipes = connection.get()
    assert len(recipes) > 0
    assert isinstance(recipes[0], unicode)

def test_get_recipe_from_api():
    """ 
    """
    connection = DatabaseRecipeConnection(POSTGREST_URL)
    # This test depends on the name of the production recipe
    # not changing, not optimal.
    recipe = connection.get(id="ams-unemployment-monthly.json")

    # Should work without .json also
    recipe = connection.get(id="ams-unemployment-monthly")

def test_get_schema_from_api():
    """ Make sure that listing schemas from database works
    """
    connection = DatabaseSchemaConnection(POSTGREST_URL)
    resp = connection.get_by_id("marple-dataset.json")
    assert resp['$schema'] == 'http://json-schema.org/draft-04/schema#'
    assert isinstance(resp,dict)

def test_get_recipes_from_api():
    """ Make sure that listing schemas from database works
    """
    connection = DatabaseRecipeConnection(POSTGREST_URL)
    recipes = connection.get()
    for recipe_id in recipes:
        recipe = connection.get_by_id(recipe_id)
        # TODO: Assert that the recipe actully matches json schema
        assert len(recipe.keys()) > 0

def test_get_recipe_with_unicode_id_and_no_json():
    connection = DatabaseRecipeConnection(POSTGREST_URL)
    recipe = connection.get_by_id(u"brå-reported_crime_by_crime_type-monthly")
    assert len(recipe.keys()) > 0

# ===================
#    DATASET TESTS
# ===================
@pytest.fixture(scope="session")
def database_dataset_connection():
    """ Set up database dataset test by adding an existing dataset
    """
    return DatabaseDatasetConnection(POSTGREST_URL, "dataset_test",
        jwt_token=POSTGREST_JWT_TOKEN, db_role=POSTGREST_ROLE)


@pytest.fixture(scope="session")
def get_existing(database_dataset_connection):
    connection = database_dataset_connection
    # Add an pre-existing dataset to the database
    existing_ds = Dataset(u"tests/data/connection/dataset/ams-unemployment-monthly-count-total-2016-09.json")
    dataset_id = existing_ds.extension["id"]
    r = connection.store(dataset_id, existing_ds.json, on_existing="override")
    if r.status_code not in [201,204]:
        raise ValueError("Error setting up database connection")     
    
    return existing_ds


def test_append_dataset_on_database_connection(database_dataset_connection, get_existing):
    """ Append a dataset to an existing one
    """
    connection, existing_ds = database_dataset_connection, get_existing
    new_ds = ds = Dataset(u"tests/data/connection/dataset/ams-unemployment-monthly-count-total-2016-10.json")
    original_ds = deepcopy(existing_ds)
    existing_ds.append(new_ds)
    dataset_id = existing_ds.extension["id"]
    r = connection.store(dataset_id, existing_ds.json)
    assert r.status_code == 204

    # Fetch the newly created dataset and make sure it is in sync with original
    json_data_from_db = connection.get_by_id(dataset_id)
    ds_from_db = Dataset(json_data_from_db)
    assert ds_from_db.dimension("timepoint").length == 2
    assert ds_from_db.dimension("region").length == original_ds.dimension("region").length

def test_override_existing_dataset_on_database_connection(database_dataset_connection, get_existing):
    """ Override an existing dataset with on_existing="override" param
    """
    connection, existing_ds = database_dataset_connection, get_existing
    
    # Add a new dataset with override=Ture
    new_ds = ds = Dataset(u"tests/data/connection/dataset/ams-unemployment-monthly-count-total-2016-10.json")
    original_ds = deepcopy(existing_ds)
    dataset_id = existing_ds.extension["id"]
    connection.store(dataset_id, new_ds.json, on_existing="override")

    # Get and validate the new dataset from db
    json_data_from_db = connection.get_by_id(dataset_id)
    ds_from_db = Dataset(json_data_from_db)

    timepoint_new = ds_from_db.dimension("timepoint").labels.keys()[0]
    timepoint_original = original_ds.dimension("timepoint").labels.keys()[0]
    
    assert timepoint_new != timepoint_original



# ==========================
#    ALARM TESTS
# ==========================
@pytest.fixture(scope="session")
def database_alarm_connection():
    return DatabaseConnection(POSTGREST_URL, "alarm_test",
        jwt_token=POSTGREST_JWT_TOKEN, db_role=POSTGREST_ROLE)

def test_add_alarm_on_database_connection(database_alarm_connection):
    """ Trying adding an alarm to database
    """
    connection = database_alarm_connection
    with open("tests/data/connection/alarm/example_alarm.json") as f:
        alarm = json.load(f)
    r = connection.store(alarm["id"], alarm)
    assert r.status_code in [201, 204]


def test_alarm_connection_on_database_without_auth():
    """ Make a basic request without auth
    """
    connection = DatabaseConnection(POSTGREST_URL, "alarm_test")
    # Sample query
    assert connection.exists(id="foo") == False

def test_query_alarms_with_list_on_database_connection():
    """ Make a query with a list of regions
        This test depends on the old db entries not changing
    """
    connection = DatabaseConnection(POSTGREST_URL, "alarm")
    alarms = connection.get(source="AMS", trigger_date="2016-10-01",
        region=[u"Älmhults kommun", u"Åmåls kommun"])
    assert len(alarms) == 7

def test_delete_alarm():
    connection = DatabaseConnection(POSTGREST_URL, "alarm_test",
        jwt_token=POSTGREST_JWT_TOKEN, db_role=POSTGREST_ROLE)
    with open("tests/data/connection/alarm/example_alarm.json") as f:
        alarm = json.load(f)
    r = connection.store(alarm["id"], alarm)
    if r.status_code in [201, 204]:
        r = connection.delete(alarm["id"])
        assert r.status_code == 204
    else:

        assert False, "Error setting up test"

# ==========================
#    NEWSLEAD TESTS
# ==========================
@pytest.fixture(scope="session")
def database_newslead_connection():
    return DatabaseConnection(POSTGREST_URL, "newslead_test",
        jwt_token=POSTGREST_JWT_TOKEN, db_role=POSTGREST_ROLE)

def test_add_newslead_on_database_connection(database_newslead_connection):
    """ Trying adding an alarm to database
    """
    connection = database_newslead_connection
    with open("tests/data/connection/newslead/example_newslead.json") as f:
        newslead = json.load(f)
    r = connection.store(newslead["id"], newslead)
    assert r.status_code in [201, 204]


# ============================
#   AMAZON TESTS
# ============================
@pytest.fixture(scope="session")
def get_aws_connection():
    """ Connects to the test bucket
    """ 
    return AWSConnection("marple", AWS_ACCESS_ID, AWS_ACCESS_KEY)


def test_store_text_file_to_s3(get_aws_connection):
    """ Store a simple text file 
    """
    connection = get_aws_connection
    resp = connection.store(u"test_text_file_åäö.txt", u"Hej världen", folder="test")
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

def test_store_image_file_to_s3(get_aws_connection):
    """ Store a simple text file 
    """
    connection = get_aws_connection
    with open("tests/data/connection/sample_chart.png") as f:
        connection.store(u"sample_chart.png", f, folder="test")
