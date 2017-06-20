# encoding: utf-8

import json
import pytest
from jsonschema import Draft4Validator, FormatChecker
from marple.schema import Schema
from marple.csv import CsvFile
from marple.dataset import Dataset
from marple.connection import DatabaseSchemaConnection
from data.config import POSTGREST_URL

BASE_DIR = "tests/data/schema/schemas"
DATATYPES_DIR = "tests/data/datatypes"
connection = DatabaseSchemaConnection(POSTGREST_URL)
TEST_SCHEMA_ID = 'ams-unemployment-monthly-rate-foreignborn'

@pytest.fixture(scope="session")
def get_schema():
    return Schema(TEST_SCHEMA_ID, connection, base_dir=BASE_DIR,
        datatypes_dir=DATATYPES_DIR)

def test_sample_schema(get_schema):
    """ Open a sample schema and do some chekcs
    """
    x = get_schema
    assert len(x.dimensions) == 6
    assert x.label == u"Andel öppet arbetslösa utlandsfödda"
    assert x.id == 'ams-unemployment-monthly-rate-foreignborn'
    assert x.source == "AMS"
    assert x.topic == "unemployment"
    assert x.measure == "rate"
    assert x.periodicity == "monthly"

def test_dimension_metods(get_schema):
    """ Test that dimension metods and props have as they should
    """
    x = get_schema
    dim = x.dimension("gender")
    assert dim.label == u"Kön"
    try:
        assert isinstance(dim.labels(), dict)
    except:
        import pdb;pdb.set_trace()
    assert dim.labels().keys().sort() == dim.allowed_values.sort() 

def test_json_schema_output(get_schema):
    """ Make sure that the json schema output is a valid json schema
    """
    x = get_schema
    with open("tests/data/schema/jsonschema-draft-04.json") as f:
        # Get the schema for jsonschemas (much meta!)
        schema_for_schema = json.load(f)
        validator = Draft4Validator(schema_for_schema, format_checker=FormatChecker())
        assert validator.is_valid(x.as_json_schema)

def test_validate_dataset_with_generated_json_schema():
    """ Apply the json schema generated from the Schema class on an actual
        dataset.
    """
    dataset_id = "ams-unemployment-monthly-count-total"
    dataset = Dataset("tests/data/schema/dataset/{}.json".format(dataset_id))
    connection = DatabaseSchemaConnection(POSTGREST_URL)
    schema = Schema(dataset_id, connection, base_dir="tests/data/schema/schemas",
        datatypes_dir="tests/data/datatypes")

    validator = Draft4Validator(schema.as_json_schema, format_checker=FormatChecker())
    assert validator.is_valid(dataset.json)

def test_language_methods():
    """ Get schema with a an alternative language
    """
    schema_en = Schema(TEST_SCHEMA_ID, connection, base_dir=BASE_DIR,
        datatypes_dir=DATATYPES_DIR, lang="en")

    schema_no_lang = Schema(TEST_SCHEMA_ID, connection, base_dir=BASE_DIR,
        datatypes_dir=DATATYPES_DIR)


    # This one should be translated
    assert schema_en.dimension("timepoint").label == "Timepoint"

    # This one should not be
    assert schema_en.dimension("gender").label == u"Kön"

    labels_en = schema_en.dimension("gender").labels()

    assert labels_en["male"] == "Men"

    defalt_labels = schema_no_lang.dimension("gender").labels()

    assert defalt_labels["male"] == u"Män"
