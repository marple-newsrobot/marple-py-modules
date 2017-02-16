# encoding: utf-8

import json
import pytest
from jsonschema import Draft4Validator, FormatChecker
from marple.schema import Schema
from marple.dataset import Dataset
from marple.connection import DatabaseSchemaConnection
from data.config import POSTGREST_URL

@pytest.fixture(scope="session")
def get_schema():
    connection = DatabaseSchemaConnection(POSTGREST_URL)
    test_schema_id = 'ams-unemployment-monthly-rate-foreignborn'
    return Schema(test_schema_id, connection, base_dir="tests/data/schema/schemas",
        datatypes_dir="tests/data/datatypes")

def test_sample_schema(get_schema):
    """ Open a sample schema and do some chekcs
    """
    x = get_schema
    assert len(x.dimensions) == 6
    assert x.label == u"Andel öppet arbetslösa utlandsfödda"
    assert x.id == 'ams-unemployment-monthly-rate-foreignborn'
    assert x.source == "AMS"
    assert x.topic == "unemployment"

def test_dimension_metods(get_schema):
    """ Test that dimension metods and props have as they should
    """
    x = get_schema
    dim = x.dimension("gender")
    assert dim.label == u"Kön"
    assert isinstance(dim.labels, dict)
    assert dim.labels.keys().sort() == dim.allowed_values.sort() 

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
    dataset_id = "ams-unemployment-monthly-count-total"
    dataset = Dataset("tests/data/schema/dataset/{}.json".format(dataset_id))
    connection = DatabaseSchemaConnection(POSTGREST_URL)
    schema = Schema(dataset_id, connection, base_dir="tests/data/schema/schemas",
        datatypes_dir="tests/data/datatypes")

    validator = Draft4Validator(schema.as_json_schema, format_checker=FormatChecker())
    assert validator.is_valid(dataset.json)
