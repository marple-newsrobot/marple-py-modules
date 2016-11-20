# encoding: utf-8

import json
from jsonschema import Draft4Validator, FormatChecker
from marple.schema import Schema
from marple.connection import DatabaseSchemaConnection
from data.config import POSTGREST_URL


def test_sample_schema():
    """ Open a sample schema and do some chekcs
    """
    connection = DatabaseSchemaConnection(POSTGREST_URL)
    test_schema_id = 'ams-unemployment-monthly-rate-foreignborn'
    x = Schema(test_schema_id, connection)
    assert len(x.dimensions) == 6
    assert x.label == u"Andel öppet arbetslösa utlandsfödda"
    assert x.id == test_schema_id
    assert x.source == "AMS"
    assert x.topic == "unemployment"

def test_dimension_metods():
    """ Test that dimension metods and props have as they should
    """
    connection = DatabaseSchemaConnection(POSTGREST_URL)
    test_schema_id = 'ams-unemployment-monthly-rate-foreignborn'
    x = Schema(test_schema_id, connection)
    dim = x.dimension("gender")
    assert dim.label == u"Kön"
    assert isinstance(dim.labels, dict)
    assert dim.labels.keys().sort() == dim.allowed_values.sort() 

def test_json_schema_output():
    """ Make sure that the json schema output is a valid json schema
    """
    connection = DatabaseSchemaConnection(POSTGREST_URL)
    test_schema_id = 'ams-unemployment-monthly-rate-foreignborn'
    x = Schema(test_schema_id, connection)
    with open("tests/data/schema/jsonschema-draft-04.json") as f:
        # Get the schema for jsonschemas (much meta!)
        schema_for_schema = json.load(f)
        validator = Draft4Validator(schema_for_schema, format_checker=FormatChecker())
        assert validator.is_valid(x.as_json_schema)
