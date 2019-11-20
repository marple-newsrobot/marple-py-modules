# encoding: utf-8
from marple.utils import (list_files, guess_periodicity, to_timepoint,
    subtract_periods, parse_lingual_object, get_decimal_encoder, parse_decimal)
import pytest
import json
import numpy as np
from decimal import Decimal

def test_list_files():
    directory = "tests/data/utils/list_files"

    all_files = list_files(directory)
    txt_files1 = list_files(directory,extension="txt")
    txt_files2 = list_files(directory,extension=".txt")
    by_file_name = list_files(directory,file_name="bar.txt")

    assert len(all_files) == 3
    assert len(txt_files1) == 2
    assert len(txt_files2) == 2
    assert len(by_file_name) == 1

def test_guess_periodicity():
    assert guess_periodicity("2015") == "yearly"
    assert guess_periodicity(2015) == "yearly"
    assert guess_periodicity("2015-01") == "monthly"
    assert guess_periodicity("2015K1") == "quarterly"
    assert guess_periodicity("2015Q1") == "quarterly"
    assert guess_periodicity("2015-K1") == "quarterly"
    assert guess_periodicity("2015-Q1") == "quarterly"


def test_to_timepoint():
    assert to_timepoint("2015") == "2015-01-01"
    assert to_timepoint(2015) == "2015-01-01"
    assert to_timepoint("2015-03") == "2015-03-01"
    assert to_timepoint("2015Q1") == "2015-01-01"
    assert to_timepoint("2015K2") == "2015-04-01"
    assert to_timepoint("2015-Q3") == "2015-07-01"
    assert to_timepoint("2015-K4") == "2015-10-01"


def test_subtract_periods():
    assert subtract_periods(2015, 2, "yearly") == "2013-01-01"
    assert subtract_periods("2015-03", 2, "monthly") == "2015-01-01"
    assert subtract_periods("2015-07-01", 6, "monthly") == "2015-01-01"

def test_parse_lingual_object():
    obj = {
        "en": "dog",
        "sv": "hund",
        "label": u"also dog",
        "label__sv": u"också hund",
    }
    assert parse_lingual_object(obj, "en") == "dog"
    assert parse_lingual_object(obj, "sv") == "hund"
    assert parse_lingual_object(obj, None) == "dog"
    assert parse_lingual_object("dog") == "dog"

    assert parse_lingual_object(obj, prefix="label") == u"also dog"
    assert parse_lingual_object(obj, "sv", prefix="label") == u"också hund"

    with pytest.raises(KeyError):
        parse_lingual_object(obj,"foo")

def test_parse_decimal():
    assert parse_decimal(1.23) == 1.23
    assert parse_decimal(123) == 123
    assert parse_decimal(None) == None
    assert parse_decimal(np.nan) == None

def test_decimal_encoder():
    data = {
        "value": Decimal(1.546),
    }
    assert(json.dumps(data, cls=get_decimal_encoder(1)) == '{"value": 1.5}')
    assert(json.dumps(data, cls=get_decimal_encoder(2)) == '{"value": 1.55}')
    assert(json.dumps(data, cls=get_decimal_encoder(3)) == '{"value": 1.546}')
