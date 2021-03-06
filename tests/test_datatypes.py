# encoding: utf-8

import pytest
from marple.datatypes import Domain, Datatype
from marple.utils import isNaN

DATATYPES_DIR = "tests/data/datatypes"

def test_that_domain_returns_correct_number_of_files():
    x = Domain("regions/*", datatypes_dir=DATATYPES_DIR)
    assert len(x.files) == 6

def test_that_childen_method_works():
    x = Domain("regions/*", datatypes_dir=DATATYPES_DIR)
    assert len(x.children(u"Stockholms län")) == 26

def test_child_metod_with_kwargs():
    x = Domain("regions/*", datatypes_dir=DATATYPES_DIR)
    assert len(x.children(u"Sweden", region_level="county", end="9")) == 21

def test_children_method_when_parent_column_is_missing():
    x = Domain("misc/age_groups", datatypes_dir=DATATYPES_DIR)
    assert len(x.children(u"20-64")) == 0


def test_that_parent_returns_parent():
    x = Domain("regions/*", datatypes_dir=DATATYPES_DIR)
    assert x.parent(u"Stockholms län") == "Sweden"

def test_that_parentless_region_returns_none():
    x = Domain("regions/*", datatypes_dir=DATATYPES_DIR)
    assert x.parent(u"Sweden") == None

def test_that_missing_row_returns_throws_error():
    x = Domain("regions/*", datatypes_dir=DATATYPES_DIR)
    with pytest.raises(KeyError):
        assert x.row("i_do_not_exist")

def test_labels():
    domain = Domain("misc/test", datatypes_dir=DATATYPES_DIR)

    assert domain.labels() == { "row_id": "Test datatyp" }
    assert domain.labels(lang="en") == { "row_id": "Test datatype in English" }

    assert domain.label("row_id") == "Test datatyp"
    assert domain.label("row_id", lang="en") == "Test datatype in English"
    assert domain.label("row_id", lang="non_existing") == "Test datatyp"

    regions_domain = Domain("regions/*", datatypes_dir=DATATYPES_DIR)

    for index, label in regions_domain.labels().items():
        msg = u"No label for '{}'".format(index)
        assert not isNaN(label) and label is not None, msg

    for index, label in regions_domain.labels(lang="en").items():
        msg = u"No label for '{}'".format(index)
        assert not isNaN(label) and label is not None, msg

def test_missing_label():
    # This file does not have a label column, and should instead return id
    periodicity_domain = Domain("misc/periodicity", datatypes_dir=DATATYPES_DIR)
    assert periodicity_domain.label("yearly") == "yearly"

def test_datatype_basics():
    d = Datatype("gender", datatypes_dir=DATATYPES_DIR)
    allowed_values = d.allowed_values
    assert len(allowed_values) == 5
    assert isinstance(d.data, dict)
    assert d.id == "gender"

def test_labels_in_multiple_languages():
    gender_en = Datatype("gender", datatypes_dir=DATATYPES_DIR)
    gender_sv = Datatype("gender", datatypes_dir=DATATYPES_DIR, lang="sv")

    assert gender_en.label == "Gender"
    assert gender_sv.label == u"Kön"
