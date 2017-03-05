# encoding: utf-8

import pytest
from marple.datatypes import Domain
from marple.utils import isNaN

def test_that_domain_returns_correct_number_of_files():
    x = Domain("regions/*", datatypes_dir="tests/data/datatypes")
    assert len(x.files) == 6

def test_that_childen_method_works():
    x = Domain("regions/*", datatypes_dir="tests/data/datatypes")
    assert len(x.children(u"Stockholms län")) == 26

def test_children_method_when_parent_column_is_missing():
    x = Domain("misc/age_groups", datatypes_dir="tests/data/datatypes")
    assert len(x.children(u"20-64")) == 0


def test_that_parent_returns_parent():
    x = Domain("regions/*", datatypes_dir="tests/data/datatypes")
    assert x.parent(u"Stockholms län") == "Sweden"

def test_that_parentless_region_returns_none():
    x = Domain("regions/*", datatypes_dir="tests/data/datatypes")
    assert x.parent(u"Sweden") == None

def test_that_missing_row_returns_throws_error():
    x = Domain("regions/*", datatypes_dir="tests/data/datatypes")
    with pytest.raises(KeyError):
        assert x.row("i_do_not_exist")

def test_labels():
    domain = Domain("misc/test", datatypes_dir="tests/data/datatypes")

    assert domain.labels() == { "row_id": "Test datatyp" }
    assert domain.labels(lang="en") == { "row_id": "Test datatype in English" }

    assert domain.label("row_id") == "Test datatyp"
    assert domain.label("row_id", lang="en") == "Test datatype in English"
    assert domain.label("row_id", lang="non_existing") == "Test datatyp"

    regions_domain = Domain("regions/*", datatypes_dir="tests/data/datatypes")

    for index, label in regions_domain.labels().iteritems():
        msg = u"No label for '{}'".format(index)
        assert not isNaN(label) and label is not None, msg 

    for index, label in regions_domain.labels(lang="en").iteritems():
        msg = u"No label for '{}'".format(index)
        assert not isNaN(label) and label is not None, msg 