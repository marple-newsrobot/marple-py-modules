# encoding: utf-8

from marple.datatypes import Domain

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
