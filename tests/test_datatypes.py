# encoding: utf-8

from marple.datatypes import Domain

def test_that_domain_returns_correct_number_of_files():
    x = Domain("regions/*", datatypes_dir="tests/data/datatypes")
    assert len(x.files) == 6
