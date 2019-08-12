# encoding: utf-8

import pytest
from glob import glob
from copy import deepcopy
import json
import pandas as pd

from marple.dataset import Dataset, MalformedJSONStat, MergeFailure
from data.dataset.dataset_example_data import *
from jsonschema.exceptions import ValidationError

# ==== INITIALIZATION TESTS ======


def test_init_from_json():
    ds = Dataset().from_json(deepcopy(complete_dataset))
    ds2 = Dataset(deepcopy(complete_dataset))
    assert ds.json == complete_dataset
    assert ds2.json == complete_dataset


def test_init_from_file():
    """ Test agains all tests/data/dataset/dataset_*.json files
    """
    files = glob("tests/data/dataset/dataset_*.json")
    for file_path in files:
        ds = Dataset(file_path)
        with open(file_path) as f:
            json_data = json.load(f)
        assert ds.json == json_data

def test_init_from_dataframe():
    """ Test to init from simple Pandas dataframe
    """
    df = pd.DataFrame().from_csv("tests/data/dataset/dataset_dataframe.csv").reset_index()
    ds = Dataset(df)
    assert len(ds.dimensions) == 2
    assert ds.dimension("gender")
    assert ds.dimension("region")
    assert ds.json["size"] == [2,2]


# ==== VALIDATION TESTS ======
def test_json_data_with_missing_req_props_should_error():
    """ Make sure that incomplete json data throws error
    """
    faulty_jsons = [
        { "value": "foo", "size": "bar" },
        { "value": [], "size": [] },
        { "id": [], "size": [] },
        { "value": [], "id": [] },
    ]
    for json_data in faulty_jsons:
        with pytest.raises(ValidationError):
            Dataset().from_json(json_data)

def test_json_data_with_missing_dimension():
    """ Make sure that json data with a missing dimension throws error
    """
    with pytest.raises(MalformedJSONStat):
        Dataset().from_json(missing_dimension_dataset)

def test_json_data_with_incomplete_id():
    """ Make sure that json data with missing id property throws exception
    """
    with pytest.raises(MalformedJSONStat):
        Dataset().from_json(missing_id_dataset)

def test_json_data_with_wrong_size():
    """ Make sure that json data with size property error throws exception
    """
    with pytest.raises(MalformedJSONStat):
        Dataset().from_json(wrong_size_dataset)

def test_json_data_with_wrong_value_length():
    """ Make sure that json data with size property error throws exception
    """
    with pytest.raises(MalformedJSONStat):
        Dataset().from_json(wrong_value_length_dataset)

# ==== TEST METHODS ====
def test_print_unicodes():
    ds = Dataset().from_json(complete_dataset)
    print ds


def test_add_category_labels():
    """ Test adding labels to a dimension
    """
    gender_labels = { "M": "Male", "F": "Female" }
    ds = Dataset().from_json(complete_dataset)
    ds.add_labels("gender", gender_labels)
    assert ds.dimension("gender").labels == gender_labels

def test_transform_to_table():
    """ Make sure that a table outputed from dataset has same length as original
    """
    ds = Dataset().from_json(deepcopy(complete_dataset))
    table = ds.to_table()
    assert len(table) - 1 == len(ds.json["value"])

def test_dataframe_to_dataset_and_back():
    """ Create a dataset from csv, transform back to dataframe and
        assert they are the same.
    """
    df = pd.DataFrame().from_csv("tests/data/dataset/dataset_dataframe.csv").reset_index()
    ds = Dataset().from_dataframe(df)
    new_df = ds.to_dataframe()
    m_sthlm = new_df.loc[(new_df.gender=="M") & (new_df.region=="Stockholm"),:]
    assert m_sthlm.value[0] == 2
    assert m_sthlm.status[0] == "foo"
    assert new_df.gender.unique().tolist() == ["M", "F"]
    assert new_df.region.unique().tolist() == ["Stockholm", "Solna"]

def test_filter_dataset():
    ds = Dataset().from_json(deepcopy(complete_dataset))
    ds.filter_by_query({"region": "Stockholm"})

def _assert_metadata_equality(ds1, ds2):
    """ Assert that meta data is equal in two datasets
    """
    assert ds1.source == ds2.source
    assert ds1.label == ds2.label
    assert ds1.extension == ds2.extension
    assert ds1.note == ds2.note

def _assert_metadata_equality_in_dimensions(ds1, ds2):
    """ Assert that meta data is equal in two datasets
    """
    for dim in ds1.dimensions:
        dim2 = ds2.dimension(dim.id)
        assert dim.label == dim2.label
        assert dim.note == dim2.note

def _assert_metadata_equality_in_categories(ds1, ds2):
    """ Assert that meta data is equal in two datasets
    """
    for dim1 in ds1.dimensions:
        dim2 = ds2.dimension(dim1.id)
        for cat1 in dim1.categories:
            try:
                cat2 = dim2.category(cat1.id)
                assert cat1.label == cat2.label
                assert cat1.note == cat2.note
            except KeyError:
                pass

def test_that_metadata_is_preserved_after_filter():
    ds = Dataset().from_json(deepcopy(complete_dataset))
    original_ds = deepcopy(ds)
    ds.filter_by_query({"region": "Stockholm"})
    _assert_metadata_equality(original_ds, ds)
    _assert_metadata_equality_in_dimensions(original_ds, ds)

def test_that_metadata_is_preserved_after_append():
    """ Merge two datasets and test that metadata (labels etc)
        are preserved.
    """
    ds1 = Dataset().from_json(deepcopy(complete_dataset))
    ds2 = Dataset().from_json(deepcopy(dataset_to_append))
    original_ds = deepcopy(ds1)
    ds1.append(ds2, on_metadata_conflict="preserve")

    _assert_metadata_equality(original_ds, ds1)

    # Make sure that all category labels are preserved
    for dim2 in ds2.dimensions:
        dim1 = ds1.dimension(dim2.id)
        original_dim = original_ds.dimension(dim2.id)
        for cat in dim1.categories:
            new_label = dim1.category(cat.id).label
            try:
                # All category labels that existed in the original dataset
                # should be preserved
                original_label = original_dim.category(cat.id).label
                assert original_label == new_label
            except KeyError:
                # Category labels that only existed in the appended dataset
                # should fetched from that
                appended_label = dim2.category(cat.id).label
                assert appended_label == new_label

def test_that_metadata_is_updated_after_append():
    """ Merge two datasets and test that metadata (labels etc)
        are updated if on_metadata_conflict is set to that.
    """
    ds1 = Dataset().from_json(deepcopy(complete_dataset))
    ds2 = Dataset().from_json(deepcopy(dataset_to_append_with_overlap))
    original_ds = deepcopy(ds2)
    ds1.append(ds2, on_metadata_conflict="update", on_duplicates="update")
    region = ds1.dimension("region")

    assert ds1.note[0] == "This is a conflicting dataset note"
    assert region.note[0] == "This is conflicting note"
    assert region.category("Solna").note[0] == "Conflicting Solna note"

def test_that_length_is_correct_after_append():
    """ Merge two datasets and assert that it has grown as expected
    """
    ds1 = Dataset().from_json(deepcopy(complete_dataset))
    ds2 = Dataset().from_json(deepcopy(dataset_to_append))
    original_ds = deepcopy(ds1)
    ds1.append(ds2)
    assert ds1.length == 6
    assert ds1.dimension("region").length == 3


def test_append_with_update():
    """ Merge two datasets with overlapping data
    """
    ds1 = Dataset().from_json(deepcopy(complete_dataset))
    ds2 = Dataset().from_json(deepcopy(dataset_to_append_with_overlap))

    with pytest.raises(MergeFailure):
        ds1.append(ds2)

    ds1.append(ds2, on_duplicates="preserve").to_dataframe()
    assert ds1.json["value"] == [1,2,3,4]

    ds1.append(ds2, on_duplicates="update").to_dataframe()
    assert ds1.json["value"] == [1,2,50,60]

    assert ds1.note == ["My dataset note"]
    assert ds1.dimension("region").note == ["My region note"]
    assert ds1.dimension("region").category("Solna kommun").note == ["My Solna note"]

def test_bra_merge_example():
    """ Test case for bug
    """
    ds1 = Dataset().from_file("tests/data/dataset/dataset_bra_merge_existing.json")
    ds2 = Dataset().from_file("tests/data/dataset/dataset_bra_merge_to_append.json")
    ds1.append(ds2, on_duplicates="update")

    #assert len(ds1.dimension("region").categories[-1].note) == 1
    #_assert_metadata_equality(ds1, ds2)
    _assert_metadata_equality_in_dimensions(ds1, ds2)
    _assert_metadata_equality_in_categories(ds1, ds2)


def test_categories():
    ds = Dataset(deepcopy(complete_dataset))
    ds.dimension("gender").categories


def test_read_notes():
    ds = Dataset(deepcopy(complete_dataset))
    assert ds.note == ["My dataset note"]
    assert ds.dimension("region").note == ["My region note"]
    assert ds.dimension("region").category("Solna kommun").note == ["My Solna note"]
    assert len(ds.all_notes) == 3

def test_write_notes():
    # Write dataset note
    ds = Dataset(deepcopy(complete_dataset))
    ds.add_note("My second dataset note")
    assert ds.note[1] == "My second dataset note"
    assert len(ds.note) == 2

    # Write dimension note
    dim = ds.dimension("region")
    dim.add_note("My second region note")
    assert dim.note[1] == "My second region note"
    assert len(dim.note) == 2

    # Write category note
    cat = dim.category("Solna kommun")
    cat.add_note("My second Solna note")
    assert cat.note[1] == "My second Solna note"
    assert len(cat.note) == 2

    # Write dim note to dim without previous notes
    dim2 = ds.dimension("gender")
    dim2.add_note("My note")
    assert dim2.note[0] == "My note"
    assert len(dim2.note) == 1

    # Write cat note to cat without previous notes
    cat2 = dim2.category("M")
    cat2.add_note("My first cat note")
    assert cat2.note[0] == "My first cat note"
    assert len(cat2.note) == 1

def test_notes_from_csv_method():
    # Test the .notes_from_csv() method
    ds = Dataset(deepcopy(complete_dataset))
    ds.notes_from_csv("tests/data/dataset/notes.csv")

    assert ds.note[-1] == "A dataset note from csv"
    assert ds.dimension("gender").note[-1] == "A dimension note from csv"
    assert ds.dimension("gender").category("M").note[-1] == "A category note from csv"

def test_notes_from_csv_method_with_break_on_faulty_data():
    # A notes csv file with reference to a non existing data point should throw error if on_missing="break"
    ds = Dataset(deepcopy(complete_dataset))
    with pytest.raises(KeyError):
        ds.notes_from_csv("tests/data/dataset/notes.csv", on_missing="break")


def test_read_units():
    ds = Dataset(deepcopy(complete_dataset))
    assert ds.dimension("gender").category("M").unit == None
    assert ds.dimension("measure").category("share").unit == {
        "decimals:": 1,
        "label": "%"
    }
    assert ds.dimension("measure").units == {
        "share": {
            "decimals:": 1,
            "label": "%"
        }
    }

def test_set_units():
    ds = Dataset(deepcopy(complete_dataset))
    updated_units = {
        "share": {
            "decimals": 0,
            "label": "pct"
        },
        "non_existing_cat": {
            "decimals": 0
        }
    }
    dim = ds.dimension("measure")
    dim.units = updated_units
    assert dim.category("share").unit == updated_units["share"]

def test_round():
    df = pd.DataFrame([
        ["2018-01-01", "A", 0.1234],
        ["2018-01-02", "A", 0.9876],
    ], columns=["timepoint", "category", "value"])
    ds = Dataset().from_dataframe(df)

    # TODO: Validate
    #ds.to_json_file(output_dir + "/ds1.json", decimals=1)
    #ds.to_json_file("/ds2.json", decimals=2)
