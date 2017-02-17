# encoding: utf-8
import pytest
import os
from marple.utils import list_files, CsvFile

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

def test_basic_csv_file():
    csv_file = CsvFile("tests/data/utils/simple_csv_file.csv", required_cols=["id","label"])

    with pytest.raises(KeyError):
        csv_file.row("this_id_dont_exist")


    assert csv_file.row("ok_id")["label"] == "This is a valid row"


def test_multiindex_csv_file():
    csv_file = CsvFile("tests/data/utils/multiindex_csv_file.csv", index_col=["id","multi_id"])

    assert csv_file.row(["duplicated_id","a"])["label"] == "This is not okay on multiindex"

    # Shouldn't be able to query multi index with string
    with pytest.raises(KeyError):
        csv_file.row("duplicated_id")

    # ..or to long list
    with pytest.raises(KeyError):
        csv_file.row(["duplicated_id", "foo", "bar"])

def test_append_to_csv_file():
    csv_file = CsvFile("tests/data/utils/simple_csv_file.csv", required_cols=["id","label"])

    # Original length
    assert len(csv_file.data) == 1

    # Add a row
    csv_file.append([{ "id": "new_id", "label": "new label" }])
    assert len(csv_file.data) == 2

    # Try to add a row with existing id
    csv_file.append([{ "id": "new_id", "label": "ignore me, this index already exist" }])    
    assert len(csv_file.data) == 2
    assert csv_file.data.iloc[-1]["label"] == "new label"

    # Try to add a row with new col
    csv_file.append([{ "id": "new_col_row", "new_col": "foo" }])
    assert csv_file.data.iloc[-1]["new_col"] == "foo"

def test_new_csv_file():
    """ Init CsvFile with file that doesn't exist and add row
    """
    path_to_new_file = "tests/data/utils/new_csv_file.csv"
    try:
        os.remove(path_to_new_file)
    except OSError:
        pass
    csv_file = CsvFile(path_to_new_file, required_cols=["id", "label"])
    assert len(csv_file.data) == 0

    csv_file.append([{ "id": "foo", "label": "bar"}])
    assert len(csv_file.data) == 1

    # Store after append
    csv_file.save()
    with open(path_to_new_file) as f:
        lines = f.readlines()
        assert lines[0].strip() == "id,label"
        assert lines[1].strip() == "foo,bar"

