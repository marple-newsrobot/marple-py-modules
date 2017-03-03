# encoding: utf-8
import pytest
import os
from marple.csv import CsvFile, DimensionsCsv

DATA_DIR_PATH = "tests/data/csv/"

def test_basic_csv_file():
    csv_file = CsvFile(os.path.join(DATA_DIR_PATH, "simple_csv_file.csv"), required_cols=["id","label"])

    with pytest.raises(KeyError):
        csv_file.row("this_id_dont_exist")


    assert csv_file.row("ok_id")["label"] == "This is a valid row"

    assert csv_file.row("ok_id")["empty"] is None

    assert csv_file.to_dictlist() == [
        {
            "id": "ok_id",
            "label": "This is a valid row",
            "empty": None
        }
    ]


def test_multiindex_csv_file():
    file_path = os.path.join(DATA_DIR_PATH,"multiindex_csv_file.csv")
    csv_file = CsvFile(file_path, index_col=["id","multi_id"])

    assert csv_file.row(["duplicated_id","a"])["label"] == "This is not okay on multiindex"

    # Shouldn't be able to query multi index with string
    with pytest.raises(KeyError):
        csv_file.row("duplicated_id")

    # ..or to long list
    with pytest.raises(KeyError):
        csv_file.row(["duplicated_id", "foo", "bar"])

    csv_file.save()
    with open(file_path) as f:
        lines = f.readlines()
        assert lines[0].strip() == "id,multi_id,label"

def test_append_to_csv_file():
    csv_file = CsvFile(os.path.join(DATA_DIR_PATH,"simple_csv_file.csv"), required_cols=["id","label"])

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
    path_to_new_file = os.path.join(DATA_DIR_PATH,"new_csv_file.csv")

    def setup():
        """ Remove existing
        """
        try:
            os.remove(path_to_new_file)
        except OSError:
            pass

    setup()
    csv_file = CsvFile(path_to_new_file, required_cols=["id", "label"])
    assert len(csv_file.data) == 0

    with open(path_to_new_file) as f:
        lines = f.readlines()
        assert len(lines) == 1
        assert lines[0].strip() == "id,label"

    csv_file.append([{ "id": "foo", "label": "bar"}])
    assert len(csv_file.data) == 1

    # Store after append
    csv_file.save()
    with open(path_to_new_file) as f:
        lines = f.readlines()
        assert lines[0].strip() == "id,label"
        assert lines[1].strip() == "foo,bar"

    # Setup without required cols
    setup()
    csv_file = CsvFile(path_to_new_file)
    csv_file.append([{"id": "foo2", "label": "bar2"}])
    csv_file.save()
    with open(path_to_new_file) as f:
        lines = f.readlines()
        assert lines[0].strip() == "id,label"
        assert lines[1].strip() == "foo2,bar2"

    # Empty multiindex file
    setup()
    csv_file = CsvFile(path_to_new_file, index_col=["id1","id2"],
        required_cols=["label"])
    csv_file.save()
    with open(path_to_new_file) as f:
        lines = f.readlines()
        assert lines[0].strip() == "id1,id2,label"

    # Clean up
    setup()


