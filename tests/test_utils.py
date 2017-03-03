# encoding: utf-8
from marple.utils import list_files, guess_periodicity, to_timepoint

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

def test_to_timepoint():
    assert to_timepoint("2015") == "2015-01-01"
    assert to_timepoint(2015) == "2015-01-01"
    assert to_timepoint("2015-03") == "2015-03-01"
