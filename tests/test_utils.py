# encoding: utf-8

from marple.utils import list_files

def test_list_files():
    all_files = list_files("tests/data/utils/list_files")
    txt_files1 = list_files("tests/data/utils/list_files","txt")
    txt_files2 = list_files("tests/data/utils/list_files",".txt")
    assert len(all_files) == 3
    assert len(txt_files1) == 2
    assert len(txt_files2) == 2