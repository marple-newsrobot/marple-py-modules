# coding: utf-8
""" Methods for parsing datatype data from
    https://github.com/marple-newsrobot/marple-datatypes """
from glob import glob
import pandas as pd
from os.path import dirname, realpath, basename, splitext
from os import sep as os_sep


class Domain(object):
    """Represents datatype data for a domain, e.g. gender or regions
       Usage:
           regions = Domain("regions/*")
           municipalities = Domain("regions/municipalities")
    """

    data = None
    files = []
    datatypes_dir = "../marple-datatypes"

    def __init__(self, domain, datatypes_dir=datatypes_dir):
        """
        :param domain: For example "regions/*" or "regions/municipalities"
        :param datatypes_dir: Path to base folder of datatypes. The default 
        value should work if marple-datatypes is located in the same parent directory.
        """
        datatype_path = os_sep.join([datatypes_dir,
                                     "%s.csv" % domain])
        self.files = glob(datatype_path)
        data = pd.DataFrame()
        for file_ in self.files:
            frame = pd.read_csv(file_)  # , names=columns
            frame['_category'] = splitext(basename(file_))[0]
            data = data.append(frame)  # , ignore_index=True
        self.data = data

    def category(self, id_):
        """ Category (defined as name of csv file) """
        row = self.row(id_)
        return row["_category"].decode("utf-8")

    def row(self, id_):
        """ Get a row by id """
        row = self.data\
            .loc[self.data[u'id'] == id_.encode("utf-8")]\
            .squeeze()\
            .dropna()\
            .to_dict()
        return row

    def parent(self, id_):
        """ Get the id of the parent, if there is one """
        row = self.row(id_)
        try:
            return row["parent"].decode("utf-8")
        except KeyError:
            return None

    def children(self, id_):
        """ Get id's of rows with this id as parent, if any
            TODO (?): specify depth (child of child)
        """
        return self.data.loc[self.data.parent == id_.encode("utf-8")]\
            .id.tolist()
