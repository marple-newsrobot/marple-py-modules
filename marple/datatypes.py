# coding: utf-8
""" Methods for parsing datatype data from
    https://github.com/marple-newsrobot/marple-datatypes """
from glob import glob
import pandas as pd
from os.path import dirname, realpath, basename, splitext
from os import sep as os_sep
from marple.csv import CsvFileWithLabel
from marple.utils import isNaN

class Domain(CsvFileWithLabel):
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
        self.domain_name = domain
        datatype_path = os_sep.join([datatypes_dir,
                                     "%s.csv" % domain])
        self.files = glob(datatype_path)
        data = pd.DataFrame(columns=["id", "label"])
        for file_ in self.files:
            df = pd.read_csv(file_, encoding="utf-8", dtype=object)  # , names=columns
            df['_category'] = splitext(basename(file_))[0]
            df['_file'] = file_
            data = data.append(df)  # , ignore_index=True
        self.data = data.set_index("id")

    def category(self, id_):
        """ Category (defined as name of csv file) """
        row = self.row(id_)
        return row["_category"]

    def row(self, id_):
        """ Get a row by id """
        try:
            row = self.data.loc[id_]
        except KeyError:
            msg = u"{} is missing under the domain '{}'".format(id_, self.domain_name)
            raise KeyError(msg)

        if isinstance(row, pd.DataFrame):
            if len(row) > 1:
                raise Exception(u"Multiple rows with id '{}' in this domain."\
                    .format(id_))

        elif isinstance(row, pd.Series):
            return row.to_dict()

        else:
            raise Exception("Unexpected error. Please debug.")


    def parent(self, id_):
        """ Get the id of the parent, if there is one """
        row = self.row(id_)
        try:
            parent = row["parent"]
            if isNaN(parent):
                return None
        except KeyError:
            return None

        return parent

    def children(self, id_):
        """ Get id's of rows with this id as parent, if any
            TODO (?): specify depth (child of child)
        """
        if "parent" in self.data.columns:
            children =  self.data.loc[self.data.parent == id_]\
                .index.tolist()
            # Unicode
            return [x for x in children]
        else:
            return []

    def labels(self, lang=None):
        """ Get labels for the domain
        
            :param lang: language of labels, typically "sv" or "en"
            :returns: a dict with index as key and label as value
        """
        labels = {}
        for id_, row in self.data.iterrows():
            label = self.label(id_, lang=lang)
            labels[id_] = label 

        return labels



