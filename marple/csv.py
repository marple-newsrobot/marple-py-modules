# encoding: utf-8

import pandas as pd 
import os


class CsvFile(object):
    """ Base class for parsing csv files such as dimensions.csv and datasets.csv
        Key feature is that it will treat one or many given column(s) as an index
    """

    # Name of index column in the csv file
    index_col = "id" 

    # List of required columns, used for validation
    required_cols = None 

    def __init__(self, file_path, index_col=index_col,
        required_cols=required_cols):
        """ 
            :param file_path: path to file
            :param index_col (str|list): name of index column(s)
            :param required_cols (list): validate that these cols are in csv 
        """
        self.file_path = file_path
        self._index_col = index_col

        # Init
        # Create file if it doesn't exist, create if
        if not os.path.isfile(file_path):
            open(file_path, 'w').close()

            # Add columns
            if isinstance(index_col, list):
                cols = list(set(required_cols) - set(index_col))
            else:
                try:
                    cols = list(set(required_cols) - set([index_col]))
                except TypeError:
                    # When there are no requied cols
                    cols = None

            if isinstance(index_col, list):
                # Empty multi index
                index = pd.MultiIndex(
                    levels=[[] for x in index_col],
                    labels=[[] for x in index_col],
                    names=index_col)
                self.data = pd.DataFrame(columns=cols, index=index)
            else:
                # Empyt single index
                self.data = pd.DataFrame(columns=cols)
                self.data.index.name = index_col
                
        
            # Write the column names to file
            self.save()
        else:
            self.data = self._parse_input(file_path)
        
        self.validate()
    

    def row(self, row_index):
        """ Select a row in the csv file.
            If multi index `row_index` should be a list

        """
        try:
            if isinstance(self._index_col, list):
                if not isinstance(row_index, list):
                    msg = (u"This is a multiindex csv file. Must be queried"
                            u"with list. Got '{}'.").format(row_index)
                    raise KeyError(msg.encode("utf-8"))

                if len(row_index) != len(self._index_col):
                    msg = (
                        u"Length of multiindex ({}) and query ({})"
                        u" don't match.")\
                        .format(len(self._index_col), len(row_index))
                    raise KeyError(msg.encode("utf-8"))
                row = self.data.loc[tuple(row_index),:]
                index_as_dict = dict(zip(self._index_col, row.name))
            else:
                row = self.data.loc[row_index,:]
                
                if not isinstance(row, pd.DataFrame):
                    index_as_dict = { self._index_col: row.name }

        except KeyError:
            msg = u"'{}' missing in index column ('{}') in {}"
            msg = msg.format(row_index, self._index_col, self.file_path)
            raise KeyError(msg.encode("utf-8"))            
        

        if isinstance(row, pd.DataFrame):
            msg = u"There are multiple rows with index '{}' in {}. Index column is '{}'."
            msg = msg.format(row_index, self._index_col, self.file_path)
            raise KeyError(msg.encode("utf-8"))


        row_as_dict = row.to_dict()
        row_as_dict.update(index_as_dict)

        return row_as_dict

    def column(self, col_name):
        """ Returns the values of a column as dict with index value as key
        """
        return self.data.loc[:,col_name].to_dict()

    def validate(self):
        """ Perform validations
        """
        valid_cols = self._validate_cols()
        unique_index = self._validate_index_uniqueness()

        return valid_cols and unique_index 

    def append(self, file_or_data):
        """ Add rows to the csv file using pandas .append
        """
        df = self.data
        df_to_append = self._parse_input(file_or_data)

        df = df.append(df_to_append)
        
        # Drop possible duplicates
        self.data = df[~df.index.duplicated(keep='first')]

        return self.data

    def to_dictlist(self):
        """ Returns a dict list representation of table 
        """
        return self.data.reset_index().T.to_dict().values()

    def _parse_input(self, file_or_data):
        """ Parse data from file path or dictlist

            :returns: A pandas dataframe
        """
        if isinstance(file_or_data, str) or isinstance(file_or_data, unicode):
            # Is filepath
            df = pd.read_csv(file_or_data, encoding="utf-8", dtype=object)\
                    

        elif isinstance(file_or_data, list) and isinstance(file_or_data[0], dict):
            # Is dictlist
            df = pd.DataFrame(file_or_data)
        else:
            raise ValueError(u"Unrecoginzed input data: {}".format(file_or_data))

        df = df.set_index(self._index_col)
        return df

    def save(self, file_path=None):
        """ Save file
        """
        if not file_path:
            file_path = self.file_path

        self.data.to_csv(file_path, encoding="utf-8")

    def _validate_cols(self):
        """ Make sure that all required cols exist in csv file.
            Raise error if not.
        """
        if not self.required_cols:
            return True

        for col in self.required_cols:
            if col not in self.data.reset_index().columns:
                msg = u"Required column '{}' is missing in {}."
                msg = msg.format(col, self.file_path)
                raise ValueError(msg.encode("utf-8"))

        return True

    def _validate_index_uniqueness(self):
        """ Make sure there are no duplicate indecies
        """
        index_dups = self.data[self.data.index.duplicated()]
        if len(index_dups) > 0:
            msg = u"{} contains duplicate indecies: '{}'"\
                .format(self.file_path, index_dups.index.tolist())

            raise Exception(msg.encode("utf-8"))
        return True

class DatasetCsv(CsvFile):
    """ For datasets.csv files used in /schemas
    """
    index_col = ["id", "measure"]
    required_cols = ["id", "label", "measure"]

    def __init__(self, file_path, index_col=index_col, 
        required_cols=required_cols):
        super(DatasetCsv, self).__init__(file_path, index_col=index_col,
            required_cols=required_cols)


class DimensionsCsv(CsvFile):
    """ For dimensions.csv files used in /schemas
    """
    index_col = "id"
    required_cols = ["id","datatype","label"]

    def __init__(self, file_path, index_col=index_col, 
        required_cols=required_cols):
        super(DimensionsCsv, self).__init__(file_path, index_col=index_col,
            required_cols=required_cols)



