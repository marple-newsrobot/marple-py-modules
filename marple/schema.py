# encoding: utf-8:
import csvkit as csv
import os
import json
import pandas as pd
from copy import deepcopy
import re

from marple.datatypes import Domain

class Schema(object):
    def __init__(self,
                 dataset_id,
                 json_schema_connection,
                 base_dir="../marple-datasets/schemas",
                 datatypes_dir="../marple-datatypes"):
        """
            :param dataset_id (str): id of dataset
            :param json_schema_connection: a connection instance for
            :type json_schema_connection: DatabaseSchemaConnection
            :param base_dir: path to directory with schemas
            :param datatypes_dir (str): path to datatype definitions repo
            getting json schemas for validation
        """
        # Get labele etc
        # Also validates the correctness of id
        self._id = dataset_id
        self._base_dir = base_dir
        self.datatypes_dir = datatypes_dir
        self.json_schema_connection = json_schema_connection

        self.data = self._parse_dataset_csv(dataset_id)
        self._dimensions = self._get_dimensions(dataset_id)
        self._metadata = self._get_metadata(dataset_id)

    def __repr__(self):
        return u"<Schema: {}>".format(self._id)        

    @property
    def id(self):
        return self._id

    @property
    def label(self):
        try:
            return self.data["label"]
        except KeyError:
            return None

    @property
    def description(self):
        try:
            return self.data["description"]
        except KeyError:
            return None
    

    @property
    def source(self):
        try:
            return self._metadata["source"]
        except KeyError:
            return None

    @property
    def topic(self):
        try:
            return self._metadata["topic"]["id"]
        except KeyError:
            return None

    @property
    def dimensions(self):
        """ List dimensions
        :returns: a list of all dimensions for this schema
        """
        return self._dimensions.values()

    @property
    def metadata(self):
        """ Metadata to be used and stored in jsonstat["extension"]
        """
        return self._metadata
    

    @property
    def as_json_schema(self):
        return self._to_json_schema()

    def dimension(self, dim_id):
        """ Get dimension by id
        :param dim_id: Id of dimension
        :returns: A dimension instance
        :rtype: Dimension
        """
        try:
            return self._dimensions[dim_id]
        except KeyError:
            msg = u"No dimension with id '{}'".format(dim_id)
            raise KeyError(msg)

    def _parse_dataset_csv(self, dataset_id):
        """ Get label and description from dataset.csv
        """
        id_parts = dataset_id.split("-")
        dir_path = os.path.join(self._base_dir, "/".join(id_parts[:-1]))
        dataset_name = id_parts[-1]
        # Make sure that the folder exists
        if not os.path.exists(dir_path):
            msg = u"{} does not exist in schema directory"
            raise ValueError(msg.format(dir_path).encode('utf-8'))

        # Make sure there is a datasets.csv file in the dir
        datasets_csv_path = os.path.join(dir_path, "datasets.csv")
        if not os.path.exists(datasets_csv_path):
            msg = u"datasets.csv is missing in {}"
            raise ValueError(msg.format(dir_path).encode("utf-8"))

        # Make sure that the last part of the id is listed in the dataset
        return DatasetCsv(datasets_csv_path).row(dataset_name)
        

    def _parse_schema_csv(self, path):
        """ Reads a schema.csv file and return a dict with "dimension"
        """
        schema_csv = SchemaCsv(path)
        data = {}
        for dim_id, row in schema_csv.data.iterrows():
            datatypes = row["datatype"].split(",")
            dim_label = row["label"]
            data[dim_id] = Dimension(dim_id,
                                       dim_label,
                                       datatypes,
                                       datatypes_dir=self.datatypes_dir)
        return data

    def _parse_metadata_csv(self, path):
        """ :path (str): path to csv file
            :returns (dict): A dict with the value of the property column as key and 
                MetadataProperty instances as values. 
        """
        metadata_csv = MetadataCsv(path)
        data = {}
        for prop, row in metadata_csv.data.iterrows():
            try:
                datatypes = row["datatype"].split(",")
            except AttributeError:
                datatypes = None

            if prop == "id":
                raise ValueError("'id' is not allowed as a metadata property.")

            value = metadata_csv.row(prop)["value"]

            if "__" in prop:
                x = prop.split("__")
                if x[0] not in data:
                    data[x[0]] = {}
                data[x[0]][x[1]] = value
            else:
                data[prop] = value


        return data


    def _validate_schema_csv(self, path):
        """ Validations for a schema.csv file
        """
        with open(path) as f:
            reader = csv.DictReader(f)
            cols = reader.next().keys()
            if "dimension" not in cols:
                msg = u"'dimension'' column is missing in {}".format(col, path)
                raise ValueError(msg.encode("utf-8"))

            elif "datatype" not in cols:
                msg = u"'datatype' column is missing in {}".format(path)
                raise ValueError(msg.encode("utf-8"))


    def _get_dimensions(self, dataset_id):
        """ Parse schema.csv files to get all dimensions of dataset
            :param dataset_id: id of dataset, for example
                ams/unemployment/montly/count
            :returns (dict): dimensions as dict with dimensions
                as Dimension instances
        """
        folders = dataset_id.split("-")
        base_schema_csv = self._parse_schema_csv(self._base_dir +
                                                 "/schema.csv")
        for i, folder in enumerate(folders):
            dir_path = os.path.join(self._base_dir, "/".join(folders[:(i + 1)]))
            csv_path = dir_path + "/schema.csv"
            if os.path.exists(csv_path):
                schema_csv = self._parse_schema_csv(csv_path)
                base_schema_csv.update(schema_csv)

        # "value" is not treated as dimension
        base_schema_csv.pop("value", None)

        return base_schema_csv


    def _get_metadata(self, dataset_id):
        """ Parse metadata.csv files to get all dimensions of dataset
            :param dataset_id: id of dataset, for example
                ams/unemployment/montly/count
            :returns (dict): dimensions as dict with dimensions
                as Dimension instances
        """
        folders = dataset_id.split("-")
        try:
            base_metadata_csv = self._parse_metadata_csv(self._base_dir +
                                                 "/metadata.csv")
        except IOError:
            base_metadata_csv = {}

        for i, folder in enumerate(folders):
            dir_path = os.path.join(self._base_dir, "/".join(folders[:(i + 1)]))
            csv_path = dir_path + "/metadata.csv"
            if os.path.exists(csv_path):
                metadata_csv = self._parse_metadata_csv(csv_path)
                base_metadata_csv.update(metadata_csv)

        return base_metadata_csv


    def _to_json_schema(self):
        # Open schema base from data types repo
        base_schema = self.json_schema_connection.get_by_id("marple-dataset.json")["schema"]

        json_schema = deepcopy(base_schema)

        # Add schema for required dimensions and allowed values
        for dim in self.dimensions:
            if dim.id not in json_schema["properties"]["dimension"]["required"]:
                json_schema["properties"]["dimension"]["required"].append(dim.id)

            json_schema["properties"]["dimension"]["properties"][dim.id] = {
                "type": "object",
                "properties": {
                    "category": {
                        "type": "object",
                        "properties": {
                            "index": {
                                "type": "object",
                            },
                        }
                    }
                }
            }
            if len(dim.allowed_values) > 0:
                # Format allowed values as regex
                allowed_values_re = "^(%s)$" % "|".join(
                    [ re.escape(x) for x in dim.allowed_values])
                
                allowed_obj = {
                    "type": "object",
                    "additionalProperties": False,
                    "patternProperties": {}
                }
                allowed_obj["patternProperties"][
                    allowed_values_re] = {"type": "integer"}

                json_schema["properties"]["dimension"]["properties"][dim.id]\
                    ["properties"]["category"]["properties"]["index"] = allowed_obj


        if self.source:
            allowed_source_values = Datatype("source", self.datatypes_dir).allowed_values
            json_schema["properties"]["source"]["enum"] = allowed_source_values

        return json_schema


class CsvFile(object):
    """ Base class for parsing metadata.csv, schema.csv and datasets.csv
    """

    # Name of index column in the csv file
    index_col = "id" 

    # List of required columns, used for validation
    required_cols = None 

    def __init__(self, file_path, index_col=index_col, required_cols=required_cols):
        self.file_path = file_path
        self._index_col = index_col

        # Init
        self.data = pd.read_csv(file_path, encoding="utf-8", dtype=object)
        self.validate()
        self.data = self.data.set_index(index_col)
    

    def row(self, row_index):
        """ Returns the values of a row as dict.
        """
        row = self.data.loc[self.data.index==row_index,:]
        if len(row) == 0:
            msg = u"'{}' missing in index column ('{}') in {}"
            msg = msg.format(row_index, self._index_col, self.file_path)
            raise ValueError(msg.encode("utf-8"))
        elif len(row) > 1:
            msg = u"There are multiple rows with index '{}' in {}. Index column is '{}'."
            msg = msg.format(row_index, self._index_col, self.file_path)
            raise ValueError(msg.encode("utf-8"))

        return row.iloc[0].to_dict()

    def column(self, col_name):
        """ Returns the values of a column as dict with index value as key
        """
        return self.data.loc[:,col_name].to_dict()

    def validate(self):
        """ Perform validations
        """
        return self._validate_cols()

    def _validate_cols(self):
        """ Make sure that all required cols exist in csv file.
            Raise error if not.
        """
        if not self.required_cols:
            return True

        for col in self.required_cols:
            if col not in self.data.columns:
                msg = u"Required column '{}' is missing in {}."
                msg = msg.format(col, self.file_path)
                raise ValueError(msg.encode("utf-8"))

        return True

class DatasetCsv(CsvFile):
    index_col = "id"
    required_cols = ["id", "label"]

    def __init__(self, file_path, index_col=index_col, 
        required_cols=required_cols):
        super(DatasetCsv, self).__init__(file_path, index_col=index_col,
            required_cols=required_cols)

class MetadataCsv(CsvFile):
    index_col = "property"
    required_cols = ["property","value","datatype"]

    def __init__(self, file_path, index_col=index_col, 
        required_cols=required_cols):
        super(MetadataCsv, self).__init__(file_path, index_col=index_col,
            required_cols=required_cols)

class SchemaCsv(CsvFile):
    index_col = "dimension"
    required_cols = ["dimension","datatype","label"]

    def __init__(self, file_path, index_col=index_col, 
        required_cols=required_cols):
        super(SchemaCsv, self).__init__(file_path, index_col=index_col,
            required_cols=required_cols)


""" TODO: Refactor the code below. Not very intuitive..
"""
class Dimension(object):
    def __init__(self, dim_id, dim_label, datatypes, datatypes_dir="marple-datatypes"):
        """ Represents a dimension in the schema.

        :param dim_id (str): Id of dimension
        :param dim_label (str): Label
        :param datatypes (str|list):
            Name of permitted datatypes, for example "source".
            The datatype(s) must be defined in datatypes.csv.
        """    
        
        self._id = dim_id
        self._label = dim_label
        self._datatypes_dir = datatypes_dir

        if isinstance(datatypes, str):
            datatypes = datatypes.split(",")

        self._datatypes = datatypes
        self._labels = None

    def __repr__(self):
        return u"<Dimension: {}>".format(self.id)


    @property
    def id(self):
        """ dimension id, used as key in jsonstat["index"]
        """
        return self._id

    @property
    def label(self):
        """ Dimension label
        """
        return self._label

    @property
    def allowed_values(self):
        """ Returns a list of allowed values for the listed datatypes
        """
        _allowed_values = []
        for datatype_name in self._datatypes:
            datatype = Datatype(datatype_name, datatypes_dir=self._datatypes_dir)
            _allowed_values += datatype.allowed_values

        return _allowed_values

    @property
    def labels(self):
        """ Return a dict with labels.
        """

        if self._labels is None:
            labels = {}
            for datatype_name in self._datatypes:
                datatype = Datatype(datatype_name, datatypes_dir=self._datatypes_dir)
                labels.update(datatype.labels)

            self._labels = labels

        return self._labels
    


class Datatype(object):
    """ Represents a datatype in datatypes.csv
    """
    def __init__(self, name, datatypes_dir="marple-datatypes"):
        """ :param name (str): name of datatype as defined in datatypes.csv
            :param datatypes_dir: path to datatypes directory
        """
        self.datatypes_dir = datatypes_dir
        datatypes_csv_path = os.path.join(datatypes_dir, "datatypes.csv")
        self._id = name
        self.data = self._parse_datatypes_csv(name, datatypes_csv_path)
        self._domain = None


    @property
    def id(self):
        return self._id

    @property
    def domain(self):
        if not self._domain:
            if self.data["allowed_values"] == "":
                self._domain = None
            else:
                self._domain = Domain(
                    self.data["allowed_values"],
                    datatypes_dir=self.datatypes_dir,
                    )
                
        return self._domain    

    @property
    def allowed_values(self):
        """
        :retruns: a list of allowed values
        """
        if not self.domain:
            return []
        else:
            return [x.decode("utf-8") for x in self.domain.data["id"].astype('str').tolist()]
            

    @property
    def labels(self):
        if not self.domain:
            return {}
        else:
            try:
                label_df = self.domain.data.astype('str').set_index("id")["label"]
                label_df.index = label_df.index.map(lambda x: unicode(x, 'utf-8'))
                return label_df.to_dict()
            except KeyError:
                return {}

    
    def _parse_datatypes_csv(self, name, csv_path):
        """ Get data (columns) about the datatype from datatypes.csv
            :param name (str): name of datatype
            :param csv_path: path to datatypes.csv
            :returns (dict): the matching row as dict 
        """
        with open(csv_path) as f:
            reader = csv.DictReader(f)
            try:
                return [x for x in reader if x["id"] == name][0]
            except IndexError:
                msg = """{} is not a valid datatype. Check {} to correct."""\
                    .format(name, csv_path)
                raise ValueError(msg)

