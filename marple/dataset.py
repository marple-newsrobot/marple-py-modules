# encoding: utf-8
import json
import itertools
import operator
from copy import deepcopy
import pandas as pd
from jsonschema import Draft4Validator, FormatChecker

class meta_property(property):
    """
    We use this decorator class to define what properties should be preserved
    when datasets are filtered, extended or modified in some other way.
    Used for labels, source, extension etc.
    """
    pass


class JSONStatObject(object):
    """ Represents a JSON Stat object
    """

    def _schema_validation(self, schema_path, json_data):
        """
        Validates some json data against a json schema. Raises exception 
        on error.
        
        :param schema_path: Path to schema file
        :type schema_path: str
        :param json_data: The data to be validated
        :type json_data: dict
        :raises: `jsonschema.exceptions.ValidationError`
        """
        with open(schema_path) as f:
            schema = json.load(f)
            validator = Draft4Validator(schema, format_checker=FormatChecker())
            validator.validate(json_data)

    def _get_decorated_attributes(self, decorator_class):
        """ 
        Get a list of names of all attributes decorated with a certain 
        decorator.

        :param decorator_class: The decorator class
        :type decorator_class: class
        :returns: A list of attribute names.
        """
        _class = type(self)
        return [name for name, value in vars(_class).items()
                if isinstance(value, decorator_class)]

    def _apply_meta_data(self, obj):
        """
        Take metadata from another jsonstat object (or dict) and
        apply it to self.
        Metadata is defined as properties decorated with `@meta_property`. 
        
        :param obj: Another jsonstat object (or dict)
        :returns: self
        """
        if isinstance(obj, JSONStatObject):
            meta_data = {}
            for attr in obj._get_decorated_attributes(meta_property):
                meta_data[attr] = getattr(obj, attr)
                
        elif isinstance(obj, dict):
            meta_data = obj
        else:
            raise ValueError("'obj' must be dict or JSONStatObject instance")

        for attr, value in meta_data.iteritems():
            setattr(self, attr, value)

        return self

class Dataset(JSONStatObject):
    """ Represents a Dataset object

    ..todo:: Handle these concepts:
        - href
        - role
        - link
        - note
        - error
    """
    def __init__(self, *args):
        """ 
        A dataset can be initiated with:
        - A file path (to json file)
        - A json dict
        - A json string
        - A Pandas dataframe
        
        The data can either be passed directly upon creation:
        dataset = Dataset("path/to/jsonstat.json")

        Or explicitly after:
        dataset = Dataset().from_file("path/to/jsonstat.json")

        """
        self._json_data = None
        # Schema used for validation
        self._schema_path = "marple/schemas/jsonstat_dataset_schema.json"

        if args:
            # Determine data type
            data = args[0]

            if isinstance(data, dict):
                # Init with json data
                self.from_json(data)
            elif isinstance(data, pd.DataFrame):
                # Init with dataframe
                self.from_dataframe(data)
            elif isinstance(data, str):
                try:
                    # Init with json string
                    json_data = json.loads(data)
                    self.from_json(json_data)
                except ValueError:
                    # Init from json file
                    self.from_file(data)
            else:
                msg = u"Unable to init from {}".format(data)
                raise ValueError(msg)

    # ========================
    #   INITIALIZATION METHODS
    # ========================    
    def from_file(self, file_path):
        """ Parse from json file

        :param file_path: Path to json file.
        :type file_path: str
        :returns: Itself to chain calls
        """
        
        with open(file_path) as f:
            json_string = f.read()
            self.from_string(json_string)

        return self


    def from_string(self, json_string):
        """Parse a string containing a jsonstat and initialize this dataset

        :param json_string: String containing a jsonstat
        :type json_string: str
        :returns: Itself to chain calls
        """
        json_data = json.loads(json_string)
        self.from_json(json_data)
        
        return self

    def from_dataframe(self, df, value_column="value", status_column="status"):
        """ 
        Parse a Pandas dataframe to a json stat object. Note that the created
        dataset won't have any labels, roles, units etc
            
        :param df: Data frame with only the columns to be included 
        :type df: pandas.Dataframe
        :param value_column: name of value column
        :type value_column: str
        :param status_column: name of status column
        :type status_column: str
        :returns: Itself to chain calls
        """
        if value_column not in df.columns:
            msg = u"there is no value column named {} in dataframe".format(value_column)
            raise KeyError(msg)

        dims = list(df.columns)
        has_status = status_column in dims
        dims.remove(value_column)
        if has_status:
            dims.remove(status_column)
        df = self._complete_missing(df, dims=dims)

        # Replace numpy NaN with None for correct json formating
        df = df.where((pd.notnull(df)), None)

        json_data = {}

        json_data["id"] = dims
        json_data["size"] = []
        json_data["dimension"] = {}
                
        for dim in dims:
            # Populate dimensions with index
            # Dimensions and categories won't get any labels
            json_data["dimension"][dim] = {
                "label": dim,
                "category": {},
            }
            dim_values = list(df[dim].unique())

            # Hackish! This might be a source of error in the future
            # But formating dimension values as strings comes natural 
            # when we are exporting to json files. 
            # Introduced to handle boolean values.
            dim_values = [unicode(x) for x in dim_values ]

            size = len(dim_values)
            
            index = dict(zip(dim_values, range(0,size)))

            json_data["dimension"][dim]["category"]["index"] = index
            
            # Populate size
            json_data["size"].append(size)

        # Populate value
        json_data["value"] = list(df[value_column])

        if has_status:
            status = list(df[status_column])
            # Replace None with ""
            # null/None not allowed as status value
            status = ["" if x == None else x for x in status]
            json_data["status"] = status

        self.from_json(json_data)

        return self

    def from_json(self, json_data):
        """Parse a json structure and initialize this dataset

        :param json_data: json structure
        :dict json_data: dict
        :returns: Itself to chain calls
        """
        if "class" not in json_data:
            json_data["class"] = "dataset"

        if "version" not in json_data:
            json_data["version"] = "2.0"

        self._json_data = json_data

        self._validate(json_data)

        return self

    # ========================
    #   PUBLIC ATTRIBUTES
    # ========================    
    @property
    def json(self):
        """ 
        :returns: A json representation of the dataset as dict.
        """
        return self._json_data

    @meta_property
    def source(self):
        """
        :returns: The source property of the json stat object as str.
        """
        try:
            return self.json["source"]
        except KeyError:
            return None

    @source.setter
    def source(self, value):
        """ Set value of source
        """
        self.json["source"] = value

    @meta_property
    def label(self):
        """
        :returns: The label property of the json stat object as str.
        """
        try:
            return self.json["label"]
        except KeyError:
            return None

    @label.setter
    def label(self, value):
        """ Set value of label
        """
        self.json["label"] = value

    @meta_property
    def extension(self):
        """
        Convenience attribute. 
        :returns: The extension property of the json stat object as dict.
        """
        try:
            return self.json["extension"]
        except KeyError:
            return None

    @extension.setter
    def extension(self, value):
        """ Set value of extension
        """
        self.json["extension"] = value

    @meta_property
    def updated(self):
        """
        :returns: The updated property of the json stat object as str.
        """
        try:
            return self.json["updated"]
        except KeyError:
            return None

    @updated.setter
    def updated(self, value):
        """ Set value of updated
        """
        self.json["label"] = value


    @property
    def dimensions(self):
        """
        :returns: A list of all dimensions as Dimension instances 
        """
        return [self.dimension(dim_id) for dim_id in self.json["id"]]
    
    @property
    def length(self):
        """
        Get total number of values (based on size property)
        :rtype: int
        """
        return reduce(lambda x, y: x * y, self.json["size"])

    @property
    def value_list(self):
        """
        Get a list of values. Turns dict representation to list.
        :returns: A list of values
        """
        values = self.json["value"]
        if isinstance(values, list):
            return values
        else:
            _values = [ None for x in range(0, self.length) ]
            for pos, value in values:
                try:
                    _values[pos] = value
                except:
                    msg = "Error in value property. Index {} is out of range."\
                        .format(pos)
                    raise MalformedJSONStat(msg)
            return _values

    @property
    def status_list(self):
        """
        Get a list of status values. Turns dict representation to list.
        :returns: A list of statuses
        """
        _statuses = [ "" for x in range(0, self.length) ]
        
        if "status" not in self.json:
            return _statuses

        if isinstance(self.json["status"], list):
            return self.json["status"]
        else:
            for pos, status in self.json["status"].iteritems():
                try:
                    _statuses[pos] = status
                except:
                    msg = "Error in status property. Index {} is out of range."\
                        .format(pos)
                    raise MalformedJSONStat(msg)
            return _statuses


    
    # ========================
    #   PUBLIC METHODS
    # ======================== 
    def dimension(self, dim_id):
        """
        Get dimension by id
        
        :param dim_id: Id of dimension
        :type dim_id: str
        :returns:
        """
        try:
            dim_json = self.json["dimension"][dim_id]
            return Dimension(dim_id, dim_json)
        except KeyError:
            msg = u"No dimension with id '{}'.".format(dim_id)
            raise KeyError(msg)

    # ========================
    #   PUBLIC METHOS: Export
    # ========================     
    def to_dataframe(self, content="label", value_column="value", 
        status_column="status", include_status=True):
        """
        Transforms the dataset to a pandas dataframe.

        :param content: Can be "label" or "id". If labels are not defined index 
            will be used instead.
        :param value_column: name of value column
        :type value_column: str
        :param status_column: name of status column
        :type status_column: str
        :param include_status: should the data frame inlude a status column?
        :type include_status: bool
        :returns: a list of rows, first line is the header, every row is tuple
        """
        table = self.to_table(content=content, value_column=value_column, 
            status_column="status", include_status=include_status)
        df = pd.DataFrame(table[1:], columns=table[0])
        return df


    def to_table(self, content="label", value_column="value", status_column="status",
        include_status=True):
        """
        Transforms a dataset into a table (a list of rows as tuple)
        Like so:
        [
            ('region', u'gender', 'value'),
            ('Stockholm', 'Male', 1),
            ('Stockholm', 'Female', 2),
            ('Solna', 'Male', 3),
            ('Solna', 'Female', 4)
        ]

        :param content: Can be "label" or "id". If labels are not defined index 
            will be used instead.
        :param value_column: name of value column
        :type value_column: str
        :param status_column: name of status column
        :type status_column: str
        :param include_status: should the data frame inlude a status column?
        :type include_status: bool
        :returns: a list of rows, first line is the header, every row is tuple
        """
        table = []

        # header
        if content == "label":
            header = [dim.label for dim in self.dimensions]
        else:
            header = [dim.id for dim in self.dimensions]

        header.append(value_column)
        if include_status:
            header.append(status_column)

        # Get id's/labels for all dimensions
        all_categories = []
        for dim in self.dimensions:
            dim_categories = []
            for cat in dim.categories:
                if content == "label":
                    dim_categories.append(cat.label)
                else:
                    dim_categories.append(cat.id)
            all_categories.append(dim_categories)

        # Generate a list of all id/label combinations
        combinations = list(itertools.product(*all_categories))

        # Add values
        values = [(x,) for x in self.value_list]

        if include_status:
            statuses = [(x,) for x in self.status_list]
            table = zip(combinations, values, statuses)
        else:
            table = zip(combinations, values)

        # Flatten
        table = [sum(row, ()) for row in table]

        # Add header
        table = [tuple(header)] + table

        return table

    # ========================
    #   PUBLIC METHOS: Modification
    #   These methods are hackish solutions to modifying datasets
    #   Ideally JSON Stat objects should just be a representation of data.
    #   Modifications should happen elsewhere
    # ========================     

    def filter(self, filter_fn, content="index"):
        """
        Transform the dataset to dataframe and apply a filter.
        :param filter: A function to filter by, e.g. lambda x: x['gender'] == 'M'
        :returns: self
        """
        df = self.to_dataframe(content=content)
        filtered_df = df[df.apply(filter_fn, axis=1)]

        self._rebuild(filtered_df)

        return self

    def filter_by_query(self, query, content="index"):
        """ Simple filtering of a dataset. Get all rows that equals a given value.
            For example dataset.filter_by_query({"region": "Stockholms kommun"})

            :param query: dimension id as key, value as value e.g. { "gender": "M" }
            :type query: dict
            :returns: Self 
        """
        def filter_fn(x):
            for dim_id, value in query.iteritems():
                if x[dim_id] != value:
                    return False
            return True

        return self.filter(filter_fn, content="index")

    def append(self, dataset_to_append):
        """ 
        Append another dataset. Metadata from original dataset
        will override metadata from appended dataset (e.g. labels).

        :dataset_to_append: A dataset to append (Dataset)
        :returns: self
        """
        ds1 = self
        ds2 = dataset_to_append

        # Make sure that dimensions are the same in both datasets
        dims1 = [x.id for x in ds1.dimensions]
        dims2 = [x.id for x in ds2.dimensions]
        dims = dims1
        if set(dims1) != set(dims2):
            msg = "Can't merge datasets. Unidentical dimensions. {} in original dataset, {} in appended dataset."
            msg = msg.format(dims1, dims2)
            raise MergeFailure(msg)


        df1 = ds1.to_dataframe(content="index").reset_index()
        df2 = ds2.to_dataframe(content="index").reset_index()

        df = pd.concat([df1, df2]).drop_duplicates()

        df = df.drop('index', 1)

        # Make sure that there are still no duplicates.
        # .drop_duplicates() should remove those, but if
        # value differs on otherwise identical dims
        # this error should be helpful
        duplicates = df.reset_index().set_index(dims).index.get_duplicates()
        if len(duplicates) > 0:
            raise MergeFailure("Failed to merge datasets. Duplicates rows found.")
        
        
        self._rebuild(df)

        for dim in self.dimensions:
            dim2 = ds2.dimension(dim.id)
            dim._apply_meta_data(dim2)

        return self

    def add_labels(self, dim_id, labels):
        """
        Add a labels to categories of a given dimension.

        :param dim_id: id of dimension
        :type dim_id: str
        :param labels: A dictionary with category id's as keys and labels as values.
            E.g. { "M": "Male", "F": "Female"}
        :type labels: dict
        """
        self.dimension(dim_id).labels = labels


    # ========================
    #     INTERNAL METHODS
    # ========================
    def _validate(self, json_data):
        """
        Validate that this is a correctly formated jsonstat dataset. Raises
        error if validation fails.
        
        :param json_data: A json stat object
        :type json_data: dict
        :returns: `None`
        :raises: MalformedJSONStat, `jsonschema.exceptions.ValidationError`

        """

        # 1. Validate against basic json schema
        self._schema_validation(self._schema_path, json_data)

        # 2. Make sure that all id's are in dimension and vice versa
        for dim_id in self.json["id"]:
            if dim_id not in self.json["dimension"].keys():
                msg = u"'{}' missing under the dimension property.".format(dim_id)
                raise MalformedJSONStat(msg)

        for dim_id in self.json["dimension"].keys():
            if dim_id not in self.json["id"]:
                msg = u"'{}' missing in id property.".format(dim_id)
                raise MalformedJSONStat(msg)

        # 3. Make sure that size and id have same length
        if len(self.json["size"]) != len(self.json["id"]):
            msg = "'size' and 'id' must have same length. Now {} and {}."\
                .format(len(self.json["size"]), len(self.json["id"]))
            raise MalformedJSONStat(msg)


        # 4. Make sure that size property counts dimension categories correctly
        for i, _size in enumerate(self.json["size"]):
            dim_id = self.json["id"][i]
            dim = self.dimension(dim_id)
            if _size != dim.length:
                msg = "'size' property does not match length of '{}'. Got {}, expected {}."\
                    .format(dim_id, _size, dim.length)
                raise MalformedJSONStat(msg)

        # 5. Make sure that the size factors and value length are identical
        if len(self.value_list) != self.length:
            msg = "size factors don't match length of values. Got {}, expected {}."\
                .format(size_total, len(self.json["value"]))
            raise MalformedJSONStat(msg)

        # 6. Make sure that status and value 
        # TODO:
        # Validate status against value

    def _rebuild(self, new_data):
        """
        Rebuild dataset from dataframe. Will preserve all properties
        decorated with @meta_property from original dataset.

        :param new_data: For now only supports dataframes.
        :type new_data: pd.DataFrame
        :returns: self
        """
        original_dataset = deepcopy(self)
        if isinstance(new_data, pd.DataFrame):
            self.from_dataframe(new_data)
        else:
            # TODO: Rebuild from other datatyps
            raise NotImplementedError()
        
        self._apply_meta_data(original_dataset)

        for dim in self.dimensions:
            original_dimension = original_dataset.dimension(dim.id)
            dim._apply_meta_data(original_dimension)

        
        return self

    def _complete_missing(self, df, dims=[]):
        """ 
        Completes a long dataframe with NaN for index combos that are missing
        Example:
        a,x,1
        b,y,2
        =>
        a,x,1
        a,y,NaN
        b,x,NaN
        c,y,2

        :param df: A pandas dataframe
        :param dims: List of dimensions to index by
        :returns: A pandas dataframe
        """
        values = [list(df[dim_id].unique()) for dim_id in dims]
        ix = pd.MultiIndex.from_product(values, names=dims)
        empty_df = pd.DataFrame(index=ix, columns=["empty"])
        df = df.set_index(dims)

        return pd.merge(df,empty_df,left_index=True, right_index=True, how='right')\
            .reset_index()\
            .drop('empty', 1)


class Dimension(JSONStatObject):
    """
    Represents a dimension in a Dataset
    
    ..todo:: Handle these concepts:
        - child
        - coordinates
        - unit
        - decimals
        - symbol
        - position
        - extension

    """
    def __init__(self, dim_id, dim_json):
        """
        :param dim_id: Id of dimension
        :type dim_id: str
        :param dim_json: Json data of dimension.
        :type dim_json: dict
        """
        self._id = dim_id
        self._label = None
        self._json_data = dim_json
        self._schema_path = "marple_py/schemas/jsonstat_dimension_schema.json"

        self._categories = None
        

    @property
    def json(self):
        """
        :returns: The json representation of the dimension (as dict)
        """
        return self._json_data
    

    @property
    def id(self):
        """ 
        :returns: The id of the dimension
        """
        return self._id

    @meta_property
    def label(self):
        """
        :returns: The label of the dimension (if any), otherwise id
        """
        try:
            return self.json["label"]
        except KeyError:
            return self.id

    @label.setter
    def label(self, value):
        self.json["label"] = value
    
    @property
    def length(self):
        """
        :returns: The number of categories in index
        """
        return len(self.categories)

    @property
    def categories(self):
        """ 
        :returns: A list of category ids for this dimension, sorted by position.
        """

        if self._categories == None:
            category_json = self.json["category"]

            if "index" not in category_json:
                cat_id = category_json["label"].keys()[0]
                categories = [(cat_id, 0)]
            else:
                index = category_json["index"]
                if isinstance(index, list):
                    categories = [(cat_id, i) for i,cat_id in enumerate(index)]
                elif isinstance(index,dict):
                    categories = [ (cat_id, i) for (cat_id, i) in index.iteritems()]

            categories.sort(key=lambda tup: tup[1])
            
            self._categories = [ Category(cat_id, pos, category_json)
                for (cat_id, pos) in categories ]

        return self._categories

    def category(self, id_or_label):
        """
        Get a category by label or id

        :param id_or_label: Id or label of category
        :type id_or_label: str
        :returns: The category
        :rtype: Category 
        """
        for category in self.categories:
            if (category.id == id_or_label) or (category.label == id_or_label):
                return category

        msg = u"No category with id or label '{}'.".format(id_or_label)
        raise KeyError(msg)

    
    @meta_property
    def labels(self):
        """ 
        :returns: the category labels of this dataset (if any)  
        """
        try:
            return self.json["category"]["label"]
        except KeyError:
            return {}

    @labels.setter
    def labels(self, labels):
        """
        Add category labels.

        :param labels: A dictionary with category ids as keys and labels as values.
            E.g. { "M": "Male", "F": "Female"}
        :type labels: dict
        """
        if "label" not in self.json["category"]:
            self.json["category"]["label"] = {}
        
        for cat in self.categories:
            if cat.id in labels:
                self.json["category"]["label"][cat.id] = labels[cat.id]



class Category(JSONStatObject):
    def __init__(self, cat_id, pos, cat_json):
        self.id = cat_id
        self.pos = pos
        self._json = cat_json

    @property
    def json(self):
        return self._json

    @property
    def label(self):
        """
        :returns: category label, if any. Otherwise id.
        """
        try:
            return self.json["label"][self.id]
        except KeyError:
            return self.id

    
    


  

class MalformedJSONStat(Exception):
    pass    

class MergeFailure(Exception):
    pass