# coding: utf-8
""" Contains base classes for connecting to datasources
"""
import simplejson as json
import os
import requests
from collections import OrderedDict
from requests.auth import HTTPBasicAuth
from simplejson import JSONDecodeError
from bson import json_util
from marple.postgrest import Api 
from glob import glob
from os.path import basename


class Connection(object):
    """ Base class for connections. These are the methods that connections
        are expected to handle.
    """
    def __init__(self):
        pass

    def get(self, **kwargs):
        """ Get all object by a set of rules """
        raise NotImplementedError("This method must be overridden")

    def exists(self, **kwargs):
        """Check if any objects like this exists """
        raise NotImplementedError("This method must be overridden")

    def get_by_filename(self, filename):
        """ Get an object by file name """
        raise NotImplementedError("This method must be overridden")

    def get_by_id(self, id_):
        """ Get an object by its id """
        raise NotImplementedError("This method must be overridden")

    def store(self, filename, json_data, folder=None):
        """ Store the file """
        raise NotImplementedError("This method must be overridden")


class LocalConnection(Connection):
    """ When communicating with local storage """
    path = None

    def __init__(self, folder_path):
        super(LocalConnection, self).__init__()
        self.path = folder_path

    def exists(self, **kwargs):
        """Check if any objects like this exists """
        objects = self.get(**kwargs)
        # Check if more than 0
        if sum(1 for e in objects if e != None):
            return True
        else:
            return False

    def get_by_filename(self, filename):
        file_path = os.path.join(self.path, filename)
        if not os.path.isfile(file_path):
            # File is missing!
            return None

        with open(file_path) as json_file:
            json_data = json.load(json_file, encoding="utf-8")
        return json_data        

    def get_by_id(self, id_):
        return self.get_by_filename(id_ + ".json")

    def get(self, **kwargs):
        if "id" in kwargs:
            yield self.get_by_id(kwargs["id"])
        else:
            file_expr = self._get_path_expr_from_query(kwargs)
            for file_path in glob(os.path.join(self.path, file_expr)):
                filename = os.path.basename(file_path)
                yield self.get_by_filename(filename)

    def store(self, filename, json_data, folder=None):
        """ Store the file 
        :param filename: name of file or id. ".json" added if missing  
        :param json_data: json data to be stored
        :param folder: Path to output folder. Defaults to connection folder.
        """
        if folder == None:
            folder = self.path

        if filename[-5:] != ".json":
            filename += ".json"

        file_path = os.path.join(folder, filename)
        with open(file_path, 'w') as outfile:
            json.dump(json_data, outfile,
                      ignore_nan=True,
                      indent=4, sort_keys=True,
                      default=json_util.default)


    def _get_path_expr_from_query(self, query):
        """ Generate a path expression for `glob` to search for from a query.

        :param query: For example {"source": "AMS"}.
        :type query: str 
        :returns: A path expression to be passed to `glob()` 
        :rtype: str:
        """
        if not query:
            return "*"


class LocalDatasetConnection(LocalConnection):
    """ For getting and storing datasets locally 
    """
    def _get_path_expr_from_query(self, query):
        """
        { "source": "AMS" } =>
        "ams-*-*-*-*.json"

        """
        fragments = OrderedDict((
            ("source", "*"),
            ("topic", "*"),
            ("periodicity", "*"),
            ("measure", "*"),
            ("name", "*"),
        ))
        for key, value in query.iteritems():
            if key in fragments:
                fragments[key] = query[key].lower()

        return u"-".join(fragments.values()) + ".json"

class LocalAlarmConnection(LocalConnection):
    """ For getting and storing alarms locally 
    """    
    def _get_path_expr_from_query(self, query):
        """
        { "region": "Stockholms kommun" } =>
        "*|*|Stockholms-kommun|*|*.json"

        """
        fragments = OrderedDict((
            ("dataset_id", "*"),
            ("trigger_date", "*"),
            ("region", "*"),
            ("alarm_type", "*"),
            ("hash", "*"),
        ))
        for key, value in query.iteritems():
            if key in fragments:
                fragments[key] = query[key].replace(" ", "-")

        return u"|".join(fragments.values()) + ".json"


class LocalNewsleadConnection(LocalAlarmConnection):
    """ For getting and storing alarms locally.
    Behaves just like the alarm connection 
    """    
    pass


class DatabaseConnection(Connection):
    """ A connection to the central database.
    """
    api = None
    model = None # dataset|alarm|newslead

    def __init__(self, api_url, model, jwt_token=None, db_role=None):
        self.api = Api(api_url)
        self.model = model
        self._jwt_token = jwt_token
        self._db_role = db_role

    def get(self, **kwargs):
        """ Get object by query

        :param kwargs: Query by any table column in database
        :returns (dict): A json object (or None if no match)
        """
        r = self.api.get(self.model)\
            .select("json_data")\
            .match(kwargs)\
            .jwt_auth(self._jwt_token, { "role": self._db_role})\
            .request()

        if r.status_code != 200:
            raise RequestException("{}: {}".format(r.status_code, r.reason), r)            
        else:
            # Only return the actual json objects
            try:
                data = [x["json_data"] for x in r.json()]
            except JSONDecodeError:
                data = None

        return data


    def get_by_id(self, id_):
        """ Get object by id

        :param id_ (str): Id of object
        :returns (dict): A json object (or None if no match)
        """
        r = self.api.get(self.model)\
            .single()\
            .eq("id", id_)\
            .jwt_auth(self._jwt_token, { "role": self._db_role })\
            .request()

        if r.status_code != 200:
            raise RequestException("{}: {}".format(r.status_code, r.reason), r)        
        else:
            # Only return the actual json objects
            try:
                data = r.json()["json_data"]
            except JSONDecodeError:
                data = None

        return data

    def exists(self, **kwargs):
        """ Check if object exists
        """
        # Only select the id column to reduce traffic
        r = self.api.get(self.model)\
            .select("id")\
            .match(kwargs)\
            .request()

        if r.status_code == 200:
            try:
                return len(r.json()) > 0
            except JSONDecodeError:
                return False
        else:
            return False

    def store(self, filename, json_data, **kwargs):
        """ Insert, or if object already exist, update.
        
        :param filename (str): File name (which should be same as id)
        :param json_data (dict): The json data to be stored.
        :returns (Requests.Response): A response instance from the Request module. 
        """
        id = filename.replace(".json","")

        # Try insert
        r = self.api.post(self.model)\
            .jwt_auth(self._jwt_token, { "role": self._db_role })\
            .json(json_data)\
            .request()

        # Newslead already exist => update
        if r.status_code == 409:
            r = self.api.patch(self.model)\
                .jwt_auth(self._jwt_token, { "role": self._db_role })\
                .json(json_data)\
                .eq("id", id)\
                .request()  

        return r

class DatabaseSchemaConnection(Connection):
    def __init__(self, api_url):
        self.base_url = api_url

    def get(self):
        """ List all schemas
        """
        r = requests.get(self.base_url + "/schemas")
        if r.status_code != 200:
            raise RequestException("{}: {}".format(r.status_code, r.reason), r)        

        return r.json()

    def get_by_id(self, id_):
        """ Get a schema by id
        """
        r = requests.get(self.base_url + "/schema/" + id_)
        if r.status_code != 200:
            raise RequestException("{}: {}".format(r.status_code, r.reason), r)        

        return r.json()

class RequestException(Exception):
    """ Custom exception for request errors. Makes the
        resonse instance availble in the raised exception 
        for debugging.

        :param message (str): A regular error message.
        :param resp (Requests.Response): The response instance where the error
            occured.
    """
    def __init__(self, message, resp):
        super(RequestException, self).__init__(message)

        # Store 
        self.resp = resp


