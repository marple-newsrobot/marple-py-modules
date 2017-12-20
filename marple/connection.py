# coding: utf-8
"""Contains base classes for connecting to datasources
"""
import simplejson as json
import os
import requests
import requests_cache
from collections import OrderedDict
from requests.auth import HTTPBasicAuth
from simplejson import JSONDecodeError
from bson import json_util
from marple.postgrest import Api
from marple.dataset import Dataset
from marple.utils import cache_initiated
from glob import glob
from os.path import basename
import boto3 # S3 Client
from botocore.client import Config

class Connection(object):
    """Base class for connections. These are the methods that connections
        are expected to handle.
    """
    def __init__(self):
        pass

    def get(self, **kwargs):
        """Get all object by a set of rules """
        raise NotImplementedError("This method must be overridden")

    def exists(self, **kwargs):
        """Check if any objects like this exists """
        raise NotImplementedError("This method must be overridden")

    def get_by_filename(self, filename):
        """Get an object by file name """
        raise NotImplementedError("This method must be overridden")

    def get_by_id(self, id_, **kwargs):
        """Get an object by its id """
        raise NotImplementedError("This method must be overridden")

    def store(self, filename, json_data, folder=None):
        """Store the file """
        raise NotImplementedError("This method must be overridden")

    def delete(self, id_):
        """Delete an object by id """
        raise NotImplementedError("This method must be overridden")


class LocalConnection(Connection):
    """When communicating with local storage """
    path = None

    def __init__(self, folder_path):
        super(LocalConnection, self).__init__()
        self.path = folder_path
        self.type = "local"

    def exists(self, **kwargs):
        """Check if any objects like this exists """
        objects = self.get(**kwargs)
        if objects is None:
            return False
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

    def get_by_id(self, id_, **kwargs):
        return self.get_by_filename(id_ + ".json")

    def get(self, **kwargs):
        """Get all json files in folder based on query
        """
        if "id" in kwargs:
            return self.get_by_id(kwargs["id"])
        else:
            """TODO: Make it possible to query with lists.
                e.g. { "source": ["AMS", "SMS"] }
            """
            data = []
            for key, value in kwargs.iteritems():
                if isinstance(value, list):
                    msg = ("Making queries with lists is only possible on database " +
                        "connection at the moment.")
                    raise NotImplementedError(msg)

            file_expr = self._get_path_expr_from_query(kwargs)
            for file_path in glob(os.path.join(self.path, file_expr)):
                filename = os.path.basename(file_path)
                if filename[-5:] == ".json":
                    data.append(self.get_by_filename(filename))

            return data

    def store(self, filename, json_data, folder=None, on_existing="override"):
        """Store the file
        :param filename: name of file or id. ".json" added if missing
        :param json_data: json data to be stored
        :param folder: Path to output folder. Defaults to connection folder.
        :param on_existing: Currently only supporting "override"
        """
        if on_existing not in ["override"]:
            msg = u"{} not implemented yet for LocalConnection".format(on_existing)
            raise NotImplementedError(msg)

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
        """Generate a path expression for `glob` to search for from a query.

        :param query: For example {"source": "AMS"}.
        :type query: str
        :returns: A path expression to be passed to `glob()`
        :rtype: str:
        """
        # Return all files by default
        if isinstance(query, dict):
            if len(query.keys()) > 0:
                raise ValueError("This local connection does not accept queries.")
        return "*"


class LocalDatasetConnection(LocalConnection):
    """For getting and storing datasets locally
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
            else:
                msg = u"{} is not a valid key to get local files by."\
                    .format(key)
                raise ValueError(msg)

        return u"-".join(fragments.values()) + ".json"

class LocalAlarmConnection(LocalConnection):
    """For getting and storing alarms locally
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
            else:
                msg = u"{} is not a valid key to get local files by."\
                    .format(key)
                raise ValueError(msg)

        return u"|".join(fragments.values()) + ".json"


class LocalNewsleadConnection(LocalAlarmConnection):
    """For getting and storing alarms locally.
    Behaves just like the alarm connection
    """
    pass


class DatabaseConnection(Connection):
    """A connection to the central database.
    """

    def __init__(self, api_url, model, jwt_token="", db_role=""):
        """
        :param api_url: Url of marple-api
        :param model: Database table (ie. dataset, dataset_test, alarm...)
        :param jwt_token: JWT Token
        :param db_role: Database role for authentication
        """
        self.type = "database"
        self.api = Api(api_url)
        self.model = model
        self._jwt_token = jwt_token
        self._db_role = db_role

    def get(self, cache=False, **kwargs):
        """Get object by query

        :param cache: Cache request (in memory)
        :param kwargs: Query by any table column in database
        :returns (dict): A json object (or None if no match)
        """
        self.response = self.api.get(self.model)\
            .select("json_data")\
            .match(kwargs)\
            .jwt_auth(self._jwt_token, {"role": self._db_role})\
            .request(cache=cache)
        r = self.response

        if r.status_code != 200:
            raise RequestException("{}: {}".format(r.status_code, r.reason), r)
        else:
            # Only return the actual json objects
            try:
                data = [x["json_data"] for x in r.json()]
            except JSONDecodeError:
                data = None

        return data


    def get_by_id(self, id_, cache=False):
        """Get object by id

        :param id_ (str): Id of object
        :returns (dict): A json object (or None if no match)
        """
        self.response = self.api.get(self.model)\
            .single()\
            .eq("id", id_)\
            .jwt_auth(self._jwt_token, {"role": self._db_role})\
            .request(cache=cache)

        r = self.response

        if r.status_code != 200:
            raise RequestException("{}: {}".format(r.status_code, r.reason), r)
        else:
            # Only return the actual json objects
            try:
                data = r.json()["json_data"]
            except JSONDecodeError:
                data = None

        return data

    def exists(self, cache=False, **kwargs):
        """Check if object exists
        """
        # Only select the id column to reduce traffic
        self.response = self.api.get(self.model)\
            .select("id")\
            .match(kwargs)\
            .request(cache=cache)
        r = self.response

        if r.status_code == 200:
            try:
                return len(r.json()) > 0
            except JSONDecodeError:
                return False
        else:
            return False

    def store(self, filename, json_data, **kwargs):
        """Insert, or if object already exist, update.

        :param filename (str): File name (which should be same as id)
        :param json_data (dict): The json data to be stored.
        :returns (Requests.Response): A response instance from the Request module.
        """
        id = filename.replace(".json","")
        # Check if item exists

        if not self.exists(**{"id": id}):
            # ...if not => create new with POST
            r = self.api.post(self.model)\
                .jwt_auth(self._jwt_token, { "role": self._db_role })\
                .json(json_data)\
                .request()

        else:
            # If it does exist  => update with PATCH
            r = self.api.patch(self.model)\
                .jwt_auth(self._jwt_token, { "role": self._db_role })\
                .json(json_data)\
                .eq("id", id)\
                .request()

        if r.status_code == 503:
            # Heroku throws 503, application error after timeout but is not
            # specific about why
            msg = "Error uploding data. Probably timeout from server."
            raise ConnectionError(msg)

        self.response = r

        return r


    def delete(self, id_):
        """Remove an object from database by id
        """
        # Handle both "my_id" and "my_id.json"
        id_ = id_.replace(".json","")

        self.response = self.api.delete(self.model)\
            .jwt_auth(self._jwt_token, { "role": self._db_role })\
            .eq("id",id_)\
            .request()

        return self.response


class DatabaseDatasetConnection(DatabaseConnection):
    """Datasets behave differently than other objects (alarms and newsleads).
        On `.store()` we need to be able to append to existing dataset.
    """
    def store(self, filename, json_data, on_existing="update", **kwargs):
        """Insert, or if object already exist, append.

        :param filename (str): File name (which should be same as id)
        :param json_data (dict): The json data to be stored.
        :param on_existing (str):
            - "override": replaces existing
            - "update": updates existing data and metadata if duplicates found
            - "preserve": preserves existing on duplicates
            - "break": throw error
        :returns (Requests.Response): A response instance from the Request module.
        """
        id = filename.replace(".json","")

        # Check if dataset exists
        if not self.exists(**{"id": id}):
            # Insert new
            r = self.api.post(self.model)\
                .jwt_auth(self._jwt_token, { "role": self._db_role })\
                .json(json_data)\
                .request()

        else:
            # Update/override existing dataset
            if on_existing in ["update", "preserve", "break"]:
                # Append to existing dataset
                _r = self.api.get(self.model)\
                    .eq("id", id)\
                    .single()\
                    .request()


                # Get existing data
                existing_ds = Dataset(_r.json()["json_data"])

                # Merge with new
                new_ds = Dataset(json_data)

                # Use the same action (preserve/update) for both data and metadata
                existing_ds.append(new_ds, include_status=False,
                    on_duplicates=on_existing, on_metadata_conflict=on_existing)
                json_data = existing_ds.json

            # Upload final data
            r = self.api.patch(self.model)\
                .jwt_auth(self._jwt_token, { "role": self._db_role })\
                .json(json_data)\
                .eq("id", id)\
                .request()

        if r.status_code == 400:
            e = r.json()
            try:
                reason = e["error"]
            except KeyError:
                # Hack
                reason = e["errors"]
            raise ConnectionError(u"Message: {}\nErrors:\{}"\
                .format(e["message"], reason))

        if r.status_code == 503:
            # Heroku throws 503, application error after timeout but is not
            # specific about why
            msg = "Error uploding data. Probably timeout from server."
            raise ConnectionError(msg)

        self.response = r

        return r


class DatabaseFileConnection(Connection):
    """'schema' and 'recipe' are not stored in the actual
        postgres database. Hence the API to get these are slightly different.
    """

    def __init__(self, api_url, endpoint=None):
        """:param base_url: Url to api
            :param endpoint: eg. 'recipe', 'schema'
        """
        self.base_url = api_url
        self.endpoint = endpoint

    def get(self, cache=False, **kwargs):
        """List all schemas
        :param cache (bool): Cache request
        """
        if "id" in kwargs:
            return self.get_by_id(kwargs["id"], cache=cache)
        elif len(kwargs.keys()) > 0:
            msg = "Query not supported for DatabaseFileConnection."
            raise ValueError(msg)

        url = self.base_url + "/" + self.endpoint

        return self._get_request(url, cache=cache)


    def get_by_id(self, id_, cache=False):
        """Get a schema by id. Id may or may not include .json at the end
        :param id_: Id of object, with or without ".json" extension
        :param cache (bool): Cache request
        """
        if id_[-5:] != ".json":
            id_ += ".json"

        url = self.base_url + "/" + self.endpoint + "/" + id_

        return self._get_request(url, cache=cache)["json_data"]


    def _get_request(self, url, cache=False):
        """Make a GET request, and possibly cache
        :returns: response as json
        """
        def the_request():
            self.response = requests.get(url)
            r = self.response
            if r.status_code != 200:
                raise RequestException("{}: {}".format(r.status_code, r.reason), r)

            return r.json()

        if cache:
            if not cache_initiated():
                requests_cache.install_cache(backend='memory')
            return the_request()
        else:
            with requests_cache.disabled():
                return the_request()





class DatabaseSchemaConnection(DatabaseFileConnection):
    def __init__(self, api_url):
        super(DatabaseSchemaConnection, self).__init__(api_url)
        self.endpoint = "schema"

class DatabaseRecipeConnection(DatabaseFileConnection):
    def __init__(self, api_url):
        super(DatabaseRecipeConnection, self).__init__(api_url)
        self.endpoint = "recipe"


class DatabasePipelineConnection(DatabaseFileConnection):
    def __init__(self, api_url):
        super(DatabasePipelineConnection, self).__init__(api_url)
        self.endpoint = "pipeline"



class AWSConnection(Connection):
    """For storing files at Amazon
    """
    def __init__(self, bucket_name, aws_access_key_id, aws_secret_access_key,
        region_name="eu-central-1"):
        self.s3_client = boto3.client('s3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
            config=Config(signature_version='s3v4'))
        self.bucket = bucket_name
        self.type = "aws"

    def store(self, filename, file_data, folder=None):
        """
        :param filename: Name of file
        :type filename: str
        :param file_data: File content
        :type file_data: str|file
        :param folder: subfolder to store to (optional)
        :type folder: str
        """
        if folder:
            filename = os.path.join(folder, filename)

        if isinstance(file_data, str) or isinstance(file_data, unicode):
            return self.s3_client.put_object(Bucket=self.bucket,
                Key=filename, Body=file_data)

        if isinstance(file_data, file):
            return self.s3_client.upload_fileobj(file_data, self.bucket, filename)


class RequestException(Exception):
    """Custom exception for request errors. Makes the
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

class ConnectionError(Exception):
    """Custom excpetion for the Connection class
    """
    pass
