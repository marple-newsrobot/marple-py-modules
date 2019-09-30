# encoding: utf-8

""" Client for speaking with a Postgrest API.
    Attempts to mimic this Node counterpart: https://github.com/calebmer/postgrest-client
"""
import requests
import requests_cache
from requests.auth import HTTPBasicAuth
from requests_jwt import JWTAuth
from marple.utils import cache_initiated

FILTER_OPERATORS = ['eq', 'gt', 'lt', 'gte', 'lte', 'like', 'ilike', 'is', 'in', 'not']


class Api(object):
    def __init__(self, url):
        """Creates a new PostgREST API object.
        Use this to start building PostgREST requests.
        Methods of this class can be chained. The final
        execution of the request will only happen when `.request()`
        is called.

        Example:
            api = Api("https://my_postgrest_api.com")
            r = api.get("/table")\
                .eq("id","foo")\
                .single()\
                .request()

        :param url: The base URL of the API.
        :type url: str
        """
        self.url = url


    def __unicode__(self):
        return u"<PostgrestAPI: {}>".format(self.url)

    def request(self, method, path):
        """Perform a GET request to the Postgrest API.

        :param method: An HTTP verb (post, get, put...)
        :type method: str
        :param path: Route path (for example `/dataset`)
        :type path: str
        :returns: The API request object.
        :rtype: ApiRequest
        """

        # Add "/" if missing
        if path[0] != "/":
            path = "/" + path
        return ApiRequest(method, self.url + path)

    def get(self, path):
        """Perform a GET request to the Postgrest API.

        :param path (str): Route path (for example `/dataset`)
        :returns: The API request object.
        :rtype: ApiRequest
        """
        return self.request("get", path)


    def post(self, path):
        """Perform a POST request to the Postgrest API.

        :param path (str): Route path (for example `/dataset`)
        :returns: The API request object.
        :rtype: ApiRequest
        """
        return self.request("post", path)


    def patch(self, path):
        """Perform a PATCH request to the Postgrest API.

        :param path (str): Route path (for example `/dataset`)
        :returns: The API request object.
        :rtype: ApiRequest
        """
        return self.request("patch", path)

    def delete(self, path):
        """Perform a DELETE request to the Postgrest API.

        :param path (str): Route path (for example `/dataset`)
        :returns: The API request object.
        :rtype: ApiRequest
        """
        return self.request("delete", path)


    """
    TODO: Add methods for all HTTP verbs
    """

class ApiRequest(object):
    def __init__(self, method, url):
        """A request building class which contains convenience methods for
        communicating with a PostgREST server.

        :param method (string): The HTTP method of the request.
        :param url (string): The path to the request
        """
        self.method = method
        self.url = url

        self._auth = None
        self._query = {}
        self._headers = {}
        self._json = None

    def __unicode__(self):
        return u"<ApiRequest: {} ({})>".format(self.url, self.method)

    def auth(self, user_and_pass):
        """
        When passed a single string, the request will be authenticated using
        the Bearer scheme. Two parameters will be the Basic scheme.
        An object with user and pass properties will also use the Basic scheme.

        api.get('/speakers').auth({ user: 'bob', pass: 'password' }) // Basic
        api.get('/speakers').auth('xyz') // Bearer

        :param user_and_pass (str|dict): The user, bearer token, or user/pass dict.
        :returns: The API request object.
        :rtype: ApiRequest
        """
        if isinstance(user_and_pass, str):
            raise NotImplementedError()

        elif isinstance(user_and_pass, dict):
            try:
                user = user_and_pass["user"]
                password = user_and_pass["pass"]
                auth = HTTPBasicAuth(user, password)
            except KeyError:
                raise ValueError(("auth excepted a dict with" +
                    "'user' and 'password' key. Got {}."
                    ).format(user_and_pass))

        self._auth = auth
        return self


    def jwt_auth(self, secret, payload={}):
        """A convinience method for doing a JWT auth.

        :param secret (str): Secret JWT token.
        :param payload (dict): Payload data.
        :returns: The API request object.
        :rtype: ApiRequest
        """
        auth = JWTAuth(secret, header_format='Bearer %s')
        for key, value in payload.items():
            auth.add_field(key, value)

        self._auth = auth
        return self

    def json(self, json_data):
        """
        Pass a json object as payload

        :param json_data (dict): A json object
        :returns: The API request object.
        :rtype: ApiRequest
        """
        self._json = json_data

        return self

    def csv(self):
        """ Pass csv as body
        """
        raise NotImplementedError()

    def select(self, select):
        """:param select (str|list): Columns to select
        :returns: The API request object.
        :rtype: ApiRequest
        """
        if isinstance(select, list):
            select = ",".join(select)

        self._query.update({"select": select})

        return self


    def single(self):
        self._headers.update({
            'Prefer': 'plurality=singular'
        })
        return self

    def match(self, query):
        """
        Takes a query object and translates it to a PostgREST filter query string.
        All string values are used to get exact match (using `eq.` prefix).
        Lists (e.g { "source": ["AMS", "SMS"] }) are matched with `in.`

        :param query (dict): Column names as keys, value to be selected as value.
        :returns: The API request object.
        :rtype: ApiRequest
        """
        for key, value in query.items():
            if isinstance(value, list):
                query[key] = u"in." + u",".join(value)
            else:
                query[key] = u"eq." + value

        self._query.update(query)

        return self

    def filter(self, column, operator, value):
        """Filter query with one an operator of choice (eq, gt, lt...)

        :param column (str): Column name.
        :param operator (str): Operator name ('eq', 'gt', 'lt', 'gte', 'lte', 'like', 'ilike', 'is', 'in', 'not').
        :param value (str): Value to filter by.
        :returns: The API request object.
        :rtype: ApiRequest
        """
        if operator not in FILTER_OPERATORS:
            raise ValueError("{} is not a valid filter operator.".format(operator))
        query = {}
        query[column] = u"{}.{}".format(operator, value)
        self._query.update(query)

        return self

    def eq(self, column, value):
        """A convenience method for filtering on the eq opertor

        :param column (str): Column name.
        :param value (str): Value to filter by.
        :returns: The API request object.
        :rtype: ApiRequest

        """
        return self.filter(column, "eq", value)

    def is_in(self, column, value):
        """A convenience method for filtering on the `in` opertor
        Named `is_in` as "in" is reserved.

        :param column (str): Column name.
        :param value (str|list): A comma separetad list of values (or list=.
        :returns: The API request object.
        :rtype: ApiRequest
        """
        if isinstance(value, list):
            value = ",".join(value)
        return self.filter(column, "in", value)

    def gt(self, column, value):
        """A convenience method for filtering on the "greater than" opertor

        :param column (str): Column name.
        :param value (str): Value to filter by.
        :returns: The API request object.
        :rtype: ApiRequest

        """
        return self.filter(column, "gt", value)


    def gte(self, column, value):
        """A convenience method for filtering on the "greater than or equal to"
        opertor

        :param column (str): Column name.
        :param value (str): Value to filter by.
        :returns: The API request object.
        :rtype: ApiRequest

        """
        return self.filter(column, "gte", value)


    def lt(self, column, value):
        """A convenience method for filtering on the "less than" opertor

        :param column (str): Column name.
        :param value (str): Value to filter by.
        :returns: The API request object.
        :rtype: ApiRequest

        """
        return self.filter(column, "lt", value)


    def lte(self, column, value):
        """A convenience method for filtering on the "less than or equal to"
        opertor

        :param column (str): Column name.
        :param value (str): Value to filter by.
        :returns: The API request object.
        :rtype: ApiRequest

        """
        return self.filter(column, "lte", value)

    def upsert(self, duplicates="merge"):
        """Enable bulk upsert.

        For single item upsert use PUT + eq. parameter
        http://postgrest.org/en/v6.0/api.html#upsert
        """
        allowed_values = ["ignore", "merge"]
        if duplicates not in allowed_values:
            raise ValueError("duplicates must be one of {}".format(allowed_values))
        self._headers.update({
            "Prefer": "resolution={}-duplicates".format(duplicates)
        })
        return self

    def custom(self, key, value):
        """Adds a custom query parameter.
        """
        self._query[key] = value
        return self

    def order(self):
        raise NotImplementedError()

    def range(self, start, end):
        raise NotImplementedError()

    def request(self, cache=False):
        """Execute request

        :cache (bool): Cache request (in memory)
        :returns (Requests.Repsonse): An instance of Requests.Repsonse
        """
        def the_request():
            return requests.request(self.method,
                                    auth=self._auth,
                                    url=self.url,
                                    params=self._query,
                                    headers=self._headers,
                                    json=self._json)

        if cache:
            if not cache_initiated():
                requests_cache.install_cache(backend='memory')
            return the_request()
        else:
            with requests_cache.disabled():
                return the_request()
