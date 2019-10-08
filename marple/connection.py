# coding: utf-8
"""Contains base classes for connecting to datasources
"""

# This module hs been moved to nw_connections, but we keep it here for
# backward compability

from nw_connections.connection import *

# DatabaseDatasetConnection, however, depend on marple.dataset.Dataset
# which in turn depend on pandas, which is a dependecy too heavy for
# nw_connections.py
from marple.dataset import Dataset

class DatabaseDatasetConnection(DatabaseConnection):
    """Datasets behave differently than other objects (alarms and newsleads).
        On `.store()` we need to be able to append to existing dataset +
        authentication is stricter.
    """
    @require_jwt_auth
    def get(self, *arg, **kwargs):
        return super(DatabaseDatasetConnection, self).get(*arg, **kwargs)

    @require_jwt_auth
    def get_by_id(self, *arg, **kwargs):
        return super(DatabaseDatasetConnection, self).get_by_id(*arg, **kwargs)

    @require_jwt_auth
    def exists(self, *arg, **kwargs):
        return super(DatabaseDatasetConnection, self).exists(*arg, **kwargs)


    @require_jwt_auth
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
                    .jwt_auth(self._jwt_token, { "role": self._db_role })\
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
