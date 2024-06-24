### This code is property of the GGAO ###


# Native imports
import importlib
import logging
from typing import Union

# Custom imports
from genai_sdk_services.resources.import_user_functions import import_user_functions
from genai_sdk_services.services.db import BaseDBService, MysqlService, RedisService, AthenaService


class DBController(object):

    services = [MysqlService, RedisService, AthenaService]
    user_functions_services = []
    origins = {}

    def __init__(self, config: dict = None):
        """ Init the controller

        :param config: (dict) Configuration of the controller
        """
        self.logger = logging.getLogger(__name__)

        if config:
            user_functions = config.get('user_functions', None)

            if user_functions is None or type(user_functions) != list:
                user_functions = []

            for to_be_imported in import_user_functions():
                module = to_be_imported.__module__
                class_ = to_be_imported.__name__
                if class_ in user_functions:
                    globals()[f"{class_}"] = getattr(importlib.import_module(f"{module}"), class_)

            for function in user_functions:
                self.user_functions_services.append(globals()[function])

    def set_credentials(self, origin: tuple, credentials: dict = None):
        """ Set credentials to access the database origins

        :param origin: tuple(str, str) Origin of the data, tuple with the type of the origin (i.e. redis)
                      and the origin (i.e. the name of the database).
        :param credentials: (dict) Credentials of the database.
        """
        if origin[0] not in self.origins:
            self.origins[origin[0]] = self._get_origin(origin[0])
        self.origins[origin[0]].set_credentials(origin[1], credentials)

    def _get_origin(self, origin_type: str) -> BaseDBService:
        """ Get the service of the origin of the data

        :param origin_type: (str) Type of the origin to get the service of.
        :return: (BaseDbService) Service to be used
        """
        for origin in self.user_functions_services:
            if origin.check_origin(origin_type):
                return origin()

        for origin in self.services:
            if origin.check_origin(origin_type):
                return origin()

        raise ValueError("Type not supported")

    def create(self, origin: tuple, table: str, **kwargs) -> str:
        """ Creating table

        :param origin: tuple(str, str) Origin of the data, tuple with the type of the origin (i.e. redis)
                       and the origin (i.e. the name of the database).
        :param table: (str) Table to create
        :param kwargs: (dict) Additional params.
        :return: (str) failed or Succeded if execute query successfully
        """
        self.logger.debug("Controller - Inserting into db")
        try:
            if origin[0] not in self.origins:
                self.origins[origin[0]] = self._get_origin(origin[0])
            return self.origins[origin[0]].create(origin[1], table, **kwargs)
        except Exception as ex:
            self.logger.exception("Error while inserting into db")
            raise ex

    def partition(self, origin: tuple, table: str, s3_path: str = None) -> str:
        """ Partition the table

        :param origin: tuple(str, str) Origin of the data, tuple with the type of the origin (i.e. redis)
                      and the origin (i.e. the name of the database).
        :param table: (str) Table to partition
        :param s3_path: (str) S3 path to partition
        :return: (str) failed or Succeded if execute query successfully
        """
        self.logger.debug("Controller - Repairing partitions")
        try:
            if origin[0] not in self.origins:
                self.origins[origin[0]] = self._get_origin(origin[0])
            return self.origins[origin[0]].partition(origin[1], table, s3_path)
        except Exception as ex:
            self.logger.exception("Error while inserting into db")
            raise ex

    def insert(self, origin: tuple, table: str, fields: list, values: list, **kwargs) -> str:
        """ Insert into db

        :param origin: tuple(str, str) Origin of the data, tuple with the type of the origin (i.e. redis)
        and the origin (i.e. the name of the database).
        :param table: (str) Table to insert data in
        :param fields: (list) Columns
        :param values: (list) Values to insert
        :param kwargs: (dict) Additional params.
        :return: (str) Id of the insert
        """
        self.logger.debug("Controller - Inserting into db")
        try:
            if origin[0] not in self.origins:
                self.origins[origin[0]] = self._get_origin(origin[0])
            insert_id = self.origins[origin[0]].insert(origin[1], table, fields, values, **kwargs)
            return insert_id
        except Exception as ex:
            self.logger.exception("Error while inserting into db")
            raise ex

    def update(self, origin: tuple, table: str, fields: list, values: list, **kwargs) -> Union[bool, dict]:
        """ Update data in db

        :param origin: tuple(str, str) Origin of the data, tuple with the type of the origin (i.e. redis)
                      and the origin (i.e. the name of the database).
        :param table: (str) Table to update data from
        :param fields: (list) Columns
        :param values: (list) Values to update
        :param kwargs: (dict) Columns and data to update
        :return: (bool) True if data has been updated successfully
        """
        self.logger.debug("Controller - Updating data from db")
        try:
            if origin[0] not in self.origins:
                self.origins[origin[0]] = self._get_origin(origin[0])
            return self.origins[origin[0]].update(origin[1], table, fields, values, **kwargs)
        except Exception as ex:
            self.logger.exception("Error while updating data")
            raise ex

    def upsert(self, origin: tuple, table: str, fields: list, values: list, **kwargs) -> str:
        """ Upsert data in db

        :param origin: tuple(str, str) Origin of the data, tuple with the type of the origin (i.e. redis)
                      and the origin (i.e. the name of the database).

        :param table: (str) Table to upsert data from
        :param fields: (list) Columns
        :param values: (list) Values to upsert
        :param kwargs: (dict) Columns and data to upsert
        :return: (str) Id of the upsert
        """
        self.logger.debug("Controller - Upserting data from db")
        try:
            if origin[0] not in self.origins:
                self.origins[origin[0]] = self._get_origin(origin[0])
            return self.origins[origin[0]].upsert(origin[1], table, fields, values, **kwargs)
        except Exception as ex:
            self.logger.exception("Error while upserting data")
            raise ex

    def select(self, origin: tuple, fields: list, tables: list, **kwargs) -> list:
        """ Select data from the db

        :param origin: tuple(str, str) Origin of the data, tuple with the type of the origin (i.e. redis)
                      and the origin (i.e. the name of the database).
        :param fields: (list) Fields to select
        :param tables: (list) Table (or tables) to select data from
        :param kwargs: (dict) "Where" options
        :return: (list) Result of the select
        """
        self.logger.debug("Controller - Selecting data from db")
        try:
            if origin[0] not in self.origins:
                self.origins[origin[0]] = self._get_origin(origin[0])
            result = self.origins[origin[0]].select(origin[1], fields, tables, **kwargs)

            return result
        except Exception as ex:
            self.logger.exception("Error while selecting data")
            raise ex

    def get_query_select(self, origin: tuple, fields: list, tables: list, **kwargs) -> str:
        """ Create query to select data from the db

        :param origin: tuple(str, str) Origin of the data, tuple with the type of the origin (i.e. redis)
                       and the origin (i.e. the name of the database).
        :param fields: (list) Fields to select
        :param tables: (list) Table (or tables) to select data from
        :param kwargs: (dict) "Where" options
        :return: (str) Select query
        """
        self.logger.debug("Controller - Selecting data from db")
        try:
            if origin[0] not in self.origins:
                self.origins[origin[0]] = self._get_origin(origin[0])
            result = self.origins[origin[0]].get_query_select(origin[1], fields, tables, **kwargs)

            return result
        except Exception as ex:
            self.logger.exception("Error while selecting data")
            raise ex

    def get_query_create(self, origin: tuple, table: str, s3_path: str = None, **kwargs) -> str:
        """ Create query to create a table

        :param origin: tuple(str, str) Origin of the data, tuple with the type of the origin (i.e. redis)
                      and the origin (i.e. the name of the database).
        :param table: (str) Table to create
        :param s3_path: (str) Path in s3 of the Athena table
        :param kwargs: (dict) Additional params.
            In athena: fields, lines, input, output.
        :return: (str) Select query
        """
        self.logger.debug("Controller - Selecting data from db")
        try:
            if origin[0] not in self.origins:
                self.origins[origin[0]] = self._get_origin(origin[0])
            result = self.origins[origin[0]].get_query_create(table, s3_path, **kwargs)

            return result
        except Exception as ex:
            self.logger.exception("Error while selecting data")
            raise ex

    def execute_query(self, origin: tuple, query: str, **kwargs) -> Union[list, str]:
        """ Execute query

        :param origin: <tuple(str, str)> Origin of the data, tuple with the type of the origin (i.e. redis)
                       and the origin (i.e. the name of the database).
        :param query: (str) Query to execute
        :param kwargs: (dict) Additional params
        :return: (list) Result of query
        """
        self.logger.debug("Controller - Selecting data from db")
        try:
            if origin[0] not in self.origins:
                self.origins[origin[0]] = self._get_origin(origin[0])
            result = self.origins[origin[0]].execute_query(origin[1], query, **kwargs)

            return result
        except Exception as ex:
            self.logger.exception("Error while selecting data")
            raise ex

    def count(self, origin: tuple, fields: list, tables: list, **kwargs) -> int:
        """ Count data from the db

        :param origin: tuple(str, str) Origin of the data, tuple with the type of the origin (i.e. redis)
        and the origin (i.e. the name of the database).
        :param fields: (list) Fields to select
        :param tables: (list) Table (or tables) to select data from
        :param kwargs: (dict) "Where" options
        :return: (int) Result of the count
        """
        self.logger.debug("Controller - Counting data from db")
        try:
            if origin[0] not in self.origins:
                self.origins[origin[0]] = self._get_origin(origin[0])
            result = self.origins[origin[0]].count(origin[1], fields, tables, **kwargs)

            return result
        except Exception as ex:
            self.logger.exception("Error while counting data")
            raise ex

    def delete(self, origin: tuple, table=None, **kwargs) -> str:
        """ Delete data from the db

        :param origin: tuple(str, str) Origin of the data, tuple with the type of the origin (i.e. redis)
                      and the origin (i.e. the name of the database).
        :param table: Table to delete data from
        :param kwargs: "Where" options
        """
        self.logger.debug("Controller - Deleting data from db")
        try:
            if origin[0] not in self.origins:
                self.origins[origin[0]] = self._get_origin(origin[0])
            self.origins[origin[0]].delete(origin[1], table, **kwargs)
        except Exception as ex:
            self.logger.exception("Error while deleting data")
            raise ex

    def purge(self, origin: tuple):
        """ Delete data from the db

        :param origin: tuple(str, str) Origin of the data, tuple with the type of the origin (i.e. redis)
                      and the origin (i.e. the name of the database)
        """
        self.logger.debug("Controller - Purging db")
        try:
            if origin[0] not in self.origins:
                self.origins[origin[0]] = self._get_origin(origin[0])
            self.origins[origin[0]].purge(origin[1])
        except Exception as ex:
            self.logger.exception("Error while deleting data")
            raise ex

