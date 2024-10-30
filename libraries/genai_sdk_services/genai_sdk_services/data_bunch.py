### This code is property of the GGAO ###


# Native imports
import importlib
import logging
from typing import Union, Tuple

# Installed imports
import pandas

# Custom imports
from genai_sdk_services.resources.import_user_functions import import_user_functions
from genai_sdk_services.services.dataset import BaseDatasetService, AthenaService, CSVService, HDF5Service, CSVPathsService, ParquetService


class DataBunchController(object):

    services = [AthenaService, HDF5Service, CSVService, CSVPathsService, ParquetService]
    user_functions_services = []
    origins = {}

    def __init__(self, config: dict = None):
        """ Init the controller """
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

    def set_credentials(self, origin: str, sc_credentials: tuple = None, dbc_credentials: tuple = None):
        """ Set credentials to access the origins of the datasets

        :param origin: (tuple) Origin of the data, tuple with the type of the origin
                       and the origin (i.e. the name of the bucket).
        :param sc_credentials: (tuple) Credentials of the storage controller. Needed if ds is in a remote storage.
        :param dbc_credentials: (tuple) Credentials of the database controller. Needed if dataset is in a remote database.
        """
        if origin not in self.origins:
            self.origins[origin] = self._get_origin(origin)

        self.origins[origin].set_credentials(sc_credentials, dbc_credentials)

    def _get_origin(self, origin_type: str) -> BaseDatasetService:
        """ Get the service of the origin of the data

        :param origin_type: (str) Type of the origin to get the service of.
        :return: <BaseDatasetService> Service to be used
        """
        for origin in self.user_functions_services:
            if origin.check_origin(origin_type):
                return origin()

        for origin in self.services:
            if origin.check_origin(origin_type):
                return origin()

        raise ValueError("Type not supported")

    def get_dataset(self, origin: str, origin_bucket: str, path_name: str, **kwargs) -> pandas.DataFrame:
        """ Get the dataset from the origin

        :param origin: (str) Origin of the data
        :param origin_bucket: (str) Origin bucket
        :param path_name: (str) Path of the dataset
        :return: (pandas.DataFrame) Dataset instantiated
        """
        self.logger.debug("Controller - Getting dataset")
        try:
            if origin not in self.origins:
                self.origins[origin] = self._get_origin(origin)

            if origin == "athena":
                if "origin_db" not in kwargs:
                    raise ValueError("origin_db not found. origin_db is mandatory if origin is athena")

                return self.origins[origin].get_dataset(origin_bucket, path_name, **kwargs)

            return self.origins[origin].get_dataset(origin_bucket, path_name, **kwargs)
        except Exception as ex:
            self.logger.exception("Error while getting dataset")
            raise ex

    def store_dataset(self, origin: str, origin_bucket: str, dataset: pandas.DataFrame, path_name: str, **kwargs) -> bool:
        """ Store the dataset

        :param origin: Origin of the dataset
        :param origin_bucket: Origin bucket
        :param dataset: Dataframe to store
        :param path_name: Path to store the dataset in
        :return: (bool) True if the dataset was stored successfully
        """
        self.logger.debug("Controller - Storing dataset")
        try:
            if origin not in self.origins:
                self.origins[origin] = self._get_origin(origin)
            return self.origins[origin].store_dataset(origin_bucket, dataset, path_name, **kwargs)
        except Exception as ex:
            self.logger.exception("Error while storing dataset")
            raise ex

    def split_dataset(self, origin: str, df: pandas.DataFrame, label: str = "label", test_size: float = 0.2, stratify: bool = None, random_state: int = None) -> Tuple[list, list, list, list]:
        """ Split dataset into train and test

        :param origin: (str) Origin of the data
        :param df: (pandas) Dataframe to split
        :param label: (str) Label column
        :param test_size: (float) Test size
        :param stratify: (bool) Column to stratify
        :param random_state: (int) Random state
        :return: (tuple) List of results
                X_train, X_test, y_train and y_test
        """
        self.logger.debug("Controller - Splitting dataset")
        try:
            if origin not in self.origins:
                self.origins[origin] = self._get_origin(origin)
            return self.origins[origin].split_dataset(df, label, test_size, stratify, random_state)
        except Exception as ex:
            self.logger.exception("Error while splitting dataset")
            raise ex
