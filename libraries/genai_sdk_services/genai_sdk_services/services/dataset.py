### This code is property of the GGAO ###


# Native imports
import io
import os
import logging
import json
from typing import Tuple
from abc import ABCMeta, abstractmethod

# Installed imports
import pandas as pd
from sklearn.model_selection import train_test_split

# Custom imports
from genai_sdk_services.db import DBController
from genai_sdk_services.files import FilesController
from genai_sdk_services.storage import StorageController


class BaseDatasetService:
    @abstractmethod
    def get_dataset(self, origin, path, **kwargs):
        """ Get dataset

        :param origin: Database to connect to
        :return: Connection
        """
        pass

    @abstractmethod
    def store_dataset(self, origin, dataset, path_name, **kwargs):
        """ Store dataset

        :param origin: Database to insert to
        :param table: Table to insert data into
        :param fields: Fields of the data
        :param values: Values of the fields
        :return: Id of the result
        """
        pass

    @abstractmethod
    def split_dataset(self, df, label, test_size, stratify, random_state):
        pass

    @classmethod
    def check_origin(cls, origin_type):
        """ Check if it is a valid origin for the service

        :param origin_type: Origin to check
        :return: (bool) True if the origin is valid
        """
        return origin_type in cls.ORIGIN_TYPES


class AthenaService(BaseDatasetService):
    ORIGIN_TYPES = ["athena"]

    def __init__(self, config: dict = None):
        """ Init the service

        :param config: (dict) Configuration of the service
        """
        self.sc = StorageController(config)
        self.fc = FilesController(config)
        self.dbc = DBController(config)
        self.csv = CSVService(config)
        self.logger = logging.getLogger(__name__)

    def set_credentials(self, sc_credentials: tuple = None, dbc_credentials: tuple = None):
        """ Set credentials for services

        :param sc_credentials: (tuple) Tuple, first item is origin, second one are credentials
        :param dbc_credentials: (tuple) Tuple, first item is origin, second one are credentials
        """
        if sc_credentials:
            self.sc.set_credentials(sc_credentials)
        if dbc_credentials:
            self.dbc.set_credentials(dbc_credentials)

    def get_dataset(self, origin_bucket: str, tables: list, **kwargs: dict) -> pd.DataFrame:
        """ Get dataset

        :param origin_bucket: (str) Database to connect to
        :param tables: (list) Tables to get data from
        :param kwargs: (dict) Additional arguments
        :return: (pd.DataFrame) Dataframe
        """
        if "origin_db" not in kwargs:
            raise ValueError("origin_db must be given")
        else:
            origin_db = kwargs['origin_db']

        if type(tables) is not list:
            tables = [tables]

        if "fields" not in kwargs:
            fields = ['*']
        else:
            fields = kwargs['fields']

        if "output" not in kwargs:
            raise ValueError("Output file path not given")
        else:
            output = kwargs['output']

        fn = output + "/" + self.dbc.select(origin_db, fields, tables, **kwargs)

        return self.csv.get_dataset(origin_bucket, fn)

    def store_dataset(self, dataset: str, **kwargs: dict):
        """ Store dataset

        :param dataset: (str) Database to insert to
        :param kwargs: (dict) Additional arguments
        :return: Id of the result
        """
        raise NotImplementedError("Store in Athena is not supported")

    def split_dataset(self, df: pd.DataFrame, label: str = "label", test_size: float = 0.2, stratify: str = None, random_state: str = None) -> Tuple[list, list, list, list]:
        """ Split dataset

        :param df: (pandas.DataFrame) Dataset to split
        :param label: (str) Label of the dataset
        :param test_size: (float) Size of the test set
        :param stratify: (str) Column to stratify
        :param random_state: (str) Random state
        :return: (tuple) Tuple with X_train, X_test, y_train, y_test
        """
        try:
            if label in df.columns:
                column_label = df[label]
                df = df.drop(label, axis=1)
                X_train, X_test, y_train, y_test = train_test_split(df, column_label, random_state=random_state, test_size=test_size, stratify=stratify)
        except Exception as ex:
            self.logger.error("Error while splitting dataset")
            self.logger.error("Exception: %s" % str(ex))
            raise ex

        return X_train, X_test, y_train, y_test


class ParquetService(BaseDatasetService):
    ORIGIN_TYPES = ["parquet"]

    def __init__(self, config: dict = None):
        """ Init the service

        :param config: (dict) Configuration of the service
        """
        self.sc = StorageController(config)
        self.logger = logging.getLogger(__name__)

    def set_credentials(self, sc_credentials: tuple = None, dbc_credentials: tuple = None):
        """ Set credentials for services

        :param sc_credentials: (tuple) Tuple, first item is origin, second one are credentials
        :param dbc_credentials: (tuple) Tuple, first item is origin, second one are credentials
        """
        if sc_credentials:
            self.sc.set_credentials(sc_credentials)

    def get_dataset(self, origin: str, path_name: str, **kwargs: dict) -> pd.DataFrame:
        """ Get dataset

        :param origin: (str) Database to connect to
        :param path_name: (str) Path to the file
        :param kwargs: (dict) Additional arguments
        :return: (pd.DataFrame) Dataframe
        """
        if "engine" not in kwargs:
            engine = "pyarrow"
        else:
            engine = kwargs['engine']

        dataframe = pd.read_parquet(io.BytesIO(self.sc.load_file(origin=origin, remote_file=path_name)), engine=engine)

        return dataframe

    def store_dataset(self, origin: str, dataset: pd.DataFrame, path_name: str, **kwargs: dict) -> bool:
        """ Store dataset

        :param origin: (str) Origin to store the dataset
        :param dataset: (pd.DataFrame) Dataset to store
        :param path_name: (str) Path to store the dataset
        :param kwargs: (dict) Additional arguments
        :return: (bool) Status of the operation
        """
        if "engine" not in kwargs:
            engine = "pyarrow"
        else:
            engine = kwargs['engine']

        try:
            dataset.to_parquet(path_name, engine=engine)

            status = self.sc.upload_file(origin, path_name, path_name)
            os.remove(path_name)
        except Exception as ex:
            self.logger.error("Error while storing dataset %s" % path_name)
            self.logger.error("Exception: %s" % str(ex))
            raise ex

        return status

    def split_dataset(self, df: pd.DataFrame, label: str = "label", test_size: str = 0.2, stratify: str = None, random_state: str = None) -> Tuple[list, list, list, list]:
        """ Split dataset

        :param df: (pandas.DataFrame) Dataset to split
        :param label: (str) Label of the dataset
        :param test_size: (float) Size of the test set
        :param stratify: (str) Column to stratify
        :param random_state: (str) Random state
        :return: (tuple) Tuple with X_train, X_test, y_train, y_test
        """
        if label in df.columns:
            column_label = df[label]
            df = df.drop(label, axis=1)
            X_train, X_test, y_train, y_test = train_test_split(df, column_label, random_state=random_state, test_size=test_size, stratify=stratify)

            return X_train, X_test, y_train, y_test


class HDF5Service(BaseDatasetService):
    ORIGIN_TYPES = ["hdf5"]

    def __init__(self, config: dict = None):
        """ Init the service

        :param config: (dict) Configuration of the service
        """
        self.sc = StorageController(config)
        self.logger = logging.getLogger(__name__)

    def set_credentials(self, sc_credentials: tuple = None, dbc_credentials: tuple = None):
        """ Set credentials for services

        :param sc_credentials: (tuple) Tuple, first item is origin, second one are credentials
        :param dbc_credentials: (tuple) Tuple, first item is origin, second one are credentials
        """
        if sc_credentials:
            self.sc.set_credentials(sc_credentials)

    def get_dataset(self, origin: str, path_name: str, **kwargs: dict) -> pd.DataFrame:
        """ Get dataset

        :param origin: (str) Database to connect to
        :param path_name: (str) Path to the file
        :param kwargs: (dict) Additional arguments
        :return: (pd.DataFrame) Dataframe
        """
        hdf5name = kwargs.get("hdf5name", "df")

        dataset_buffer = io.BytesIO(self.sc.load_file(origin=origin, remote_file=path_name))

        out = self._read_hdf_from_buffer(path_name, hdf5name, dataset_buffer)
        return out

    def store_dataset(self, origin: str, dataset: pd.DataFrame, path_name: str, **kwargs: dict) -> bool:
        """ Store dataset

        :param origin: (str) Origin to store the dataset
        :param dataset: (pd.DataFrame) Dataset to store
        :param path_name: (str) Path to store the dataset
        :param kwargs: (dict) Additional arguments
        :return: (bool) Status of the operation
        """
        hdf5key = kwargs.get('hdf5name', "df")

        dataset.to_hdf(path_name, key=hdf5key, mode="w")

        status = self.sc.upload_file(origin, path_name, path_name)
        os.remove(path_name)

        return status

    def _read_hdf_from_buffer(self, filename: str, hdf5object: str, buffer: str) -> pd.DataFrame:
        """ Read hdf5 from buffer

        :param filename: (str) Filename
        :param hdf5object: (str) Object to read
        :param buffer: (str) Buffer
        :return: (pd.DataFrame) Dataframe
        """
        with pd.HDFStore(
                filename,
                mode="r",
                driver="H5FD_CORE",
                driver_core_backing_store=0,
                driver_core_image=buffer.read(),
        ) as store:

            if len(store.keys()) > 1:
                raise Exception("Ambiguous matrix store. More than one dataframe in the hdf file.")
            try:
                return store[hdf5object]

            except KeyError:
                self.logger.warning("The hdf file should contain one and only key, matrix.")
                return store[store.keys()[0]]

    def split_dataset(self, df: pd.DataFrame, label: str = "label", test_size: float = 0.2, stratify: str = None, random_state: str = None) -> Tuple[list, list, list, list]:
        """ Split dataset

        :param df: (pandas.DataFrame) Dataset to split
        :param label: (str) Label of the dataset
        :param test_size: (float) Size of the test set
        :param stratify: (str) Column to stratify
        :param random_state: (str) Random state
        :return: (tuple) Tuple with X_train, X_test, y_train, y_test
        """
        if label in df.columns:
            column_label = df[label]
            df = df.drop(label, axis=1)
            X_train, X_test, y_train, y_test = train_test_split(df, column_label, random_state=random_state, test_size=test_size, stratify=stratify)

            return X_train, X_test, y_train, y_test


class CSVService(BaseDatasetService):
    ORIGIN_TYPES = ["csv"]

    def __init__(self, config: dict = None):
        """ Init the service

        :param config: (dict) Configuration of the service
        """
        self.sc = StorageController(config)
        self.fc = FilesController(config)
        self.logger = logging.getLogger(__name__)

    def set_credentials(self, sc_credentials: tuple = None, dbc_credentials: tuple = None):
        """ Set credentials for services

        :param sc_credentials: Tuple, first item is origin, second one are credentials
        :param dbc_credentials: Tuple, first item is origin, second one are credentials
        """
        if sc_credentials:
            self.sc.set_credentials(sc_credentials)

    def get_dataset(self, origin: str, path_name: str, **kwargs: dict) -> pd.DataFrame:
        """ Get dataset

        :param origin: (str) Database to connect to
        :param path_name: (str) Path to the file
        :param kwargs: (dict) Additional arguments
        :return: (pd.DataFrame) Dataframe
        """
        dataframe = pd.read_csv(
            io.BytesIO(self.sc.load_file(origin=origin, remote_file=path_name)),
            nrows=kwargs.get("limit") and kwargs.get("limit"),
            skiprows=kwargs.get("offset") and (lambda x: x != 0 and x <= kwargs.get("offset")),
        )

        return dataframe

    def store_dataset(self, origin: str, dataset: pd.DataFrame, path_name: str, **kwargs: dict) -> bool:
        """ Store dataset

        :param origin: (str) Origin to store the dataset
        :param dataset: (pd.DataFrame) Dataset to store
        :param path_name: (str) Path to store the dataset
        :param kwargs: (dict) Additional arguments
        :return: (bool) Status of the operation
        """
        dataset.to_csv(path_name, **kwargs)

        status = self.sc.upload_file(origin, path_name, path_name)
        os.remove(path_name)

        return status

    def split_dataset(self, df: pd.DataFrame, label: str = "label", test_size: float = 0.2, stratify: str = None, random_state: str = None) -> Tuple[list, list, list, list]:
        """ Split dataset

        :param df: (pandas.DataFrame) Dataset to split
        :param label: (str) Label of the dataset
        :param test_size: (float) Size of the test set
        :param stratify: (str) Column to stratify
        :param random_state: (str) Random state
        :return: (tuple) Tuple with X_train, X_test, y_train, y_test
        """
        if label in df.columns:
            column_label = df[label]
            df = df.drop(label, axis=1)
            X_train, X_test, y_train, y_test = train_test_split(df, column_label, random_state=random_state, test_size=test_size, stratify=stratify)

            return X_train, X_test, y_train, y_test


class CSVPathsService(BaseDatasetService):
    ORIGIN_TYPES = ["csv_paths"]

    def __init__(self, config: dict = None):
        """ Init the service

        :param config: (dict) Configuration of the service
        """
        self.fc = FilesController(config)
        self.sc = StorageController(config)
        self.logger = logging.getLogger(__name__)

    def set_credentials(self, sc_credentials: tuple = None, dbc_credentials: tuple = None):
        """ Set credentials for services

        :param sc_credentials: Tuple, first item is origin, second one are credentials
        :param dbc_credentials: Tuple, first item is origin, second one are credentials
        """
        if sc_credentials:
            self.sc.set_credentials(sc_credentials)

    def get_dataset(self, origin: str, path_name: str, **kwargs: dict) -> pd.DataFrame:
        """ Get dataset

        :param origin: (str) Database to connect to
        :param path_name: (str) Path to the file
        :param kwargs: (dict) Additional arguments
        :return: (pd.DataFrame) Dataframe
        """
        limit = kwargs.get('limit', -1)

        df = pd.read_csv(io.BytesIO(self.sc.load_file(origin=origin, remote_file=path_name)))

        num_classes = len(df.label.unique())
        if limit != -1:
            if limit < num_classes:
                self.logger.warning("Num of samples less than num of classes. Creating DataFrame with one sample per class")
                df = df.groupby("label", group_keys=False).apply(lambda x: x.sample(min(len(x), 1)))
            else:
                df = df.groupby("label", group_keys=False).apply(lambda x: x.sample(min(len(x), int(limit / num_classes))))

        df['text'] = df.apply(lambda x: self.fc.get_text_from_bytes(file=self.sc.load_file(origin=origin, remote_file=x.path), type_file="txt"), axis=1)

        return df

    def store_dataset(self, origin: str, dataset: pd.DataFrame, path_name: str, **kwargs: dict) -> bool:
        """ Store dataset

        :param origin: (str) Origin to store the dataset
        :param dataset: (pd.DataFrame) Dataset to store
        :param path_name: (str) Path to store the dataset
        :param kwargs: (dict) Additional arguments
        :return: (bool) Status of the operation
        """
        hdf5key = kwargs.get("hdf5name", "df")

        dataset.to_hdf(path_name, key=hdf5key, mode="w")

        status = self.sc.upload_file(origin, path_name, path_name)
        os.remove(path_name)

        return status

    def split_dataset(self, df: pd.DataFrame, label: str = "label", test_size: float = 0.2, stratify: str = None, random_state: str = None) -> Tuple[list, list, list, list]:
        """ Split dataset

        :param df: (pandas.DataFrame) Dataset to split
        :param label: (str) Label of the dataset
        :param test_size: (float) Size of the test set
        :param stratify: (str) Column to stratify
        :param random_state: (str) Random state
        :return: (tuple) Tuple with X_train, X_test, y_train, y_test
        """
        if label in df.columns:
            column_label = df[label]
            df = df.drop(label, axis=1)
            X_train, X_test, y_train, y_test = train_test_split(df, column_label, random_state=random_state, test_size=test_size, stratify=stratify)

            return X_train, X_test, y_train, y_test
