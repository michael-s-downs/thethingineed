### This code is property of the GGAO ###


# Native imports
from typing import Union, List
from abc import ABC, abstractmethod

# Installed imports
from haystack.document_stores import ElasticsearchDocumentStore


class DocumentStore(ABC):
    MODEL_FORMAT = "DocumentStore"

    DEFAULT_PARAMS = {
        'verify_certs': False
    }

    def __init__(self, index, embedding_field, embedding_dim, similarity_function=None, scheme="https") -> None:

        self.index = index
        self.embedding_field = embedding_field
        self.embedding_dim = embedding_dim
        self.similarity_function = similarity_function
        self.scheme = scheme

    @property
    @abstractmethod
    def bds(self) -> Union[ElasticsearchDocumentStore]:
        """Base document store
        """

    def get_ds_config(self, unique_params: dict) -> dict:
        """Returns the specific configuration needed for each of the document stores

        Args:
            unique_params (dict): Specific parameters to the document store

        Returns:
            dict: _description_
        """
        ds_config = {
            'index': self.index,
            'embedding_field': self.embedding_field,
            'embedding_dim': self.embedding_dim,
            'similarity': self.similarity_function,
            'scheme': self.scheme,
        }
        ds_config.update(self.DEFAULT_PARAMS)
        ds_config.update({k: v for k, v in unique_params.items() if k in self.DEFAULT_PARAMS})
        ds_config = {key: value for key, value in ds_config.items() if value is not None}
        ds_config['verify_certs'] = False
        return ds_config

    def set_ds(self, unique_params) -> Union[ElasticsearchDocumentStore]:
        """It will set the defined document stores with the ds_config params. Every element that is None will be ignored

        Args:
             unique_params (dict): Specific parameters to the document store

        Returns:
            Union[ElasticsearchDocumentStore]: Returns the instatiated class
        """
        ds_config = self.get_ds_config(unique_params)
        return self.bds(**ds_config)

    @classmethod
    def is_platform_type(cls, model_type):
        """Checks if a given model type is equel to the model format and thus it must be the one to use.
        """
        return model_type == cls.MODEL_FORMAT


class ElasticSearch(DocumentStore):

    MODEL_FORMAT = "elastic"

    DEFAULT_PARAMS = {
        'host': "",
        'username': "",
        'password': "",
        'excluded_meta_data': [],
        'synonyms': [],
        'recreate_index': False,
        'timeout': 30,
        'scheme': "https",
        'verify_certs': False
    }

    @property
    def bds(self) -> Union[ElasticsearchDocumentStore]:
        """Base document store
        """
        return ElasticsearchDocumentStore


class ManagerDocumentStore(object):
    MODEL_TYPES = [ElasticSearch]

    @staticmethod
    def get_platform(conf: dict) -> DocumentStore:
        """ Method to instantiate the document store class: [elasticsearch]

        :param conf: Model configuration. Example:  {"type":"elastic"}
        """
        for store in ManagerDocumentStore.MODEL_TYPES:
            store_type = conf.get('type')
            if store.is_platform_type(store_type):
                conf.pop('type')
                return store(**conf)
        raise ValueError(f"Platform type doesnt exist {conf}. "
                         f"Possible values: {ManagerDocumentStore.get_possible_platforms()}")

    @staticmethod
    def get_possible_platforms() -> List:
        """ Method to list the endpoints: [azure, openai]

        :param conf: Model configuration. Example:  {"platform":"openai"}
        """
        return [store.MODEL_FORMAT for store in ManagerDocumentStore.MODEL_TYPES]
