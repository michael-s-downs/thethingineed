### This code is property of the GGAO ###


# Native imports
import re
from typing import Union, List
from abc import ABC, abstractmethod

# Installed imports
from haystack.nodes import EmbeddingRetriever, BM25Retriever
from transformers import AutoConfig


class Retriever(ABC):
    MODEL_FORMAT = "BaseRetriever"

    DEFAULT_PARAMS = {

    }

    def __init__(self, document_store, embedding_model=None, api_key=None) -> None:
        self.document_store = document_store
        self.embedding_model = embedding_model
        self.api_key = api_key
        # self.device = [torch.device("cuda")] if torch.cuda.is_available() else [torch.device("cpu")]

    @property
    @abstractmethod
    def retr(self) -> Union[EmbeddingRetriever, BM25Retriever]:
        """Retriever to be set
        """

    @staticmethod
    @abstractmethod
    def get_embedding_dim(model) -> int:
        """Inferes the model dimensión from model name
        """

    @staticmethod
    @abstractmethod
    def get_similarity_function(model) -> str:
        """Best pre defined similarity for each model
        """

    def get_retr_config(self, unique_params) -> dict:
        """Document store conf
        """
        retr_config = {
            "document_store": self.document_store,
            "embedding_model": self.embedding_model,
            "api_key": self.api_key,
            "scale_score": True
        }
        retr_config.update(self.DEFAULT_PARAMS)
        retr_config.update(unique_params)
        return retr_config

    def set_retriever(self, unique_params) -> Union[EmbeddingRetriever, BM25Retriever]:
        """Returns a dicitonary with the fields that the given retriever needs
        """
        retr_conf = self.get_retr_config(unique_params)
        ds_config = {key: value for key, value in retr_conf.items() if value is not None}
        return self.retr(**ds_config)

    @classmethod
    def is_platform_type(cls, model_type):
        """Checks if a given model type is equel to the model format and thus it must be the one to use.
        """
        return model_type == cls.MODEL_FORMAT


class BM25(Retriever):

    MODEL_FORMAT = "bm25"

    DEFAULT_PARAMS = {

    }

    @property
    def retr(self) -> BM25Retriever:
        """Retriever to be set
        """
        return BM25Retriever

    @staticmethod
    def get_embedding_dim(model) -> int:
        """Inferes the model dimensión from model name
        """
        return None

    @staticmethod
    def get_similarity_function(model) -> str:
        """Best pre defined similarity for each model
        """
        return "dot_product"


class Similarity(Retriever):

    MODEL_FORMAT = "similarity"

    DEFAULT_PARAMS = {
        "model_format": "sentence_transformers",
        "batch_size": 32
    }

    @property
    def retr(self) -> EmbeddingRetriever:
        """Retriever to be set
        """
        return EmbeddingRetriever

    @staticmethod
    def get_embedding_dim(model) -> int:
        """Inferes the model dimensión from model name
        """
        return AutoConfig.from_pretrained(model).hidden_size

    @staticmethod
    def get_similarity_function(model) -> str:
        """Best pre defined similarity for each model
        """
        if re.search("-dpr-|-dot-", model):
            return "dot_product"
        return "cosine"


class OpenAI(Retriever):

    MODEL_FORMAT = "openai"

    DEFAULT_PARAMS = {
        "model_format": "openai",
        "batch_size": 32
    }

    @property
    def retr(self) -> EmbeddingRetriever:
        """Retriever to be set
        """
        return EmbeddingRetriever

    @staticmethod
    def get_embedding_dim(model) -> int:
        """Inferes the model dimensión from model name
        """
        return {
            "text-embedding-ada-002": 1536
        }.get(model, 2048)

    @staticmethod
    def get_similarity_function(model) -> str:
        """Best pre defined similarity for each model
        """
        return "cosine"


class ManagerRetriever(object):
    MODEL_TYPES = [OpenAI, Similarity, BM25]

    @staticmethod
    def get_platform(conf: dict) -> Retriever:
        """ Method to instantiate the document store class: [elasticsearch]

        :param conf: Model configuration. Example:  {"type":"elastic"}
        """
        for retriever in ManagerRetriever.MODEL_TYPES:
            retriever_type = conf.get('type')
            if retriever.is_platform_type(retriever_type):
                conf.pop('type')
                return retriever(**conf)
        raise ValueError(f"Platform type doesnt exist {conf}. "
                         f"Possible values: {ManagerRetriever.get_possible_platforms()}")

    @staticmethod
    def get_possible_platforms() -> List:
        """ Method to list the endpoints: [azure, openai]

        :param conf: Model configuration. Example:  {"platform":"openai"}
        """
        return [store.MODEL_FORMAT for store in ManagerRetriever.MODEL_TYPES]