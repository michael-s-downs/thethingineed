### This code is property of the GGAO ###


# Native imports
from typing import List
from abc import ABC, abstractmethod
import os
import logging


# Installed imports


# Custom imports
from common.errors.genaierrors import PrintableGenaiError
from common.logging_handler import LoggerHandler
from common.services import RETRIEVAL_STRATEGIES


class SimpleStrategy(ABC):
    CHUNKING_FORMAT = "ChunkingMethod"

    def __init__(self):
        log = logging.getLogger('werkzeug')
        log.disabled = True

        logger_handler = LoggerHandler(RETRIEVAL_STRATEGIES, level=os.environ.get('LOG_LEVEL', "INFO"))
        self.logger = logger_handler.logger

    @abstractmethod
    def retrieve(self):
        """ Method to retrieve the data from the model """
        pass


class GenaiStrategy(SimpleStrategy):
    MODEL_FORMAT = "genai_retrieval"

    def __init__(self, **kwargs):
        super().__init__()

    def retrieve(self):
        """ Method to retrieve the data from the model """
        self.logger.info(f"Retrieving data from {GenaiStrategy.MODEL_FORMAT}")


class GenaiRecursiveStrategy(SimpleStrategy):
    MODEL_FORMAT = "genai_retrieval_recursive"

    def __init__(self, **kwargs):
        super().__init__()

    def retrieve(self):
        """ Method to retrieve the data from the model """
        self.logger.info(f"Retrieving data from {GenaiStrategy.MODEL_FORMAT}")


class GenaiSurroundingStrategy(SimpleStrategy):
    MODEL_FORMAT = "genai_retrieval_surrounding"

    def __init__(self, **kwargs):
        super().__init__()

    def retrieve(self):
        """ Method to retrieve the data from the model """
        self.logger.info(f"Retrieving data from {GenaiStrategy.MODEL_FORMAT}")


class LlamaIndexFusionStrategy(SimpleStrategy):
    MODEL_FORMAT = "llamaindex_fusion"

    def __init__(self, **kwargs):
        super().__init__()

    def retrieve(self):
        """ Method to retrieve the data from the model """
        self.logger.info(f"Retrieving data from {GenaiStrategy.MODEL_FORMAT}")

class ManagerRetrievalStrategies(object):
    RETRIEVAL_STRATEGIES = [GenaiStrategy, GenaiRecursiveStrategy, GenaiSurroundingStrategy, LlamaIndexFusionStrategy]

    @staticmethod
    def get_chunking_method(conf: dict) -> SimpleStrategy:
        """ Method to instantiate the parsers class: [Simple, Recursive, SurroundingContextWindow]

        :param conf: Chunking method configuration. Example:  {"type":"simple", ...}
        """
        for chunking_method in ManagerRetrievalStrategies.RETRIEVAL_STRATEGIES:
            chunking_method_type = conf.get('strategy')
            if chunking_method.is_method_type(chunking_method_type):
                conf.pop('strategy')
                return chunking_method(**conf)
        raise PrintableGenaiError(400, f"Retrieval strategy type doesnt exist {conf}. "
                         f"Possible values: {ManagerRetrievalStrategies.get_possible_retrieval_strategies()}")

    @staticmethod
    def get_possible_retrieval_strategies() -> List:
        """ Method to list the methods types: [Simple, Recursive, SurroundingContextWindow]"""
        return [rs.MODEL_FORMAT for rs in ManagerRetrievalStrategies.RETRIEVAL_STRATEGIES]
