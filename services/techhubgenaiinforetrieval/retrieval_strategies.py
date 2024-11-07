### This code is property of the GGAO ###


# Native imports
from typing import List
from abc import ABC, abstractmethod
import os
import logging


# Installed imports
from llama_index.core.base.embeddings.base import BaseEmbedding
from llama_index.core import VectorStoreIndex
from llama_index.core.llms.mock import MockLLM
from llama_index.core.retrievers import QueryFusionRetriever
from llama_index.core.schema import QueryBundle
from llama_index.core.vector_stores import MetadataFilter, MetadataFilters, FilterCondition
from llama_index.core.schema import NodeWithScore


# Custom imports
from elasticsearch_adaption import \
ElasticsearchStoreAdaption  # Custom class that adapts the elasticsearch store to improve filters
# In the version 0.2.0 it's impossible to use multiple filters in the same query
from common.errors.genaierrors import PrintableGenaiError
from common.logging_handler import LoggerHandler
from common.services import RETRIEVAL_STRATEGIES
from rescoring import rescore_documents


class SimpleStrategy(ABC):
    STRATEGY_FORMAT = "ChunkingMethod"

    def __init__(self):
        log = logging.getLogger('werkzeug')
        log.disabled = True

        logger_handler = LoggerHandler(RETRIEVAL_STRATEGIES, level=os.environ.get('LOG_LEVEL', "INFO"))
        self.logger = logger_handler.logger

    @abstractmethod
    def do_retrieval_strategy(self, input_object, retrievers_arguments):
        """ Method to retrieve the data from the model """
        pass


    @staticmethod
    def get_ids_empty_scores(ids_dict: dict, all_ids: set):
        """Get the document ids that some models have not retrieved and thus don't have scores

        :param ids_dict (dict): Dictionary of ids that each of the models have retriever

        :return dict: ids that do not have scores
        """
        ids_empty_scores = {}
        for retriever_type, ids in ids_dict.items():
            # Difference between all ids and the ids that have been retrieved
            ids_empty_scores[retriever_type] = list(all_ids - set(doc.metadata['snippet_id'] for doc in ids))

        return ids_empty_scores

    @staticmethod
    def generate_llama_filters(filters):
        """ Generate llama filters

        :return: Llama filters
        """
        llama_filters = []
        for key, value in filters.items():
            if isinstance(value, list):
                multiple_filter = [MetadataFilter(key=key, value=v) for v in value]
            else:
                multiple_filter = [MetadataFilter(key=key, value=value)]
            llama_filters.append(MetadataFilters(filters=multiple_filter, condition=FilterCondition.OR))

        return MetadataFilters(filters=llama_filters, condition=FilterCondition.AND)


    @staticmethod
    def add_retrieved_document(docs: dict, retrieved_doc: NodeWithScore, retriever_type: str):
        """ Add a retrieved document to the list of documents

        :param docs: Dictionary of documents
        :param retrieved_doc: Retrieved document
        :param retriever_type: Type of retriever
        """
        if retrieved_doc.metadata['snippet_id'] not in docs:
            retrieved_doc.metadata[retriever_type] = retrieved_doc.score
            docs[retrieved_doc.metadata['snippet_id']] = retrieved_doc
        else:
            if retriever_type not in docs[retrieved_doc.metadata['snippet_id']].metadata:
                docs[retrieved_doc.metadata['snippet_id']].metadata[retriever_type] = retrieved_doc.score
                docs[retrieved_doc.metadata['snippet_id']].score += retrieved_doc.score

    @classmethod
    def is_strategy_type(cls, model_type):
        """Checks if a given model type is equel to the model format and thus it must be the one to use.
        """
        return model_type == cls.STRATEGY_FORMAT


class GenaiStrategy(SimpleStrategy):
    STRATEGY_FORMAT = "genai_retrieval"

    def __init__(self, **kwargs):
        super().__init__()

    def retrieve(self, vector_store: ElasticsearchStoreAdaption, embed_model: BaseEmbedding, retriever_type: str,
                 filters: dict, top_k: int, docs: dict, embed_query: list, query: str) -> list:
        """Retrieve documents from a retriever

        :param vector_store: Vector store to use in retrieval
        :param embed_model: Embed model to use in retrieval
        :param query: Query to retrieve
        :param filters: Filters to apply
        :param top_k: Number of documents to retrieve
        :param docs: Dictionary to store the retrieved documents
        :param embed_query: Embedding of the query

        :return: List of documents
        """

        vector_store_index = VectorStoreIndex.from_vector_store(vector_store=vector_store, embed_model=embed_model)
        retriever = vector_store_index.as_retriever(similarity_top_k=top_k, filters=self.generate_llama_filters(filters))

        if isinstance(embed_query, list):
            retrieved_docs = retriever.retrieve(QueryBundle(query_str=query, embedding=embed_query))
        else:
            #bm25 does not use embeddings
            retrieved_docs = retriever.retrieve(QueryBundle(query_str=query))
        self.logger.debug(f"{retriever_type} retrieved {len(retrieved_docs)} documents")

        for doc in retrieved_docs:
            self.add_retrieved_document(docs, doc, retriever_type)

        return retrieved_docs

    def do_retrieval_strategy(self, input_object, retrievers_arguments):
        docs_by_retrieval = {}
        unique_docs = {}
        for vector_store, embed_model, embed_query, retriever_type in retrievers_arguments:
            docs_tmp = self.retrieve(vector_store, embed_model, retriever_type, input_object.filters,
                                     input_object.top_k, unique_docs, embed_query, input_object.query)

            docs_by_retrieval[retriever_type] = docs_tmp

        # Complete the scores that have not been obtained till this point
        ids_incompleted_docs = self.get_ids_empty_scores(docs_by_retrieval, set(unique_docs.keys()))
        if sum([len(docs) for docs in docs_by_retrieval.values()]) > 0:
            self.logger.debug(f"Re-scoring with {', '.join(list(zip(*retrievers_arguments))[3])} retrievers")
            for vector_store, embed_model, embed_query, retriever_type in retrievers_arguments:
                input_object.filters['snippet_id'] = ids_incompleted_docs[retriever_type]
                top_k_new = len(ids_incompleted_docs[retriever_type])
                if top_k_new > 0:
                    self.retrieve(vector_store, embed_model, retriever_type, input_object.filters, top_k_new,
                                  unique_docs, embed_query, input_object.query)

        retrievers = [retriever_type for _, _, _, retriever_type in retrievers_arguments]
        for doc in unique_docs.values():
            for retriever_type in retrievers:
                if retriever_type not in doc.metadata:
                    # Sometimes bm25 does not retrieve all documents
                    doc.metadata[retriever_type] = 0
            doc.score /= len(retrievers_arguments)

        rescored_docs = rescore_documents(unique_docs, input_object.query, input_object.rescoring_function,
                                          self.MODEL_FORMATS, retrievers)

        sorted_docs = sorted(rescored_docs, key=lambda x: x.score, reverse=True)

        return sorted_docs

class GenaiRecursiveStrategy(SimpleStrategy):
    STRATEGY_FORMAT = "genai_retrieval_recursive"

    def __init__(self, **kwargs):
        super().__init__()

    def retrieve(self):
        """ Method to retrieve the data from the model """
        self.logger.info(f"Retrieving data from {GenaiStrategy.STRATEGY_FORMAT}")


class GenaiSurroundingStrategy(SimpleStrategy):
    STRATEGY_FORMAT = "genai_retrieval_surrounding"

    def __init__(self, **kwargs):
        super().__init__()

    def retrieve(self):
        """ Method to retrieve the data from the model """
        self.logger.info(f"Retrieving data from {GenaiStrategy.STRATEGY_FORMAT}")


class LlamaIndexFusionStrategy(SimpleStrategy):
    STRATEGY_FORMAT = "llamaindex_fusion"

    def __init__(self, **kwargs):
        super().__init__()

    def do_retrieval_strategy(self, input_object, retrievers_arguments):
        retrievers = []
        for vector_store, embed_model, _, retriever_type in retrievers_arguments:
            vector_store_index = VectorStoreIndex.from_vector_store(vector_store=vector_store, embed_model=embed_model)
            retrievers.append(vector_store_index.as_retriever(filters=self.generate_llama_filters(input_object.filters),
                                                              similarity_top_k=input_object.top_k + 1))
        retriever = QueryFusionRetriever(
            retrievers,
            llm=MockLLM(),
            mode=input_object.strategy_mode,
            similarity_top_k=input_object.top_k + 1,
            num_queries=1,  # set this to 1 to disable query generation
            use_async=True,
            verbose=True
        )
        return retriever.retrieve(input_object.query)

class ManagerRetrievalStrategies(object):
    RETRIEVAL_STRATEGIES = [GenaiStrategy, GenaiRecursiveStrategy, GenaiSurroundingStrategy, LlamaIndexFusionStrategy]

    @staticmethod
    def get_retrieval_strategy(conf: dict) -> SimpleStrategy:
        """ Method to instantiate the parsers class: [Simple, Recursive, SurroundingContextWindow]

        :param conf: Chunking method configuration. Example:  {"type":"simple", ...}
        """
        for chunking_method in ManagerRetrievalStrategies.RETRIEVAL_STRATEGIES:
            chunking_method_type = conf.get('strategy')
            if chunking_method.is_strategy_type(chunking_method_type):
                conf.pop('strategy')
                return chunking_method(**conf)
        raise PrintableGenaiError(400, f"Retrieval strategy type doesnt exist {conf}. "
                         f"Possible values: {ManagerRetrievalStrategies.get_possible_retrieval_strategies()}")

    @staticmethod
    def get_possible_retrieval_strategies() -> List:
        """ Method to list the methods types: [Simple, Recursive, SurroundingContextWindow]"""
        return [rs.STRATEGY_FORMAT for rs in ManagerRetrievalStrategies.RETRIEVAL_STRATEGIES]
