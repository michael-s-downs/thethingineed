### This code is property of the GGAO ###


# Native imports
from typing import List
from abc import ABC, abstractmethod
import os, json
import logging


# Installed imports
from llama_index.core.base.embeddings.base import BaseEmbedding
from llama_index.core import VectorStoreIndex
from llama_index.core.llms.mock import MockLLM
from llama_index.core.retrievers import QueryFusionRetriever
from llama_index.core.schema import QueryBundle, NodeWithScore, IndexNode, NodeRelationship
from llama_index.core.vector_stores import MetadataFilter, MetadataFilters, FilterCondition
from llama_index.core.postprocessor import MetadataReplacementPostProcessor
from llama_index.core.retrievers import RecursiveRetriever

# Custom imports
from elasticsearch_adaption import \
ElasticsearchStoreAdaption  # Custom class that adapts the elasticsearch store to improve filters
# In the version 0.2.0 it's impossible to use multiple filters in the same query
from common.errors.genaierrors import PrintableGenaiError
from common.logging_handler import LoggerHandler
from common.services import RETRIEVAL_STRATEGIES
from rescoring import rescore_documents
from common.utils import ELASTICSEARCH_INDEX
from common.indexing.connectors import Connector


class SimpleStrategy(ABC):
    STRATEGY_FORMAT = "SimpleStrategy"

    def __init__(self):
        log = logging.getLogger('werkzeug')
        log.disabled = True

        logger_handler = LoggerHandler(RETRIEVAL_STRATEGIES, level=os.environ.get('LOG_LEVEL', "INFO"))
        self.logger = logger_handler.logger

    @abstractmethod
    def do_retrieval_strategy(self, input_object, retrievers_arguments) -> List[NodeWithScore]:
        """ Method to retrieve the data from the model """
        pass

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

    @classmethod
    def is_strategy_type(cls, model_type):
        """Checks if a given model type is equel to the model format and thus it must be the one to use.
        """
        return model_type == cls.STRATEGY_FORMAT


class GenaiStrategy(SimpleStrategy):
    STRATEGY_FORMAT = "genai_retrieval"
    MODEL_FORMATS = {
        "bm25--score": "sparse"
    }

    def __init__(self):
        super().__init__()

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

    @staticmethod
    def get_ids_empty_scores(ids_dict: dict, all_ids: set):
        """Get the document ids that some models have not retrieved and thus don't have scores

        :param ids_dict: Dictionary of ids that each of the models have retriever
        :param all_ids: Set of all ids that should have been retrieved

        :return dict: ids that do not have scores
        """
        ids_empty_scores = {}
        for retriever_type, ids in ids_dict.items():
            # Difference between all ids and the ids that have been retrieved
            ids_empty_scores[retriever_type] = list(all_ids - set(doc.metadata['snippet_id'] for doc in ids))

        return ids_empty_scores

    def basic_genai_retrieval(self, vector_store: ElasticsearchStoreAdaption, embed_model: BaseEmbedding,
                              retriever_type: str, filters: dict, top_k: int, docs: dict, embed_query: list,
                              query: str) -> list:
        """Retrieve documents from a retriever

        :param vector_store: Vector store to use in retrieval
        :param embed_model: Embed model to use in retrieval
        :param retriever_type: Type of retriever
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

    def complete_empty_scores(self, docs_by_retrieval, unique_docs, retrievers_arguments, input_object, retrievers):
        """
        Complete the scores of the documents that have not been retrieved by all the retrievers using
        the basic_genai_retrieval method (simple retrieval) as is done with filters to get the exact score of a snippet

        :param docs_by_retrieval: Dictionary with the documents retrieved by each retriever
        :param unique_docs: Dictionary with the unique documents retrieved by all the retrievers
        :param retrievers_arguments: List of tuples with the arguments of the retrievers
        :param input_object: Object with the input data
        :param retrievers: List with the names of the retrievers
        """
        # Complete the scores that have not been obtained till this point for the embedding models
        ids_incompleted_docs = self.get_ids_empty_scores(docs_by_retrieval, set(unique_docs.keys()))
        if sum([len(docs) for docs in docs_by_retrieval.values()]) > 0:
            self.logger.debug(f"Re-scoring with {', '.join(list(zip(*retrievers_arguments))[3])} retrievers")
            for vector_store, embed_model, embed_query, retriever_type in retrievers_arguments:
                input_object.filters['snippet_id'] = ids_incompleted_docs[retriever_type]
                top_k_new = len(ids_incompleted_docs[retriever_type])
                if top_k_new > 0:
                    self.basic_genai_retrieval(vector_store, embed_model, retriever_type, input_object.filters, top_k_new,
                                  unique_docs, embed_query, input_object.query)

        # Set to 0 if a document has not been retrieved when completing the scores
        for doc in unique_docs.values():
            for retriever_type in retrievers:
                if retriever_type not in doc.metadata:
                    # Sometimes bm25 does not retrieve all documents
                    doc.metadata[retriever_type] = 0
            doc.score /= len(retrievers_arguments)

    def do_retrieval_strategy(self, input_object, retrievers_arguments) -> List[NodeWithScore]:
        """
        Method to retrieve the chunks from the connector, using the genai_strategy

        :param input_object: Object with the input data
        :param retrievers_arguments: List of tuples with the arguments of the retrievers

        :return: List of documents
        """
        docs_by_retrieval = {}
        unique_docs = {}
        retrievers = [retriever_type for _, _, _, retriever_type in retrievers_arguments]

        # First retrieval to get the documents by each retriever (embedding model)
        for vector_store, embed_model, embed_query, retriever_type in retrievers_arguments:
            docs_tmp = self.basic_genai_retrieval(vector_store, embed_model, retriever_type, input_object.filters,
                                     input_object.top_k, unique_docs, embed_query, input_object.query)

            docs_by_retrieval[retriever_type] = docs_tmp

        self.complete_empty_scores(docs_by_retrieval, unique_docs, retrievers_arguments, input_object, retrievers)

        rescored_docs = rescore_documents(unique_docs, input_object.query, input_object.rescoring_function,
                                          self.MODEL_FORMATS, retrievers)

        sorted_docs = sorted(rescored_docs, key=lambda x: x.score, reverse=True)

        return sorted_docs


class GenaiRecursiveStrategy(GenaiStrategy):
    STRATEGY_FORMAT = "recursive_genai_retrieval"

    def __init__(self, connector: Connector):
        super().__init__()
        self.connector = connector

    def do_retrieval_strategy(self, input_object, retrievers_arguments) -> List[NodeWithScore]:
        """
        Method to retrieve the chunks from the connector, using the genai_strategy

        :param input_object: Object with the input data
        :param retrievers_arguments: List of tuples with the arguments of the retrievers

        :return: List of documents
        """
        docs_by_retrieval = {}
        unique_docs = {}
        retrievers = [retriever_type for _, _, _, retriever_type in retrievers_arguments]

        for vector_store, embed_model, embed_query, retriever_type in retrievers_arguments:
            # Retriever type is formed by embedding_model (same used to name index) so split by '--' (other part is score)
            index_docs = self.connector.get_index(ELASTICSEARCH_INDEX(input_object.index, retriever_type.split('--')[0]))
            nodes_dict = self.get_all_nodes_dict(index_docs)
            docs_tmp = self.basic_genai_retrieval(vector_store, embed_model, retriever_type, input_object.filters,
                                     input_object.top_k, unique_docs, embed_query, input_object.query)

            docs_by_retrieval[retriever_type] = docs_tmp

        self.complete_empty_scores(docs_by_retrieval, unique_docs, retrievers_arguments, input_object, retrievers)

        rescored_docs = rescore_documents(unique_docs, input_object.query, input_object.rescoring_function,
                                          self.MODEL_FORMATS, retrievers)

        sorted_docs = sorted(rescored_docs, key=lambda x: x.score, reverse=True)

        return sorted_docs


    def get_all_nodes_dict(self, nodes) -> dict:
        """Get all nodes in a dictionary with the id as key

        :param nodes: List of nodes

        :return: Dictionary of nodes
        """
        for node in nodes:
            node_content_dict = json.loads(node['_source']['metadata']['_node_content'])
            embeddings = node['_source']['embedding']
            text = node['_source']['content']
            for type, content in node_content_dict['relationships'].items():
                a = NodeRelationship(type)
                if NodeRelationship(type) == NodeRelationship.SOURCE:
                    print("a")
                elif NodeRelationship(type) == NodeRelationship.PREVIOUS:
                    print("b")
                elif NodeRelationship(type) == NodeRelationship.NEXT:
                    print("c")
                elif NodeRelationship(type) == NodeRelationship.PARENT:
                    print("d")
                elif NodeRelationship(type) == NodeRelationship.CHILD:
                    print("E")
class GenaiSurroundingStrategy(GenaiStrategy):
    STRATEGY_FORMAT = "surrounding_genai_retrieval"

    def __init__(self):
        super().__init__()

    def do_retrieval_strategy(self, input_object, retrievers_arguments) -> List[NodeWithScore]:
        """
        Method to retrieve the chunks from the connector, using the genai_strategy

        :param input_object: Object with the input data
        :param retrievers_arguments: List of tuples with the arguments of the retrievers

        :return: List of documents
        """
        docs_by_retrieval = {}
        unique_docs = {}
        retrievers = [retriever_type for _, _, _, retriever_type in retrievers_arguments]

        for vector_store, embed_model, embed_query, retriever_type in retrievers_arguments:
            docs_tmp = self.basic_genai_retrieval(vector_store, embed_model, retriever_type, input_object.filters,
                                     input_object.top_k, unique_docs, embed_query, input_object.query)

            # Replace the surrounding window (window metadata key) in the retrieved documents before store
            MetadataReplacementPostProcessor(target_metadata_key="window").postprocess_nodes(docs_tmp)
            docs_by_retrieval[retriever_type] = docs_tmp

        # When completing, just scores metadata are modified
        self.complete_empty_scores(docs_by_retrieval, unique_docs, retrievers_arguments, input_object, retrievers)

        rescored_docs = rescore_documents(unique_docs, input_object.query, input_object.rescoring_function,
                                          self.MODEL_FORMATS, retrievers)

        sorted_docs = sorted(rescored_docs, key=lambda x: x.score, reverse=True)

        return sorted_docs

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
