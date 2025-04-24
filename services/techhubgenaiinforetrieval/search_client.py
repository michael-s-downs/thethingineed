### This code is property of the GGAO ###


# Native imports
from abc import ABC
from typing import List

from common.errors.genaierrors import PrintableGenaiError

from elasticsearch_adaption import ElasticsearchStoreAdaption  # Custom class that adapts the elasticsearch store to improve filters
from elasticsearch.helpers.vectorstore import AsyncBM25Strategy
from elasticsearch import AsyncElasticsearch
from azure.search.documents.indexes import SearchIndexClient as AzSearchIndexClient

from llama_index.vector_stores.azureaisearch import AzureAISearchVectorStore
            


class SearchClient(ABC):
    SEARCH_TYPE = "client"

    def __init__(self):
        self.connection = None

    @classmethod
    def is_connector_type(cls, model_type):
        """Checks if a given model type is equel to the model format and thus it must be the one to use.
        """
        return model_type == cls.SEARCH_TYPE

class ElasticClient(SearchClient):
    SEARCH_TYPE = "elastic"

    def __init__(self, connector, index_name):
        self.client = AsyncElasticsearch(hosts=f"{connector.scheme}://{connector.host}:{connector.port}",
                                           http_auth=(connector.username, connector.password),
                                           verify_certs=False, timeout=30)
    
    def create_store(self, index_name):
        self.vector_store = ElasticsearchStoreAdaption(index_name=index_name, es_client=self.client,
                                                          retrieval_strategy=AsyncBM25Strategy())
        return self.vector_store

    def indexed_models_init(self):
        return ["bm25"]

    def close_vector_store(self):
        self.vector_store.close()

class AzureAiClient(SearchClient):
    SEARCH_TYPE = "ai_search"

    def __init__(self, connector, index_name):
        self.client = AzSearchIndexClient(connector.host, connector.credential)

    def create_store(self, index_name):
        self.vector_store = AzureAISearchVectorStore(
                search_or_index_client=self.client,
                index_name=index_name,
                hidden_field_keys=["embedding"],
                id_field_key="id",
                chunk_field_key="content",
                embedding_field_key="embeddings",
                metadata_string_field_key="metadata",
                doc_id_field_key="doc_id",
                language_analyzer="en.lucene",
                vector_algorithm_type="exhaustiveKnn",
                semantic_configuration_name="mySemanticConfig",
                embedding_dimensionality=1536,
            )
        return self.vector_store

    def indexed_models_init(self):
        return []

    def close_vector_store(self):
        return


class ManagerSearchClient(object):
    SEARCH_TYPES = [ElasticClient, AzureAiClient]

    @staticmethod
    def get_client(connector, index_name:str) -> SearchClient:
        """ Method to instantiate the document store class: [elasticsearch]

        :param conf: Model configuration. Example:  {"vector_storage_type":"elastic"}
        """
        for client in ManagerSearchClient.SEARCH_TYPES:
            if client.is_connector_type(connector.MODEL_FORMAT):
                return client(connector, index_name)
        raise PrintableGenaiError(400, f"Vector database type doesnt exist {connector.MODEL_FORMAT}. "
                         f"Possible values: {ManagerSearchClient.get_possible_clients()}")

    @staticmethod
    def get_possible_clients() -> List:
        """ Method to list the clients: [elastic, azure_ai_search]
        """
        return [store.SEARCH_TYPE for store in ManagerSearchClient.SEARCH_TYPES]

