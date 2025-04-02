import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from common.errors.genaierrors import PrintableGenaiError
from elasticsearch_adaption import ElasticsearchStoreAdaption
from elasticsearch import AsyncElasticsearch
from azure.search.documents.indexes import SearchIndexClient as AzSearchIndexClient
from llama_index.vector_stores.azureaisearch import AzureAISearchVectorStore

from search_client import ElasticClient, AzureAiClient, ManagerSearchClient

class MockConnector:
    def __init__(self, model_format, host="localhost", port=9200, scheme="http", username="user", password="pass", credential=None):
        self.MODEL_FORMAT = model_format
        self.host = host
        self.port = port
        self.scheme = scheme
        self.username = username
        self.password = password
        self.credential = credential

@pytest.mark.asyncio
async def test_elastic_client():
    mock_connector = MockConnector("elastic")
    with patch("elasticsearch.AsyncElasticsearch", autospec=True) as mock_es:
        mock_es_instance = AsyncMock(spec=AsyncElasticsearch)
        mock_es.return_value = mock_es_instance
        client = ElasticClient(mock_connector, "test-index")
        
        assert isinstance(client.client, AsyncElasticsearch)
        
        with patch("elasticsearch_adaption.ElasticsearchStoreAdaption", autospec=True) as mock_store:
            mock_store_instance = MagicMock(spec=ElasticsearchStoreAdaption)
            mock_store.return_value = mock_store_instance
            store = client.create_store("test-index")
            assert store == mock_store_instance

        assert client.indexed_models_init() == ["bm25"]
        
        client.vector_store = MagicMock()
        client.close_vector_store()
        client.vector_store.close.assert_called_once()

@pytest.mark.asyncio
async def test_azure_ai_client():
    mock_connector = MockConnector("ai_search", credential=MagicMock())
    with patch("search_client.AzSearchIndexClient") as mock_az_client:
        mock_az_client_instance = MagicMock(spec=AzSearchIndexClient)
        mock_az_client.return_value = mock_az_client_instance
        client = AzureAiClient(mock_connector, "test-index")
        
        assert isinstance(client.client, AzSearchIndexClient)
        
        with patch("llama_index.vector_stores.azureaisearch.AzureAISearchVectorStore", autospec=True) as mock_store:
            mock_store_instance = MagicMock(spec=AzureAISearchVectorStore)
            mock_store.return_value = mock_store_instance
            store = client.create_store("test-index")
            assert store == mock_store_instance
        
        assert client.indexed_models_init() == []
        assert client.close_vector_store() is None

@pytest.mark.asyncio
async def test_manager_search_client():
    mock_connector_elastic = MockConnector("elastic")
    mock_connector_azure = MockConnector("ai_search", credential=MagicMock())
    mock_connector_invalid = MockConnector("invalid")
    
    with patch.object(ElasticClient, "is_connector_type", return_value=True):
        with patch("elasticsearch.AsyncElasticsearch", autospec=True):
            client = ManagerSearchClient.get_client(mock_connector_elastic, "test-index")
            assert isinstance(client, ElasticClient)
    
    with patch.object(AzureAiClient, "is_connector_type", return_value=True):
        with patch("azure.search.documents.indexes.SearchIndexClient", autospec=True):
            client = ManagerSearchClient.get_client(mock_connector_azure, "test-index")
            assert isinstance(client, AzureAiClient)
    
    with pytest.raises(PrintableGenaiError):
        ManagerSearchClient.get_client(mock_connector_invalid, "test-index")
    
    assert ManagerSearchClient.get_possible_clients() == [client.SEARCH_TYPE for client in ManagerSearchClient.SEARCH_TYPES]

@pytest.mark.asyncio
async def test_elastic_indexed_models_init():
    client = ElasticClient(MockConnector("elastic"), "test-index")
    assert client.indexed_models_init() == ["bm25"]

@pytest.mark.asyncio
async def test_elastic_close_vector_store():
    client = ElasticClient(MockConnector("elastic"), "test-index")
    client.vector_store = MagicMock()
    client.close_vector_store()
    client.vector_store.close.assert_called_once()

@pytest.mark.asyncio
async def test_azure_indexed_models_init():
    client = AzureAiClient(MockConnector("ai_search", credential=MagicMock()), "test-index")
    assert client.indexed_models_init() == []

@pytest.mark.asyncio
async def test_azure_close_vector_store():
    client = AzureAiClient(MockConnector("ai_search", credential=MagicMock()), "test-index")
    assert client.close_vector_store() is None