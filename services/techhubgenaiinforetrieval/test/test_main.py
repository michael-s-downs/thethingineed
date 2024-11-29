### This code is property of the GGAO ###
# Native imports
import re, copy, json

# Installed imports
import pytest
from unittest.mock import patch, MagicMock
from llama_index.core import MockEmbedding
from llama_index.core.schema import NodeWithScore
from llama_index.core.schema import TextNode
import elasticsearch


# Local imports
from common.errors.genaierrors import PrintableGenaiError
from main import app, InfoRetrievalDeployment, manage_actions_get_elasticsearch, manage_actions_delete_elasticsearch
from elasticsearch_adaption import ElasticsearchStoreAdaption

ada_002_germany = {
    'embedding_model_name': 'ada-002-germany',
    'embedding_model': 'text-embedding-ada-002',
    'azure_api_version': '2022-12-01',
    'azure_deployment_name': 'ada-002-germany',
    'zone': 'techhubinc-GermanyWestCentral',
    'model_pool': ['ada-pool'],
    'platform': 'azure'
}


models_credentials = {"URLs": {"AZURE_EMBEDDINGS_URL": "https://$ZONE.openai.azure.com/"},
                    "api-keys": {"azure": {
                        "techhubinc-GermanyWestCentral": "test_key",
                        "techhubinc-AustraliaEast": "test_key"}}
                      }
vector_storages = [{
    'vector_storage_name': 'elastic-test',
    'vector_storage_type': 'elastic',
    'vector_storage_host': 'localhost',
    'vector_storage_port': 9200,
    'vector_storage_scheme': 'https',
    'vector_storage_username': 'test',
    'vector_storage_password': 'test'
}]

aws_credentials = {"access_key": "346545", "secret_key": "87968"}

documents_ada = [
    NodeWithScore(node=TextNode(text="Test", metadata={"filename": "test", "snippet_id": "0"}), score=0.5),
    NodeWithScore(node=TextNode(text="Test-2", metadata={"filename": "test-2", "snippet_id": "1"}), score=0.55),
    NodeWithScore(node=TextNode(text="Test-2", metadata={"filename": "test-2", "snippet_id": "1"}), score=0.7),
    NodeWithScore(node=TextNode(text="Test-4", metadata={"filename": "test-4", "snippet_id": "3"}), score=0.34)
]
documents_bm25 = [
    NodeWithScore(node=TextNode(text="Test", metadata={"filename": "test", "snippet_id": "0"}), score=0.23),
    NodeWithScore(node=TextNode(text="Test-2", metadata={"filename": "test-2", "snippet_id": "1"}), score=0.778),
    NodeWithScore(node=TextNode(text="Test-2", metadata={"filename": "test-2", "snippet_id": "1"}), score=0.72)
]

def get_ir_deployment():
    with patch('main.load_secrets') as mock_load_secrets:
        with patch('common.storage_manager.ManagerStorage.get_file_storage') as mock_get_file_storage:
            mock_load_secrets.return_value = models_credentials, vector_storages, aws_credentials
            storage_mock_object = MagicMock()

            storage_mock_object.get_available_pools.return_value = {'ada-pool': ['ada-002-germany']}
            storage_mock_object.get_available_embedding_models.return_value = [ada_002_germany]
            storage_mock_object.get_unique_embedding_models.return_value = ['text-embedding-ada-002']
            storage_mock_object.get_embedding_equivalences.return_value = {"bm25": "bm25",
                                                                           "text-embedding-ada-002": "ada-pool"}

            mock_get_file_storage.return_value = storage_mock_object
            return InfoRetrievalDeployment()

def get_connector():
    connector = MagicMock(scheme="https", host="localhost", port=9200, username="test", password="test")
    connector.exist_index.return_value = True
    connector.close.return_value = None
    connector.get_documents_filenames.return_value = ('finished', [{'filename': 'test', 'chunks': 1},
                                                                  {'filename': 'test2', 'chunks': 1}], 200)
    connector.get_documents.return_value = ('finished', ['doc1', 'doc2'], 200)
    magic_object = MagicMock(body={'failures': [], 'deleted': 2})
    magic_object.get.return_value = 2
    connector.delete_documents.return_value = magic_object
    connector.delete_index.return_value = True
    return connector

class TestMain:
    models = [
        {
            "alias": "test-ada",
            "embedding_model": "text-embedding-ada-002",
        },{
            "alias": "test-bm25",
            "embedding_model": "bm25"
        }
    ]

    headers = {
        'x-tenant': 'develop',
        'x-department': 'main',
        'x-reporting': '',
    }

    connector = get_connector()
    deployment = get_ir_deployment()

    def test_assert_correct_models(self):
        self.connector.exist_index.return_value = False
        with pytest.raises(PrintableGenaiError):
            InfoRetrievalDeployment.assert_correct_models("test", self.models, self.connector)

    def test_max_num_queue(self):
        assert self.deployment.max_num_queue == 1

    def test_get_bm25_vector_store(self):
        # Case wrong (the index have not been indexed)
        with pytest.raises(PrintableGenaiError):
            self.deployment.get_bm25_vector_store("test_index", self.connector, MagicMock())

        # Case correct
        self.connector.exist_index.return_value = True
        assert isinstance(self.deployment.get_bm25_vector_store("test_index", self.connector, MagicMock()), ElasticsearchStoreAdaption)

    def test_get_retrievers_arguments(self):
        models = [
            {
                "alias": "bm25",
                "embedding_model": "bm25"
            },
            {
                "alias": "ada",
                "embedding_model": "text-embedding-ada-002",
                "platform": "azure",
                "azure_deployment_name": "test",
                "api_key": "test",
                "azure_base_url": "test",
                "azure_api_version": "test"
            }
        ]
        with patch('main.get_embed_model') as mock_get_embed_model:
            mock_obj = MagicMock()
            mock_obj.get_query_embedding.return_value = [0.23424, 0.234234234, 0.455]
            mock_get_embed_model.return_value = mock_obj
            retrievers = self.deployment.get_retrievers_arguments(models, "test", MagicMock(), self.connector, "query")
            assert (isinstance(retrievers[0][0], ElasticsearchStoreAdaption) and isinstance(retrievers[0][1], MockEmbedding)
                    and retrievers[0][2] == "query" and retrievers[0][3] == "bm25--score")
            assert (isinstance(retrievers[1][0], ElasticsearchStoreAdaption) and isinstance(retrievers[1][1], MagicMock)
                    and len(retrievers[1][2]) == 3 and retrievers[1][3] == "text-embedding-ada-002--score")

    def test_get_default_models(self):
        models_with_credentials = self.deployment.get_default_models("test", self.connector)
        assert models_with_credentials[1]['alias'] == 'ada-002-germany'
        assert models_with_credentials[1]['api_key'] == 'test_key'

    def test_generate_llama_filters(self):
        list_filter = {
            "filename": ["a.pdf", "b.pdf"]
        }
        response = self.deployment.generate_llama_filters(list_filter)
        assert len(response.filters[0].filters) == 2
        assert str(response.filters[0].condition.value) == "or"
        assert str(response.condition.value) == "and"
        assert len(response.filters) == 1

        string_filter = {
            "filename": "a.pdf"
        }
        response = self.deployment.generate_llama_filters(string_filter)
        assert len(response.filters[0].filters) == 1
        assert str(response.filters[0].condition.value) == "or"
        assert str(response.condition.value) == "and"
        assert len(response.filters) == 1


    def test_init_exception(self):
        with patch('common.storage_manager.ManagerStorage.get_file_storage') as mock_get_file_storage:
            mock_get_file_storage.side_effect = Exception
            assert not hasattr(InfoRetrievalDeployment(), "available_models")

    def test_process_exception(self):
        with pytest.raises(PrintableGenaiError):
            self.deployment.process({})

    def test_genai_retrieval_strategy(self):
        with patch('llama_index.core.base.base_retriever.BaseRetriever.retrieve') as mock_retrieve:
            with patch('main.get_connector', return_value=self.connector):
                with patch('main.InfoRetrievalDeployment.get_retrievers_arguments') as mock_get_retrievers_arguments:
                    mock_retrieve.side_effect = [documents_bm25, documents_ada, documents_bm25, documents_ada]
                    json_input = {
                        "index_conf": {
                            "top_k": 3,
                            "filters": {},
                            "query": "query",
                            "index": "test",
                            "strategy": "genai_retrieval",
                            "rescoring_function": "mean",
                            "models": []
                        }
                    }
                    mock_get_retrievers_arguments.return_value = [
                        (MagicMock(), MagicMock(), "query", "bm25--score"),
                        (MagicMock(), MagicMock(), [0.1231, 0.2323], "ada--score")
                    ]
                    _, result, _ = self.deployment.process({**json_input, 'project_conf': copy.deepcopy(self.headers)})
                    rescored_docs = result['docs']
                    assert result['status_code'] == 200
                    assert rescored_docs[2]['meta']["bm25--score"] == 0
                    assert rescored_docs[0]['content'] == "Test-2"
                    assert rescored_docs[0]['meta']["ada--score"] == 0.55
                    assert len(rescored_docs) == 3
                    assert rescored_docs[1]['meta']["bm25--score"] == 0.23 and rescored_docs[1]['meta']["ada--score"] == 0.5

    def test_llamaindex_fusion_strategy(self):
        with patch('llama_index.core.base.base_retriever.BaseRetriever.retrieve') as mock_retrieve:
            with patch('main.get_connector', return_value=self.connector):
                with patch('main.InfoRetrievalDeployment.get_retrievers_arguments') as mock_get_retrievers_arguments:
                    mock_retrieve.side_effect = [documents_bm25, documents_ada, documents_bm25, documents_ada]
                    json_input = {
                        "index_conf": {
                            "top_k": 3,
                            "filters": {},
                            "query": "query",
                            "index": "test",
                            "strategy": "llamaindex_fusion",
                            "strategy_mode": "reciprocal_rerank",
                            "models": ["bm25", "ada-002-germany"]
                        }
                    }
                    mock_get_retrievers_arguments.return_value = [
                        (MagicMock(), MagicMock(), "query", "bm25--score"),
                        (MagicMock(), MagicMock(), [0.1231, 0.2323], "ada--score")
                    ]
                    _, result, _ = self.deployment.process({**json_input, 'project_conf': copy.deepcopy(self.headers)})
                    rescored_docs = result['docs']
                    assert result['status_code'] == 200
                    assert len(rescored_docs) == 3
                    assert rescored_docs[0]['content'] == "Test"
                    assert rescored_docs[0]['meta']["ada--score"] == 0.5
                    assert rescored_docs[1]['meta']["bm25--score"] == 0.778 and rescored_docs[1]['meta']["ada--score"] == 0.55

@pytest.fixture
def client():
    with patch('main.deploy', get_ir_deployment()):
        with app.test_client() as client:
            yield client

def test_retrieve_documents(client):
    # ok case
    body = {
        "index": "test",
        "filters": "{\"test_system_query_v\": {\"system\": \"$system\", \"user\": [{\"type\": \"text\", \"text\": \"Answer the question as youngster: \"},{\"type\": \"image_url\",\"image\": {\"url\": \"https://static-00.iconduck.com/assets.00/file-type-favicon-icon-256x256-6l0w7xol.png\",\"detail\": \"high\"}},\"$query\"]}}"
    }
    with patch('main.get_connector') as mock_get_connector:
        mock_get_connector.return_value = get_connector()
        response = client.post("/retrieve_documents", json=body)
        result = json.loads(response.text).get('result')
        assert response.status_code == 200
        assert result.get('docs') == ['doc1', 'doc2']

    # Not filters passed
    body = {
        "index": "test"
    }
    response = client.post("/retrieve_documents", json=body)
    result = json.loads(response.text)
    assert response.status_code == 400
    assert result.get('result') == "There must at least one filter"


def test_retrieve_documents_filenames(client):
    body = {
        "index": "test"
    }
    with patch('main.get_connector') as mock_get_connector:
        mock_get_connector.return_value = get_connector()
        response = client.post("/get_documents_filenames", json=body)
        result = json.loads(response.text).get('result')
        assert response.status_code == 200
        assert result.get('docs') == [{'filename': 'test', 'chunks': 1}, {'filename': 'test2', 'chunks': 1}]

    # Not index passed
    response = client.post("/get_documents_filenames", json={})
    result = json.loads(response.text)
    assert response.status_code == 400
    assert result.get('result') == "Missing parameter: index"


def test_get_models(client):
    response = client.get("/get_models", query_string={"platform": "azure", "zone": "dd"})
    result = json.loads(response.text)
    assert response.status_code == 400
    assert result.get('error_message') == "You must provide only one parameter between 'platform', 'pool', 'zone' and 'embedding_model' param"

    response = client.get("/get_models", query_string={"platform": "azure"})
    result = json.loads(response.text).get('result')
    assert response.status_code == 200
    assert len(result.get('models')) > 0


def test_delete_index(client):
    body = {
        "index": "test"
    }
    with patch('main.get_connector') as mock_get_connector:
        mock_get_connector.return_value = get_connector()
        response = client.post("/delete_index", json=body)
        result = json.loads(response.text).get('result')
        assert response.status_code == 200
        assert result == "Index 'test' deleted for '1' models"

def test_process(client):
    with patch('main.deploy.sync_deployment') as mock_post:
        mock_post.return_value = {"status_code": 200, "result": "success", "status": "finished"}
        json_input = {}
        response = client.post("/process", json=json_input, headers=copy.deepcopy(TestMain.headers))
        assert response.status_code == 200

def test_healthcheck(client):
    response = client.get("/healthcheck")
    response = json.loads(response.text)
    assert response.get('status') == 'Service available'

def test_delete_documents(client):
    body = {
        "index": "test",
        "delete": {
            "filename": ["test.pdf"]
        }
    }
    with patch('main.get_connector') as mock_get_connector:
        mock_get_connector.return_value = get_connector()
        response = client.post("/delete-documents", json=body)
        result = json.loads(response.text).get('result')
        assert response.status_code == 200
        assert result == "Documents that matched the filters were deleted for 'test'"


def test_manage_actions_get_elastic():
    with patch('main.deploy', get_ir_deployment()):
        # Index not found
        connector = MagicMock()
        connector.get_documents_filenames.side_effect = elasticsearch.NotFoundError(message="dd", meta=MagicMock(), body={})
        response, status_code = manage_actions_get_elasticsearch("test", "get_documents_filenames", {}, connector)
        assert status_code == 400
        assert response['result'] == "Index 'test' not found"

        # Operation not allowed
        response, status_code = manage_actions_get_elasticsearch("test", "wrong_operation", {}, connector)
        assert status_code == 400
        assert response['result'] == "Unsupported operation"

        #Exception during process
        connector.get_documents_filenames.side_effect = KeyError("d")
        response, status_code = manage_actions_get_elasticsearch("test", "get_documents_filenames", {}, connector)
        assert status_code == 400
        assert response['result'] == "Error processing operation 'get_documents_filenames': 'd'"


def test_manage_actions_delete_elastic():
    with patch('main.deploy', get_ir_deployment()):
        # Index not found
        connector = MagicMock()
        connector.delete_documents.side_effect = elasticsearch.NotFoundError(message="dd", meta=MagicMock(), body={})
        response, status_code = manage_actions_delete_elasticsearch("test", "delete-documents", {}, connector)
        assert status_code == 400
        assert response['result'] == "Documents not found for filters: {}"

        # Operation not allowed
        response, status_code = manage_actions_delete_elasticsearch("test", "wrong_operation", {}, connector)
        assert status_code == 400
        assert response['result'] == "Unsupported operation"

        #Exception during process
        connector.delete_documents.side_effect = KeyError("d")
        response, status_code = manage_actions_delete_elasticsearch("test", "delete-documents", {}, connector)
        assert status_code == 400
        assert response['result'] == "Error processing operation 'delete-documents': 'd'"

        # Failures during delete
        connector = MagicMock()
        magic_object = MagicMock(body={'failures': [{"test": "test"}], 'deleted': 0})
        magic_object.get.return_value = 0
        connector.delete_documents.return_value = magic_object
        response, status_code = manage_actions_delete_elasticsearch("test", "delete-documents", {}, connector)
        assert status_code == 400
        assert response['result'] == "Documents not found for filters: {}"

        # Documents not found
        connector = MagicMock()
        magic_object = MagicMock(body={'failures': [], 'deleted': 0})
        magic_object.get.return_value = 0
        connector.delete_documents.return_value = magic_object
        response, status_code = manage_actions_delete_elasticsearch("test", "delete-documents", {}, connector)
        assert status_code == 400
        assert response['result'] == "Documents not found for filters: {}"

        # Error deleting
        magic_object = MagicMock(body={'failures': [], 'deleted': -1})
        magic_object.get.return_value = -1
        connector.delete_documents.return_value = magic_object
        response, status_code = manage_actions_delete_elasticsearch("test", "delete-documents", {}, connector)
        assert status_code == 400
        assert re.match(r"Error deleting documents: \[<MagicMock name='mock.delete_documents\(\)' id='(?P<id>\d+)'>\]", response['result'])

        # Delete index index not found
        connector.delete_index.side_effect = elasticsearch.NotFoundError(message="dd", meta=MagicMock(), body={})
        response, status_code = manage_actions_delete_elasticsearch("test", "delete_index", {}, connector)
        assert status_code == 400
        assert response['result'] == "Index 'test' not found"
