### This code is property of the GGAO ###
# Native imports
import re, copy, json
import subprocess
import sys
import os

# Installed imports
import pytest
from unittest.mock import patch, MagicMock
from llama_index.core import MockEmbedding
from llama_index.core.schema import NodeWithScore
from llama_index.core.schema import TextNode
import elasticsearch


# Local imports
from common.errors.genaierrors import PrintableGenaiError
from main import app, InfoRetrievalDeployment
from retrieval_strategies import SimpleStrategy
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
    connector.assert_correct_chunking_method = MagicMock(return_value=None)
    connector.list_indices.return_value = (['index1_model1', 'index2_model2', 'index2_model2'])
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
        vector_store, _ = self.deployment.get_bm25_vector_store("test_index", self.connector, MagicMock())
        assert isinstance(vector_store, ElasticsearchStoreAdaption)

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
        self.deployment.get_default_models(MagicMock(), self.connector)

    def test_generate_llama_filters(self):
        list_filter = {
            "filename": ["a.pdf", "b.pdf"]
        }
        response = SimpleStrategy.generate_llama_filters(list_filter)
        assert len(response.filters[0].filters) == 2
        assert str(response.filters[0].condition.value) == "or"
        assert str(response.condition.value) == "and"
        assert len(response.filters) == 1

        string_filter = {
            "filename": "a.pdf"
        }
        response = SimpleStrategy.generate_llama_filters(string_filter)
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
        with patch('retrieval_strategies.GenaiStrategy.do_retrieval_strategy') as mock_retrieve:
            with patch('main.get_connector', return_value=self.connector):
                with patch('main.InfoRetrievalDeployment.get_retrievers_arguments') as mock_get_retrievers_arguments:
                    mock_retrieve.return_value = documents_bm25
                    json_input = {
                        "indexation_conf": {
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
                    assert rescored_docs[1]['content'] == "Test-2"
                    assert rescored_docs[0]['score'] == 0.23
                    assert len(rescored_docs) == 3
                    assert rescored_docs[2]['meta']['filename'] == "test-2"

    def test_llamaindex_fusion_strategy(self):
        with patch('retrieval_strategies.LlamaIndexFusionStrategy.do_retrieval_strategy') as mock_retrieve:
            with patch('main.get_connector', return_value=self.connector):
                with patch('main.InfoRetrievalDeployment.get_retrievers_arguments') as mock_get_retrievers_arguments:
                    mock_retrieve.return_value = documents_bm25
                    json_input = {
                        "indexation_conf": {
                            "top_k": 3,
                            "filters": {},
                            "query": "query",
                            "index": "test",
                            "strategy": "llamaindex_fusion",
                            "strategy_mode": "reciprocal_rerank",
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
                    assert rescored_docs[1]['content'] == "Test-2"
                    assert rescored_docs[0]['score'] == 0.23
                    assert len(rescored_docs) == 3
                    assert rescored_docs[2]['meta']['filename'] == "test-2"


    def test_recursive_genai_retrieval_strategy(self):
        with patch('retrieval_strategies.GenaiRecursiveStrategy.do_retrieval_strategy') as mock_retrieve:
            with patch('main.get_connector', return_value=self.connector):
                with patch('main.InfoRetrievalDeployment.get_retrievers_arguments') as mock_get_retrievers_arguments:
                    mock_retrieve.return_value = documents_bm25
                    json_input = {
                        "indexation_conf": {
                            "top_k": 3,
                            "filters": {},
                            "query": "query",
                            "index": "test",
                            "strategy": "recursive_genai_retrieval",
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
                    assert rescored_docs[1]['content'] == "Test-2"
                    assert rescored_docs[0]['score'] == 0.23
                    assert len(rescored_docs) == 3
                    assert rescored_docs[2]['meta']['filename'] == "test-2"


    def test_surrounding_genai_retrieval_strategy(self):
        with patch('retrieval_strategies.GenaiSurroundingStrategy.do_retrieval_strategy') as mock_retrieve:
            with patch('main.get_connector', return_value=self.connector):
                with patch('main.InfoRetrievalDeployment.get_retrievers_arguments') as mock_get_retrievers_arguments:
                    mock_retrieve.return_value = documents_bm25
                    json_input = {
                        "indexation_conf": {
                            "top_k": 3,
                            "filters": {},
                            "query": "query",
                            "index": "test",
                            "strategy": "surrounding_genai_retrieval",
                            "rescoring_function": "mean",
                            "models": ["bm25"]
                        }
                    }
                    mock_get_retrievers_arguments.return_value = [
                        (MagicMock(), MagicMock(), "query", "bm25--score"),
                        (MagicMock(), MagicMock(), [0.1231, 0.2323], "ada--score")
                    ]
                    _, result, _ = self.deployment.process({**json_input, 'project_conf': copy.deepcopy(self.headers)})
                    rescored_docs = result['docs']
                    assert result['status_code'] == 200
                    assert rescored_docs[1]['content'] == "Test-2"
                    assert rescored_docs[0]['score'] == 0.23
                    assert len(rescored_docs) == 3
                    assert rescored_docs[2]['meta']['filename'] == "test-2"

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
    with patch('endpoints.get_connector') as mock_get_connector:
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
    assert result.get('error_message') == "There must be at least one filter"

def test_retrieve_documents_exceptions(client):
    body = {
        "index": "test",
        "filters": "{\"test_system_query_v\": {\"system\": \"$system\", \"user\": [{\"type\": \"text\", \"text\": \"Answer the question as youngster: \"},{\"type\": \"image_url\",\"image\": {\"url\": \"https://static-00.iconduck.com/assets.00/file-type-favicon-icon-256x256-6l0w7xol.png\",\"detail\": \"high\"}},\"$query\"]}}"
    }

    with patch('endpoints.get_connector') as mock_get_connector:
        mock_connector = MagicMock()
        mock_get_connector.return_value = mock_connector

        not_found_error = elasticsearch.NotFoundError(
            message="Index not found",
            meta={"status": 404, "request_id": "test_id"},
            body={"error": {"root_cause": [{"type": "index_not_found_exception", "reason": "Index not found"}]}}
        )
        mock_connector.get_documents.side_effect = not_found_error
        response = client.post("/retrieve_documents", json=body)
        result = json.loads(response.text)
        assert response.status_code == 400
        assert "Index 'test'" in result.get('error_message')

        mock_connector.get_documents.side_effect = Exception("Unexpected error")
        response = client.post("/retrieve_documents", json=body)
        result = json.loads(response.text)
        assert response.status_code == 400
        assert "Error processing operation: Unexpected error" in result.get('error_message')


def test_retrieve_documents_filenames(client):
    with patch('endpoints.get_connector') as mock_get_connector:
        mock_get_connector.return_value = get_connector()
        response = client.get("/get_documents_filenames?index=test")
        result = response.json.get('result')
        assert response.status_code == 200
        assert result.get('docs') == [{'filename': 'test', 'chunks': 1}, {'filename': 'test2', 'chunks': 1}]

    # Not index passed
    response = client.get("/get_documents_filenames")
    result = response.json
    assert response.status_code == 400
    assert result.get('error_message') == "Missing parameter: index"

def test_retrieve_documents_filenames_exceptions(client):
    with patch('endpoints.get_connector') as mock_get_connector:
        mock_connector = MagicMock()
        mock_get_connector.return_value = mock_connector

        # Excepción genérica
        mock_connector.get_documents_filenames.side_effect = Exception("Mocked exception")
        response = client.get("/get_documents_filenames?index=test")
        result = response.json
        assert response.status_code == 400
        assert result.get('error_message') == "Error processing operation: Mocked exception"

        # elasticsearch.NotFoundError
        not_found_error = elasticsearch.NotFoundError(
            message="Index not found",
            meta={"status": 404, "request_id": "test_id"},
            body={"error": {"root_cause": [{"type": "index_not_found_exception", "reason": "Index not found"}]}}
        )
        mock_connector.get_documents_filenames.side_effect = not_found_error
        response = client.get("/get_documents_filenames?index=test")
        result = response.json
        assert response.status_code == 400
        assert "Index 'test'" in result.get('error_message')



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
    with patch('endpoints.get_connector') as mock_get_connector:
        mock_get_connector.return_value = get_connector()
        response = client.delete("/delete_index?index=test")
        result = response.json.get('result')
        assert response.status_code == 200
        assert result == "Index 'test' deleted for '1' models"

def test_delete_index_exceptions(client):
    body = {
        "index": "test"
    }
    with patch('endpoints.get_connector') as mock_get_connector:

        mock_connector = MagicMock()
        mock_get_connector.return_value = mock_connector

        mock_connector.delete_index.side_effect = Exception("Mocked exception")
        response = client.delete("/delete_index?index=test")
        result = response.json
        assert response.status_code == 400
        assert result.get('error_message') == "Error processing delete index operation: Mocked exception"

        not_found_error = elasticsearch.NotFoundError(
            message="Index not found",
            meta={"status": 404, "request_id": "test_id"},
            body={"error": {"root_cause": [{"type": "index_not_found_exception", "reason": "Index not found"}]}}
        )
        mock_connector.delete_index.side_effect = not_found_error
        response = client.delete("/delete_index?index=test")
        result = response.json
        assert response.status_code == 400
        assert ("Index 'test' not found") in result.get('error_message')


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
    query_params = f"?index={body['index']}&filename={body['delete']['filename'][0]}"
    with patch('endpoints.get_connector') as mock_get_connector:
        mock_get_connector.return_value = get_connector()

        # Send the DELETE request with query parameters
        response = client.delete(f"/delete_documents{query_params}")
        result = response.json.get('result')
        assert response.status_code == 200
        assert result == "Documents that matched the filters were deleted for 'test'"


def test_delete_documents_exeception(client):
    body = {
        "index": "test",
        "delete": {
            "filename": ["test.pdf"]
        }
    }
    query_params = f"?index={body['index']}&filename={body['delete']['filename'][0]}"
    with patch('endpoints.get_connector') as mock_get_connector:

        mock_connector = MagicMock()
        mock_get_connector.return_value = mock_connector

        mock_connector.delete_documents.side_effect = Exception("Mocked exception")
        response = client.delete(f"/delete_documents{query_params}")
        result = response.json
        assert response.status_code == 400
        assert result.get('error_message') == "Error processing delete operation: Mocked exception"

        not_found_error = elasticsearch.NotFoundError(
            message="Index not found",
            meta={"status": 404, "request_id": "test_id"},
            body={"error": {"root_cause": [{"type": "index_not_found_exception", "reason": "Index not found"}]}}
        )
        mock_connector.delete_documents.side_effect = not_found_error
        response = client.delete(f"/delete_documents{query_params}")
        result = response.json
        assert response.status_code == 400
        assert "Documents not found for filters: {'filename': ['test.pdf']}" in result.get('error_message')

        mock_connector.delete_documents.side_effect = None
        mock_connector.delete_documents.return_value = MagicMock(
            body={"failures": [{"reason": "test failure"}], "deleted": 0}
        )
        response = client.delete(f"/delete_documents{query_params}")
        result = response.json
        assert response.status_code == 400
        assert "Documents not found for filters: {'filename': ['test.pdf']}" in result.get('error_message')

        mock_connector.delete_documents.return_value = MagicMock(
            body={"failures": [], "deleted": 0}
        )
        response = client.delete(f"/delete_documents{query_params}")
        result = response.json
        assert response.status_code == 400
        assert "Documents not found for filters: {'filename': ['test.pdf']}" in result.get('error_message')


def test_list_indices(client):
    with patch('endpoints.get_connector') as mock_get_connector:
        mock_get_connector.return_value = get_connector()
        response = client.get("/list_indices")
        result = json.loads(response.text)
        assert response.status_code == 200
        assert len(result.get('indices')) == 2
