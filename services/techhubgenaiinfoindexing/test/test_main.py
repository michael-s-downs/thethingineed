### This code is property of the GGAO ###
# Native imports
import re, copy, json

# Installed imports
import pytest
from unittest.mock import patch, MagicMock
import pandas as pd


# Local imports
from common.errors.genaierrors import PrintableGenaiError
from main import InfoIndexationDeployment

models_credentials = {"URLs": {"AZURE_EMBEDDINGS_URL": "https://$ZONE.openai.azure.com/"},
                    "api-keys": {"azure": {
                        "test": "test_key",
                        "test-2": "test_key"}}
                      }

vector_storages = [{
    "vector_storage_name": "elastic-test",
    "vector_storage_type": "elastic",
    "vector_storage_host": "host-test",
    "vector_storage_port": 9200,
    "vector_storage_scheme": "https",
    "vector_storage_username": "test",
    "vector_storage_password": "test"
}]

available_models = {
    "azure": [{
        "embedding_model_name": "ada-test-1",
        "embedding_model": "text-embedding-ada-002",
        "azure_api_version": "2022-12-01",
        "azure_deployment_name": "test-ada",
        "zone": "test",
        "model_pool": ["ada-test-002-pool"]
    }, {
        "embedding_model_name": "ada-test-2",
        "embedding_model": "text-embedding-ada-002",
        "azure_api_version": "2022-12-01",
        "azure_deployment_name": "test-ada",
        "zone": "test",
        "model_pool": ["ada-test-002-pool"]
    }]
}

available_pools = {
    "azure": {
        "text-embedding-ada-002": {
            "ada-test-002-pool": ["ada-test-002-1", "ada-test-002-2"]
        }
    }
}

aws_credentials = {"access_key": "346545", "secret_key": "87968"}


def get_indexing_deployment():
    with patch('main.load_secrets') as mock_load_secrets:
        with patch('common.storage_manager.ManagerStorage.get_file_storage') as mock_get_file_storage:
            mock_load_secrets.return_value = models_credentials, vector_storages, aws_credentials
            storage_mock_object = MagicMock()

            storage_mock_object.get_pools_per_embedding_model.return_value = available_pools
            storage_mock_object.get_available_embedding_models.return_value = available_models
            storage_mock_object.get_unique_embedding_models.return_value = ['text-embedding-ada-002']


            mock_get_file_storage.return_value = storage_mock_object
            return InfoIndexationDeployment()

def get_connector():
    connector = MagicMock(scheme="https", host="localhost", port=9200, username="test", password="test", MODEL_FORMAT="elastic")
    connector.assert_correct_index_conf = MagicMock(return_value=None)
    connector.close.return_value = None
    connector.connect.return_value = None
    return connector

class TestInfoIndexationDeployment():
    json_input = {
        "generic": {
            "project_conf": {
                "report_url": "test_url",
                "process_id": "ir_index_20240125_113850926508AMjg3z3i",
                "process_type": "ir_index",
                "department": "test",
                "csv": False
            },
            "preprocess_conf": {
                "layout_conf": {
                    "do_titles": True,
                    "do_tables": True,
                }
            },
            "index_conf": {
                "index": "test_indexing",
                "windows_overlap": 10,
                "windows_length": 300,
                "modify_index_docs": {},
                "models": [
                    {
                        "embedding_model": "text-embedding-ada-002",
                        "platform": "azure",
                        "alias": "ada-test-1"
                    }
                ],
                "vector_storage": "elastic-test"
            }
        },
        "specific": {
            "dataset": {
                "dataset_key": "ir_index_20240125_113850926508AMjg3z3i:ir_index_20240125_113850926508AMjg3z3i",
            },
            "paths": {
                "text": "test/infoindexing/data/indexes/ir_index_20240125_113850926508AMjg3z3i/txt/prodsimpl/docs/NOTA TECNICA RIESGO COLECTIVO producto 5.txt",
            }
        }
    }
    deployment = get_indexing_deployment()

    def test_max_num_queue(self):
        assert self.deployment.max_num_queue == 1

    def test_exception_init(self):
        with patch('common.storage_manager.ManagerStorage.get_file_storage') as mock_get_file_storage:
            mock_get_file_storage.side_effect = Exception("Error")
            assert not hasattr(InfoIndexationDeployment(), "available_pools")

    def test_file_storage_testing(self):
        with patch('common.storage_manager.ManagerStorage.get_file_storage') as mock_get_file_storage:
            with patch('common.ir.vector_storages.ManagerVectorDB.get_vector_database') as mock_get_vector_db:
                with patch('common.ir.connectors.ManagerConnector.get_connector') as mock_get_connector:
                    with patch('os.getenv') as mock_getenv:
                        with patch('main.update_full_status',
                                   side_effect=lambda redis_status, dataset_status_key, status_code, message:
                                   (redis_status, dataset_status_key, status_code, message)) as mock_update_full_status:
                            storage_manager = MagicMock()
                            storage_manager.get_specific_files.return_value = MagicMock(), []
                            mock_get_file_storage.return_value = storage_manager

                            vector_database = MagicMock()
                            vector_database.get_processed_data.return_value = ["doc1-test"]
                            vector_database.index_documents.return_value = [{'ir_index/test-embedding-model/pages': {'num': 14, 'type': 'PAGS'},
                              'ir_index/test-embedding-model/tokens': {'num': 9391, 'type': 'TOKENS'}}]
                            mock_get_vector_db.return_value = vector_database

                            mock_get_connector.return_value = get_connector()
                            mock_getenv.return_value = "True"
                            must_continue, _, output_queue = self.deployment.process(self.json_input)
                            indexation_response = mock_update_full_status.call_args[0][1:]

                            assert indexation_response[1] == 200
                            assert indexation_response[2] == "Indexing finished"
    def test_exception_process(self):
        with patch('common.ir.parsers.ManagerParser.get_parsed_object') as mock_parsers:
            with patch('main.update_full_status',
                       side_effect=lambda redis_status, dataset_status_key, status_code, message:
                       (redis_status, dataset_status_key, status_code, message)) as mock_update_full_status:
                mock_parsers.side_effect = Exception("Error")
                self.deployment.process(self.json_input)
                indexation_response = mock_update_full_status.call_args[0][1:]

                assert indexation_response[1] == 500
                assert indexation_response[2] == "Error"

    def test_process(self):
        with patch('common.storage_manager.ManagerStorage.get_file_storage') as mock_get_file_storage:
            with patch('common.ir.vector_storages.ManagerVectorDB.get_vector_database') as mock_get_vector_db:
                with patch('common.ir.connectors.ManagerConnector.get_connector') as mock_get_connector:
                    with patch('main.update_full_status', side_effect=lambda redis_status, dataset_status_key, status_code, message:
                      (redis_status, dataset_status_key, status_code, message)) as mock_update_full_status:
                        storage_manager = MagicMock()
                        storage_manager.get_specific_files.return_value = MagicMock(),  []
                        mock_get_file_storage.return_value = storage_manager

                        vector_database = MagicMock()
                        vector_database.get_processed_data.return_value = ["doc1-test"]
                        vector_database.index_documents.return_value = [{'ir_index/test-embedding-model/pages': {'num': 14, 'type': 'PAGS'},
                          'ir_index/test-embedding-model/tokens': {'num': 9391, 'type': 'TOKENS'}}]
                        mock_get_vector_db.return_value = vector_database

                        mock_get_connector.return_value = get_connector()

                        self.deployment.process(self.json_input)
                        indexation_response = mock_update_full_status.call_args[0][1:]

                        assert indexation_response[1] == 200
                        assert indexation_response[2] == "Indexing finished"

