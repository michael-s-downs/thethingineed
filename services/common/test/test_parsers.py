### This code is property of the GGAO ###

import copy
# Native imports
import os, platform, json

# Installed imports
import pytest
from unittest.mock import MagicMock, patch
from unittest import mock

# Local imports
from common.ir.parsers import ManagerParser, ParserInforetrieval, ParserInfoindexing
from common.errors.genaierrors import PrintableGenaiError





models_credentials = {
    "URLs": {
		"AZURE_EMBEDDINGS_URL": "https://$ZONE.openai.azure.com/"
    },
    "api-keys": {
        "azure": {
            "test": "test-key"
        }
    }
}


class TestManagerParsers:
    conf = {'type': "IRStorage", 'workspace': "test", 'origin': "test"}

    def test_get_possible_managers(self):
        platforms = ManagerParser.get_possible_platforms()
        assert platforms == ["infoindexing", "inforetrieval"]

    def test_wrong_manager(self):
        self.conf['type'] = "nonexistent"
        with pytest.raises(PrintableGenaiError):
            ManagerParser.get_parsed_object(self.conf)

class TestParserInforetrieval:
    json_input = {
        "generic": {
            "index_conf": {
                "index": "test",
                "rescoring_function": "posnorm",
                "strategy": "genai_retrieval",
                "query": "Señores Ponce, Diez, Campos y Alvera?",
                "top_k": 5,
                "filters": {
                },
                "models": [
                ],
                "vector_storage_conf": {}
            }
        },
        "project_conf":  {
            "x-reporting": "test_config"
        }
    }

    available_models = [{
            "embedding_model_name": "ada-test-1",
            "embedding_model": "text-embedding-ada-002",
            "azure_api_version": "2022-12-01",
            "azure_deployment_name": "test-ada",
            "zone": "test",
            "platform": "azure",
            "model_pool": ["ada-test-pool"]
        }, {
            "embedding_model_name": "ada-test-2",
            "embedding_model": "text-embedding-ada-002",
            "azure_api_version": "2022-12-01",
            "azure_deployment_name": "test-ada",
            "zone": "test",
            "platform": "azure",
            "model_pool": ["ada-test-pool"]
        }
    ]

    available_pools = {
        "ada-test-pool": ["ada-test-1", "ada-test-2"],
    }

    conf = {
        'type': 'inforetrieval',
        'json_input': json_input,
        'available_pools': available_pools,
        'available_models': available_models,
        'models_credentials': models_credentials
    }

    def test_parse(self):
        # Models empty
        conf_input = copy.deepcopy(self.conf)
        retrieval_object = ManagerParser.get_parsed_object(conf_input)
        assert isinstance(retrieval_object, ParserInforetrieval)

        # Models passed and not generic
        conf_input = copy.deepcopy(self.conf)
        conf_input['json_input']['index_conf'] = conf_input['json_input']['generic'].pop('index_conf')
        conf_input['json_input'].pop('generic')
        conf_input['json_input']['index_conf']['models'] = ["bm25", "ada-test-1"]
        retrieval_object = ManagerParser.get_parsed_object(conf_input)
        assert isinstance(retrieval_object, ParserInforetrieval)

class TestParserInfoindexing:
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
                        "alias": "ada-test-pool"
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
            "model_pool": ["ada-test-pool"]
        }, {
            "embedding_model_name": "ada-test-2",
            "embedding_model": "text-embedding-ada-002",
            "azure_api_version": "2022-12-01",
            "azure_deployment_name": "test-ada",
            "zone": "test",
            "model_pool": ["ada-test-pool"]
        }]
    }

    available_pools = {
        "azure": {
            "ada-test-pool": ["ada-test-1", "ada-test-2"],
        }
    }

    conf = {
        'type': 'infoindexing',
        'json_input': json_input,
        'vector_storages': vector_storages,
        'available_pools': available_pools,
        'available_models': available_models,
        'models_credentials': models_credentials
    }

    def test_parse(self):
        conf_input = copy.deepcopy(self.conf)
        retrieval_object = ManagerParser.get_parsed_object(conf_input)
        assert isinstance(retrieval_object, ParserInfoindexing)
