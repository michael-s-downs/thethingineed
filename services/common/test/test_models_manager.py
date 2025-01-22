# Native imports
import re, copy, json, io

# Installed imports
import pytest
from unittest.mock import MagicMock, patch

# Local imports
from common.models_manager import BaseModelConfigManager, ManagerModelsConfig, EmbeddingsModelsConfigManager, LLMModelsConfigManager
from common.errors.genaierrors import PrintableGenaiError

gpt_model = {
    "model": "techhubinc-EastUS2-gpt-35-turbo-16k-0613",
    "model_type": "gpt-3.5-turbo-16k",
    "max_input_tokens": 16384,
    "zone": "techhubinc-EastUS2",
    "message": "chatGPT",
    "api_version": "2024-02-15-preview",
    "model_pool": ["techhubinc-pool-us-gpt-3.5-turbo-16k", "techhubinc-pool-world-gpt-3.5-turbo-16k"],
    "models_credentials": {}
}

gpt_v_model = {
    "model": "techhubinc-AustraliaEast-gpt-4o-2024-05-13",
    "model_type": "gpt-4o",
    "max_input_tokens": 128000,
    "zone": "techhubinc-AustraliaEast",
    "message": "chatGPT-v",
    "api_version": "2024-02-15-preview",
    "model_pool": ["techhubinc-pool-world-gpt-4o"],
    "models_credentials": {}
}

claude_model = {
    "model": "claude-v2:1-NorthVirginiaEast",
    "model_id": "anthropic.claude-v2:1",
    "model_type": "claude-v2.1",
    "max_input_tokens": 200000,
    "zone": "us-east-1",
    "message": "chatClaude",
    "api_version": "bedrock-2023-05-31",
    "model_pool": ["anthropic-pool-us-claude-v2.1", "anthropic-pool-world-claude-v2.1"],
    "models_credentials": {}
}


claude3_model = {
    "model": "claude-3-haiku-ParisWest",
    "model_id": "anthropic.claude-3-haiku-20240307-v1:0",
    "model_type": "claude-v3-haiku",
    "max_input_tokens": 200000,
    "zone": "eu-west-3",
    "message": "chatClaude-v",
    "api_version": "bedrock-2023-05-31",
    "model_pool": ["claude-v3-haiku-pool-europe", "claude-v3-haiku-pool-world"],
    "models_credentials": {}
}


available_models = {
    "azure": [copy.deepcopy(gpt_model), copy.deepcopy(gpt_v_model)],
    "bedrock": [copy.deepcopy(claude_model), copy.deepcopy(claude3_model)]
}

available_pools = {
    "anthropic-pool-us-claude-v2.1": [copy.deepcopy(claude_model)],
    "anthropic-pool-world-claude-v2.1": [copy.deepcopy(claude_model)],
    "claude-v3-haiku-pool-europe": [copy.deepcopy(claude3_model)],
    "claude-v3-haiku-pool-world": [copy.deepcopy(claude3_model)],
    "techhubinc-pool-us-gpt-3.5-turbo-16k": [copy.deepcopy(gpt_model)],
    "techhubinc-pool-world-gpt-3.5-turbo-16k": [copy.deepcopy(gpt_model)],
    "techhubinc-pool-world-gpt-4o": [copy.deepcopy(gpt_v_model)],
}

models_credentials = {"api-keys": {}, "URLs": {}}

class TestManagerModelsConfig:
    def test_get_posible_manager_model(self):
        ManagerModelsConfig.get_posible_manager_model() == ["llm", "embeddings"]

    def test_get_manager_model(self):
        conf={
            "available_models": {},
            "available_pools": {},
            "models_credentials": {},
            "type": "llm"
        }
        assert isinstance(ManagerModelsConfig.get_models_config_manager(conf), LLMModelsConfigManager)
        conf['type'] = "embeddings"
        assert isinstance(ManagerModelsConfig.get_models_config_manager(conf), EmbeddingsModelsConfigManager)
        conf['type'] = "wrong"
        with pytest.raises(PrintableGenaiError):
            ManagerModelsConfig.get_models_config_manager(conf)
class TestBaseModelConfigManager:
    base_model_config_manager = BaseModelConfigManager(available_pools, available_models, models_credentials)
    
    def test_get_model_api_key_by_zone(self):
        assert self.base_model_config_manager.get_model_api_key_by_zone("azure", "techhubinc-EastUS2-gpt-35-turbo-16k-0613") == None

    def test_is_model_type(self):
        assert not BaseModelConfigManager.is_manager_model_type("llm")

class TestLLMModelsConfigManager:
    llm_models_config_manager = LLMModelsConfigManager(available_pools, available_models, models_credentials)
    
    def test_get_model(self):
        assert self.llm_models_config_manager.get_model("techhubinc-EastUS2-gpt-35-turbo-16k-0613", "azure") == gpt_model
        with pytest.raises(PrintableGenaiError):
            self.llm_models_config_manager.get_model("techhubinc-EastUS2-gpt-35-turbo-16k-0613", "bedrock")
        assert self.llm_models_config_manager.get_model("techhubinc-pool-us-gpt-3.5-turbo-16k", "azure") == gpt_model

    def test_get_different_model_from_pool(self):
        self.llm_models_config_manager.get_different_model_from_pool("techhubinc-pool-us-gpt-3.5-turbo-16k", "old_model", "azure") == gpt_model
        self.llm_models_config_manager.get_different_model_from_pool("wrong", "techhubinc-EastUS2-gpt-35-turbo-16k-0613", "azure") == gpt_model


