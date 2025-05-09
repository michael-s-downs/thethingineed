### This code is property of the GGAO ###


# Native imports
from abc import ABC
from typing import List
import copy
import os
import random

# Installed imports


# Custom imports
from common.logging_handler import LoggerHandler
from common.services import MANAGER_MODELS
from common.errors.genaierrors import PrintableGenaiError


class BaseModelConfigManager(ABC):
    MODEL_FORMAT = None

    def __init__(self, available_pools, available_models, models_credentials, **kwargs):
        self.available_pools = available_pools
        self.available_models = available_models
        self.models_credentials = models_credentials

        logger_handler = LoggerHandler(MANAGER_MODELS, level=os.environ.get('LOG_LEVEL', "INFO"))
        self.logger = logger_handler.logger

    def get_model(self, model_name: str, platform) -> dict:
        """ Method to get a model configuration

        :param model_name: Model name
        :param platform: Platform
        """
        raise NotImplementedError
    
    def get_model_data(self, model_name: str, platform) -> dict:
        """ Method to get the model data

        :param model_name: Model name
        :param platform: Platform
        """
        return
    
    def get_model_from_pool(self, pool_name: str, platform) -> dict:
        """ Method to get a model configuration from a pool

        :param pool_name: Pool name
        :param platform: Platform
        """
        raise NotImplementedError
    
    def get_different_model_from_pool(self, pool_name: str, used_model_name: str, platform) -> dict:
        """ Method to get a different model configuration from a pool

        :param model_name: Model name
        :param used_model_name: Used model name
        :param platform: Platform
        """
        raise NotImplementedError
    
    def get_model_api_key_by_zone(self, zone: str, platform_name) -> str:
        """ Method to get the model API key

        :param zone: Zone
        :param platform_name: Platform name
        """
        return self.models_credentials.get('api-keys', {}).get(platform_name, {}).get(zone, None)

    
    @staticmethod
    def is_manager_model_type(model_type: str) -> bool:
        """ Method to check if the model type matches the manager's model type

        :param model_type: Model type
        """
        return model_type == BaseModelConfigManager.MODEL_FORMAT
    
class LLMModelsConfigManager(BaseModelConfigManager):
    MODEL_FORMAT = "llm"

    def get_model(self, model_name: str, platform: str) -> dict:
        model = self.get_model_from_pool(model_name, platform)
        if not model:
            model = self.get_model_data(model_name, platform)
        if model:
            return model
        else:
            raise PrintableGenaiError(400, f"Model {model_name} not found in the available models for platform {platform}")
        
    def get_model_data(self, model_name: str, platform) -> dict:
        selected_model = None
        for model_conf in self.available_models.get(platform, []):
            ## check if model is in the available models and update the configuration
            if model_conf['model'] == model_name:
                selected_model = copy.deepcopy(model_conf)
                break
        return selected_model

    def get_model_from_pool(self, pool_name: str, platform) -> dict:
        if pool_name not in self.available_pools:
            return None
        else:
            model = copy.deepcopy(random.choice(self.available_pools[pool_name]))
            self.logger.debug(f"Model selected from pool: {model.get('model')}")
            return model

    def get_different_model_from_pool(self, pool_name: str, used_model_name: str, platform) -> dict:
        if pool_name not in self.available_pools:
            return None
        else:
            selected_model = copy.deepcopy(random.choice(self.available_pools[pool_name]))
            if selected_model.get('model') == used_model_name:
                return self.get_different_model_from_pool(pool_name, used_model_name, platform)
            else:
                self.logger.debug(f"Model selected from pool: {selected_model.get('model')}")
                return selected_model

    @staticmethod
    def is_manager_model_type(model_type: str) -> bool:
        return model_type == LLMModelsConfigManager.MODEL_FORMAT


class EmbeddingsModelsConfigManager(BaseModelConfigManager):
    MODEL_FORMAT = "embeddings"

    def get_model(self, model_name: str, platform) -> dict:
        # Implementation to get a model configuration for Embeddings
        pass

    def get_model_data(self, model_name: str, platform) -> dict:
        """ Method to get the model data

        :param model_name: Model name
        """
        return

    def get_model_from_pool(self, pool_name: str, platform) -> dict:
        # Implementation to get a model configuration from a pool for Embeddings
        pass

    def get_different_model_from_pool(self, model_name: str, used_model_name: str, platform) -> dict:
        # Implementation to get a different model configuration from a pool for Embeddings
        pass

    @staticmethod
    def is_manager_model_type(model_type: str) -> bool:
        return model_type == EmbeddingsModelsConfigManager.MODEL_FORMAT

class ManagerModelsConfig(object):
    MODEL_TYPES = [LLMModelsConfigManager, EmbeddingsModelsConfigManager]

    @staticmethod
    def get_models_config_manager(conf: dict) -> BaseModelConfigManager:
        """ Method to instantiate the models manager class: [LLMModelsConfigManager, EmbeddingsModelsConfigManager]

        :param conf: Loader configuration. Example:  {"type":"embeddings"}
        """
        for manager in ManagerModelsConfig.MODEL_TYPES:
            manager_type = conf.get('type')
            if manager.is_manager_model_type(manager_type):
                conf.pop('type')
                return manager(**conf)
        raise PrintableGenaiError(400, f"Manager type doesnt exist {conf}. "
                         f"Possible values: {ManagerModelsConfig.get_posible_manager_model()}")

    @staticmethod
    def get_posible_manager_model() -> List:
        """ Method to list the models manager: [LLMModelsConfigManager, EmbeddingsModelsConfigManager]

        :param conf: Model configuration. Example:  {"type":"llm"}
        """
        return [manager.MODEL_FORMAT for manager in ManagerModelsConfig.MODEL_TYPES]