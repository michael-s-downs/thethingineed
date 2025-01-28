
# Native imports
from typing import List
import copy

# Local imports
from common.errors.genaierrors import PrintableGenaiError
from models.gptmodel import ChatGPTModel, DalleModel, ChatGPTVision
from models.claudemodel import ChatClaudeModel, ChatClaudeVision
from models.llamamodel import LlamaModel
from models.novamodel import ChatNova, ChatNovaVision
from common.models_manager import BaseModelConfigManager
from generatives import GenerativeModel


class ManagerModel(object):
    MODEL_TYPES = [ChatGPTModel, ChatClaudeModel, DalleModel, ChatClaudeVision, ChatGPTVision, LlamaModel, ChatNova, ChatNovaVision]

    @staticmethod
    def find_model_in_available_models(model_in: str, available_models: List[dict]) -> dict:
        """ Method to find the model in the available models.

        :param model_in: Model name.
        :param available_models: List of available models.
        :return: Model configuration.
        """
        selected_model = None
        for model_conf in available_models:
            ## check if model is in the available models and update the configuration
            if model_conf['model'] == model_in:
                selected_model = copy.deepcopy(model_conf)
                break
        return selected_model


    @staticmethod
    def get_model(conf: dict, platform_name: str, available_pools: dict, manager_models_config: BaseModelConfigManager) -> GenerativeModel:
        """ Method to instantiate the model: [gpt3, gpt-3.5-turbo, gpt4]

        :param conf: Model configuration. Example:  {"max_tokens": 1000,"model": "gpt-3.5-turbo"}
        :param platform_name: Platform name.
        :param available_pools: List of available pools.
        :param manager_models_config: Model configuration manager.
        :return: Model object.
        """
        pool_name = None
        if conf.get('model') in available_pools:
            pool_name = conf.get('model')

        selected_model = manager_models_config.get_model(conf.get('model'), platform_name)
        selected_model['pool_name'] = pool_name
        ## check model message: chatGPT, chatGPT-v,....
        for model in ManagerModel.MODEL_TYPES:
            if model.get_message_type(selected_model.get('message')):
                conf.pop('model', None)
                selected_model.pop('message', None)
                selected_model.pop('model_pool', None)
                selected_model.update(conf)
                try:
                    return model(**selected_model)
                except TypeError as e:
                    raise PrintableGenaiError(400, f"Parameter:{str(e).split('argument')[1]} not supported in model: "
                                        f"'{selected_model.get('model')}'")