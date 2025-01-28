### This code is property of the GGAO ###


# Native imports
import os
import re
import copy
import json
import random
from typing import List
from abc import ABC, abstractmethod

#Installed imports
import tiktoken

# Local imports
from common.services import GENAI_LLM_GENERATIVES
from common.logging_handler import LoggerHandler
from common.errors.genaierrors import PrintableGenaiError
from common.models_manager import BaseModelConfigManager
from message.messagemanager import ManagerMessages
from limiters import ManagerQueryLimiter

DEFAULT_STOP_MSG = "<|endoftext|>"

NOT_FOUND_MSG = "Not found"

EMPTY_TEMPLATE_MSG = "Template is empty"

USER_AND_SYSTEM_KEY_ALLOWED_MSG = "Template can only have user and system key"

USER_KEY_MANDATORY_MSG = "Template must contain the user key"

TEMPLATE_IS_NOT_DICT_MSG = "Template is not well formed, must be a dict {} structure"


class GenerativeModel(ABC):
    MODEL_MESSAGE = None
    DEFAULT_TEMPLATE_NAME = "system_query"

    def __init__(self, models_credentials, zone):
        """It is the object in charge of modifying whether the inputs and the outputs of the gpt models


        :param models_credentials: Credentials to use the model
        :param zone: zone in where the model is located
        """
        logger_handler = LoggerHandler(GENAI_LLM_GENERATIVES, level=os.environ.get('LOG_LEVEL', "INFO"))
        self.logger = logger_handler.logger
        self.api_key = models_credentials.get(zone, None)

    def set_message(self, config: dict):
        """Sets the message as an argument of the class
           It also modifies the message taking into account the number of tokens.

        :param config: Dict with the message to be used
        """
        config['message'] = self.MODEL_MESSAGE
        if hasattr(self, 'max_img_size_mb'):
            config['max_img_size_mb'] = self.max_img_size_mb
        message = ManagerMessages().get_message(config)
        queryLimiter = ManagerQueryLimiter.get_limiter({"message": message, "model": self.MODEL_MESSAGE,
                                                        "max_tokens": self.max_input_tokens,
                                                        "bag_tokens": self.bag_tokens,
                                                        "persistence": message.persistence, "querylimiter": "azure"})
        self.message = queryLimiter.get_message()

    @abstractmethod
    def get_result(self, response: dict) -> dict:
        """ Method to format the model response.

        :param response: Dict returned by  LLM endpoint.
        :return: Dict with the answer, tokens used and logprobs.
        """

    @classmethod
    def get_message_type(cls, message_type: str):
        """Check if the model_type is one of the possible ones.

        :param message_type: Type of the model
        :return: True if the message_type is correct, False otherwise
        """
        return message_type == cls.MODEL_MESSAGE