### This code is property of the GGAO ###


# Native imports
from typing import List
from string import Template
from abc import ABC, abstractmethod
import os

# Local imports
from common.services import GENAI_LLM_MESSAGES
from common.logging_handler import LoggerHandler
from common.errors.genaierrors import PrintableGenaiError
from adapters import ManagerAdapters

DEFAULT_SYSTEM_MSG = 'You are a helpful assistant'
VCONTEXT_NOT_ALLOWED = "Context param not allowed in vision models"

class Message(ABC):
    MODEL_FORMAT = "Message"

    def __init__(self):
        """Message object"""
        logger_handler = LoggerHandler(GENAI_LLM_MESSAGES, level=os.environ.get('LOG_LEVEL', "INFO"))
        self.logger = logger_handler.logger

    @staticmethod
    def _get_user_query_tokens(query: list) -> [int, List]:
        """Given a query it will return the number of tokens

        :param query: Query containing the user content
        :return: Number of tokens
        """
        n_tokens = []
        for message in query:
            if message.get('role') == "user":
                if isinstance(message['content'], str):
                    n_tokens.append(message.get('n_tokens'))
                elif isinstance(message['content'], list):
                    for item in message['content']:
                        n_tokens.append(item.get('n_tokens'))

        return n_tokens[0] if len(n_tokens) == 1 else n_tokens

    def unitary_persistence(self) -> List:
        """Given a persistence it will return a list with the messages in a unitary format.

        :return: List with the messages in a unitary format
        """
        unitary_persistence = []
        for pair in self.persistence:
            for message in pair:
                unitary_persistence.append(message)
        return unitary_persistence

    @abstractmethod
    def preprocess(self):
        """Given a query and a context it will return the text in the GPT model format. 
        """

    @classmethod
    def is_message_type(cls, message_type: str):
        """It will chech if a model_type coincides with the model format
        """
        return message_type == cls.MODEL_FORMAT

    def __repr__(self):
        return f'{{query:{self.query}, ' \
               f'context:{self.context}, ' \
               f'template_name:{self.template_name}, ' \
               f'template:{self.template}}}'
