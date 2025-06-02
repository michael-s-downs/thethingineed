### This code is property of the GGAO ###


# Native imports
import os
from typing import List
from abc import ABC

# Installed imports
import tiktoken
from transformers import GPT2TokenizerFast

# Local imports
from messages import Message
from common.services import GENAI_LLM_LIMITERS
from common.logging_handler import LoggerHandler
from common.errors.genaierrors import PrintableGenaiError


class QueryLimiter(ABC):
    MARGIN = 50
    MODEL_FORMAT = "QueryLimiter"

    def __init__(self, message: Message, model: str, max_tokens: int, bag_tokens: int,
                 persistence: List[dict] = None) -> None:
        """Modifies the messsage in order to limit the number of tokens.
           It will crop the context if necessary, if not cropped and persistance exists, it will delete the n first items in order to meet the condition
            
        :param message: Message like class
        :param max_tokens: Maximum numbre of tokens
        :param bag_tokens: Number of tokens reserved to generative models.
        :param persistence (Optional): If given indicates that a predecent conversation must me taken into account 
        """
        self.message = message
        self.persistence = persistence
        self.max_tokens = max_tokens
        self.bag_tokens = bag_tokens
        self.model = model
        self.num_images = 0
        self.max_images = 10

        self.encoding = tiktoken.get_encoding("cl100k_base")
        self.encoding = tiktoken.encoding_for_model("gpt-3.5-turbo")

        logger_handler = LoggerHandler(GENAI_LLM_LIMITERS, level=os.environ.get('LOG_LEVEL', "INFO"))
        self.logger = logger_handler.logger

    @staticmethod
    def _get_n_tokens(pair) -> int:
        """ Method to get the number of tokens of the message

        :param pair: List of messages
        :return: Number of tokens
        """
        # pop is done because is not possible to send it to the LLM
        n_tokens = 0
        for message in pair:
            if isinstance(message['content'], str):
                n_tokens += message.pop('n_tokens')
            elif isinstance(message['content'], list):
                for item in message['content']:
                    n_tokens += item.pop('n_tokens')
        return n_tokens

    @staticmethod
    def _get_num_images(pair) -> int:
        """ Method to get the number of images in the message

        :param pair: List of messages
        :return: Number of images
        """
        num_images = 0
        for message in pair:
            if isinstance(message.get('content'), list):
                for element in message.get('content'):
                    if element.get('type') in ["image", "image_url"]:  # for claude and chatgpt format
                        num_images += 1
                return num_images
        return num_images

    def _limit_message_tokens(self, message: Message, delta_token: int) -> Message:
        """ Method to reduce the length of the message

        :param message: Message object to be reduced.
        :param delta_token: Different token length between two texts.
        :return: Message object with the reduced length.
        """
        if delta_token - self.MARGIN > 0:
            encoded_input = self.encoding.encode(message.context)
            message.context = self.encoding.decode(encoded_input[:delta_token - self.MARGIN])
            self.logger.debug(f"Context has been limited to {delta_token - self.MARGIN} tokens")
        else:
            message.context = ""
        return message

    def _parse_message_base(self) -> Message:
        """Modifies the message and returns it with the number of tokens available in the model

        :return: Message object limited to the number of tokens
        """
        self.logger.info("Proceding without persistence")
        if self._get_num_images(self.message.substituted_query) > self.max_images:
            raise PrintableGenaiError(400, f"Too many images in request. Max is {self.max_images}.")
        tokens_api_call = self._get_n_tokens(self.message.substituted_query)
        max_tokens_with_bag = self.max_tokens - self.bag_tokens
        if tokens_api_call > max_tokens_with_bag and self.message.context:
            self.message = self._limit_message_tokens(self.message, max_tokens_with_bag)
        return self.message

    def _parse_message_persistence(self) -> Message:
        """Modifies the message and returns it with the number of tokens of the text

        :return: Message object limited to the number of tokens
        """

        self.logger.debug("Proceding persistence")
        images_query = self._get_num_images(self.message.substituted_query)
        if images_query > self.max_images:
            raise PrintableGenaiError(400, f"Too many images in request. Max is {self.max_images}.")
        self.num_images += images_query

        tokens_api_call = self._get_n_tokens(self.message.substituted_query)
        max_tokens_with_bag = self.max_tokens - self.bag_tokens
        if tokens_api_call > max_tokens_with_bag and self.message.context:
            self.message = self._limit_message_tokens(self.message, max_tokens_with_bag)
            tokens_api_call = max_tokens_with_bag

        for pair in reversed(self.message.persistence):
            images_message = self._get_num_images(pair)

            if images_message + self.num_images > self.max_images:
                self.message.persistence.remove(pair)
                self.logger.info(
                    f"Reached maximum images permitted. Persistence has been limited for images < {self.max_images}.")
            else:
                self.num_images += images_message

                tokens_persistence_i = self._get_n_tokens(pair)
                if tokens_api_call + tokens_persistence_i > max_tokens_with_bag - self.MARGIN:
                    self.message.persistence.remove(pair)
                    self.logger.debug(f"Pair {pair} was deleted")
                    continue
                tokens_api_call += tokens_persistence_i

        return self.message

    def get_message(self) -> Message:
        """ Method to return the message object processed

        :return: Message object
        """
        if self.message.multiprompt:
            return self._parse_message_persistence()
        else:
            return self._parse_message_base()

    @classmethod
    def is_limiter_type(cls, model_type: str):
        """It will chech if a limiter_type coincides with the model format
        """
        return model_type == cls.MODEL_FORMAT


class AzureQueryLimiter(QueryLimiter):
    MODEL_FORMAT = "azure"

    def __init__(self, message: Message, model: str, max_tokens: int, bag_tokens: int,
                 persistence: List[dict] = None) -> None:
        """ Class that limits the number of tokens for the Azure models

        :param message: Message like class
        :param model: Model name
        :param max_tokens: Maximum numbre of tokens
        :param bag_tokens: Number of tokens reserved to generative models.
        :param persistence: If given indicates that a predecent conversation must me taken into account
        """
        super().__init__(message, model, max_tokens, bag_tokens, persistence)
        self.max_images = 10


class BedrockQueryLimiter(QueryLimiter):
    MODEL_FORMAT = "bedrock"

    def __init__(self, message: Message, model: str, max_tokens: int, bag_tokens: int,
                 persistence: List[dict] = None) -> None:
        """ Class that limits the number of tokens for the claude model

        :param message: Message like class
        :param model: Model name
        :param max_tokens: Maximum numbre of tokens
        :param bag_tokens: Number of tokens reserved to generative models.
        :param persistence: If given indicates that a predecent conversation must me taken into account
        """
        super().__init__(message, model, max_tokens, bag_tokens, persistence)
        self.max_images = 20

class NovaQueryLimiter(QueryLimiter):
    MODEL_FORMAT = "nova"

    def __init__(self, message: Message, model: str, max_tokens: int, bag_tokens: int,
                 persistence: List[dict] = None) -> None:
        """ Class that limits the number of tokens for the claude model

        :param message: Message like class
        :param model: Model name
        :param max_tokens: Maximum numbre of tokens
        :param bag_tokens: Number of tokens reserved to generative models.
        :param persistence: If given indicates that a predecent conversation must me taken into account
        """
        super().__init__(message, model, max_tokens, bag_tokens, persistence)
        self.max_images = 20

    @staticmethod
    def _get_num_images(pair) -> int:
        """ Method to get the number of images in the message

        :param pair: List of messages
        :return: Number of images
        """
        num_images = 0
        for message in pair:
            if isinstance(message.get('content'), list):
                for element in message.get('content'):
                    if element.get('image'):  # for nova format (check as it must be by size)
                        num_images += 1
        return num_images

class VertexQueryLimiter(QueryLimiter):
    MODEL_FORMAT = "vertex"

    def __init__(self, message: Message, model: str, max_tokens: int, bag_tokens: int,
                 persistence: List[dict] = None) -> None:
        """ Class that limits the number of tokens for the claude model

        :param message: Message like class
        :param model: Model name
        :param max_tokens: Maximum numbre of tokens
        :param bag_tokens: Number of tokens reserved to generative models.
        :param persistence: If given indicates that a predecent conversation must me taken into account
        """
        super().__init__(message, model, max_tokens, bag_tokens, persistence)
        self.max_images = 20
    @staticmethod
    def _get_n_tokens(pair: List[dict]) -> int:
        """
        Method to get the number of tokens from the message list and modify it.

        :param pair: List of messages.
        :return: Total number of tokens.
        """
        total_tokens = 0

        for i, message in enumerate(pair):
            new_parts = []

            for part in message.get("parts", []):
                current_part = {}

                if "text" in part and isinstance(part["text"], dict):
                    total_tokens += part["text"].pop("n_tokens", 0)
                    current_part["text"] = part["text"]["content"]

                elif "inlineData" in part and isinstance(part["inlineData"], dict):
                    total_tokens += part["inlineData"].pop("n_tokens", 0)
                    current_part["inlineData"] = {
                        "data": part["inlineData"].get("data", ""),
                        "mimeType": part["inlineData"].get("mimeType", "")
                    }

                if current_part:
                    new_parts.append(current_part)

            message["parts"] = new_parts

        return total_tokens

    @staticmethod
    def _get_num_images(pair: List[dict]) -> int:
        """
        Method to get the number of images in the message.

        :param pair: List of messages.
        :return: Number of images.
        """
        num_images = 0
        for message in pair:
            for part in message.get("parts", []):
                if "inlineData" in part and isinstance(part["inlineData"], dict):
                    mime_type = part["inlineData"].get("mimeType", "")
                    if mime_type.startswith("image/"):
                        num_images += 1
        return num_images

class TsuzumiQueryLimiter(QueryLimiter):
    MODEL_FORMAT = "tsuzumi"

    def __init__(self, message: Message, model: str, max_tokens: int, bag_tokens: int,
                 persistence: List[dict] = None) -> None:
        """ Class that limits the number of tokens for the Azure models

        :param message: Message like class
        :param model: Model name
        :param max_tokens: Maximum numbre of tokens
        :param bag_tokens: Number of tokens reserved to generative models.
        :param persistence: If given indicates that a predecent conversation must me taken into account
        """
        super().__init__(message, model, max_tokens, bag_tokens, persistence)

class ManagerQueryLimiter(object):
    MODEL_TYPES = [AzureQueryLimiter, BedrockQueryLimiter, NovaQueryLimiter, VertexQueryLimiter, TsuzumiQueryLimiter, QueryLimiter]

    @staticmethod
    def get_limiter(conf: dict) -> QueryLimiter:
        """ Method to instantiate the limiter based on the platform class: [azure, bedrock]

        :param conf: Model configuration. Example:  {"querylimiter":"azure"}
        """
        for querylimiter in ManagerQueryLimiter.MODEL_TYPES:
            querylimiter_type = conf.get('querylimiter')
            if querylimiter.is_limiter_type(querylimiter_type):
                conf.pop('querylimiter')
                return querylimiter(**conf)
        raise PrintableGenaiError(400, f"QueryLimiter type doesnt exist {conf}. "
                         f"Possible values: {ManagerQueryLimiter.get_possible_querylimiters()}")

    @staticmethod
    def get_possible_querylimiters() -> List:
        """ Method to list the limiters: [azure, bedrock]

        :param conf: Limiter configuration. Example:  {"querylimiter":"azure"}
        """
        return [querylimiter.MODEL_FORMAT for querylimiter in ManagerQueryLimiter.MODEL_TYPES]
