### This code is property of the GGAO ###


# Native imports
import os
from abc import ABC
from typing import List, Tuple
import re
import io
import base64
from PIL import Image
from math import ceil

# Installed imports
import tiktoken
from transformers import GPT2TokenizerFast
import requests

# Local imports
from common.services import GENAI_LLM_ADAPTERS
from common.logging_handler import LoggerHandler
from common.errors.genaierrors import PrintableGenaiError


class BaseAdapter(ABC):
    ADAPTER_FORMAT = "base"

    # Adapter for the models with only text type
    def __init__(self, message):
        """ Constructor for the BaseAdapter class

        :param message: Message like class
        """
        logger_handler = LoggerHandler(GENAI_LLM_ADAPTERS, level=os.environ.get('LOG_LEVEL', "INFO"))
        self.logger = logger_handler.logger
        self.message = message
        self.encoding = tiktoken.get_encoding("cl100k_base")
        self.encoding = tiktoken.encoding_for_model("gpt-3.5-turbo")

        preprocessed_message = self.message.preprocess()
        self.message.substituted_query = [preprocessed_message[0], preprocessed_message[-1]]

    def adapt_query_and_persistence(self):
        """ Method to add the number of tokens to the message"""
        self._adapt_messages(self.message.substituted_query)

        for pair in self.message.persistence:
            self._adapt_messages(pair)

    def _adapt_messages(self, messages):
        """ Method to add the number of tokens to the messages

        :param messages: List of messages
        """
        for message in messages:
            if isinstance(message['content'], str) and not message.get('n_tokens'):
                message['n_tokens'] = len(self.encoding.encode(message['content']))
            elif isinstance(message['content'], list):
                for item in message['content']:
                    if item.get('n_tokens'):
                        continue
                    elif item['type'] == "text":
                        item['n_tokens'] = len(self.encoding.encode(item['text']))
                    elif item['type'] == "image_url":
                        item['n_tokens'] = self._adapt_image(item)

    def _adapt_image(self, item):
        """ Method to get the number of tokens from an image"""
        pass

    @staticmethod
    def resize_image(width, height, width_resize, height_resize) -> Tuple[int, int]:
        """ Resize an image to have the correct format in order to count tokens

        :param width: Width of the image
        :param height: Height of the image
        :param width_resize: Width to resize to
        :param height_resize: Height to resize to
        :return: tuple - Width and height of the resized image
        """
        if width > width_resize or height > height_resize:
            if width > height:
                height = int(height * height_resize / width)
                width = width_resize
            else:
                width = int(width * width_resize / height)
                height = height_resize
        return width, height

    @classmethod
    def is_adapter_type(cls, adapter_type: str):
        """It will chech if a adapter_type coincides with the model format
        """
        return adapter_type == cls.ADAPTER_FORMAT


class GPT4VAdapter(BaseAdapter):
    ADAPTER_FORMAT = "gpt4v"

    def __init__(self, message):
        """ Constructor for the ClaudeAdapter class

        :param message: Message like class
        """
        super().__init__(message)

    def _adapt_messages(self, messages):
        """ Method to add the number of tokens to the messages

        :param messages: List of messages
        """
        for message in messages:
            if isinstance(message['content'], str) and not message.get('n_tokens'):
                message['n_tokens'] = len(self.encoding.encode(message['content']))
            elif isinstance(message['content'], list):
                for item in message['content']:
                    if item.get('n_tokens') and item['type'] in ["image_url", "image_b64"]:
                        self._adapt_image(item)
                    elif item.get('n_tokens'):
                        continue
                    elif item['type'] == "text":
                        item['n_tokens'] = len(self.encoding.encode(item['text']))
                    elif item['type'] in ["image_url", "image_b64"]:
                        item['n_tokens'] = self._adapt_image(item)

    def _adapt_image(self, image):
        """ Method to get tokens from an image and adapt it to gpt4v format

        :param image: Image to get tokens from
        :return: int - Number of tokens
        """
        if image['type'] == "image_url":
            content = image['image']['url']
            try:
                img = Image.open(io.BytesIO(requests.get(content).content))
            except:
                raise PrintableGenaiError(400, "Image must be a valid url format")
        elif image['type'] == "image_b64":
            content = image['image']['base64']
            try:
                img = Image.open(io.BytesIO(base64.decodebytes(bytes(content, "utf-8"))))
            except:
                raise PrintableGenaiError(400, "Image must be a valid base64 format")

        if image['image'].get('detail') == "low":
            total = 85
        else:
            width, height = self.resize_image(img.width, img.height, 1024, 1024)
            h = ceil(height / 512)
            w = ceil(width / 512)
            total = 85 + 170 * h * w

        media_type = img.format
        if media_type not in ['JPEG', 'PNG', 'GIF', 'WEBP']:
            raise PrintableGenaiError(400, "Image must be in format [jpeg, png, gif, webp]")

        # Format change to gpt4v
        if image['type'] == "image_b64":
            content = "data:image/" + media_type.lower() + ";base64," + content
        image['type'] = "image_url"
        if image['image'].get('detail'):
            image['image_url'] = {'url': content, 'detail': image['image']['detail']}
        else:
            image['image_url'] = {'url': content}
        image.pop('image')

        return total


class DalleAdapter(BaseAdapter):
    ADAPTER_FORMAT = "dalle"

    def __init__(self, message):
        """ Constructor for the DalleAdapter class

        :param message: Message like class
        """
        super().__init__(message)

    def adapt_query_and_persistence(self):
        """ Method to add the number of tokens to the message"""
        super().adapt_query_and_persistence()
        dalle_persistence = []
        for pair in self.message.persistence:
            for item in pair:
                if item['role'] == "user":
                    # item in a list to keep the same format as the other adapters
                    dalle_persistence.append([item])
        self.message.persistence = dalle_persistence

    @classmethod
    def is_adapter_type(cls, adapter_type: str):
        """It will chech if a adapter_type coincides with the model format

        :param adapter_type: Adapter type
        return: bool - True if the adapter type is the same as the model format
        """
        return adapter_type == cls.ADAPTER_FORMAT


class Claude3Adapter(BaseAdapter):
    ADAPTER_FORMAT = "claude"

    def __init__(self, message):
        """ Constructor for the ClaudeAdapter class

        :param message: Message like class
        """
        super().__init__(message)
        self.encoding = GPT2TokenizerFast.from_pretrained('Xenova/claude-tokenizer')
        self.message.substituted_query = self.message.preprocess()[-2:]

    def _adapt_messages(self, messages):
        """ Method to add the number of tokens to the messages

        :param messages: List of messages
        """
        for message in messages:
            if isinstance(message['content'], str) and not message.get('n_tokens'):
                message['n_tokens'] = len(self.encoding.encode(message['content']))
            elif isinstance(message['content'], list):
                for item in message['content']:
                    if item.get('n_tokens') and item['type'] in ["image_url", "image_b64"]:
                        self._adapt_image(item)
                    elif item.get('n_tokens'):
                        continue
                    elif item['type'] == "text":
                        item['n_tokens'] = len(self.encoding.encode(item['text']))
                    elif item['type'] in ["image_url", "image_b64"]:
                        item['n_tokens'] = self._adapt_image(item)

    def _adapt_image(self, image):
        """ Method to get tokens from an image and adapt it to claude3 format

        :param image: Image to get tokens from
        :return: int - Number of tokens
        """
        if image['image'].get('detail'):
            raise PrintableGenaiError(400, "Detail parameter not allowed in Claude vision model")

        if image['type'] == "image_url":
            try:
                downloaded_img = requests.get(image['image']['url']).content
                base64_str = base64.b64encode(downloaded_img).decode("utf-8")
                img = Image.open(io.BytesIO(downloaded_img))
            except:
                raise PrintableGenaiError(400, "Image must be a valid url format")
        elif image['type'] == "image_b64":
            base64_str = image['image']['base64']
            try:
                img = Image.open(io.BytesIO(base64.decodebytes(bytes(base64_str, "utf-8"))))
            except:
                raise PrintableGenaiError(400, "Image must be a valid base64 format")

        #TODO - Check what are they doing with image tokens calculation (this way appears in the api)
        width, height = self.resize_image(img.width, img.height, 1568, 1568)
        total = int((height * width) / 750)  # Not accurate

        media_type = img.format
        if media_type not in ['JPEG', 'PNG', 'GIF', 'WEBP']:
            raise PrintableGenaiError(400, "Image must be in format [jpeg, png, gif, webp]")

        # To change format to claude
        image['type'] = "image"
        image['source'] = {"type": "base64", "media_type": "image/" + media_type.lower(), "data": base64_str}
        image.pop('image')

        return total


class ManagerAdapters(object):
    ADAPTERS_TYPES = [Claude3Adapter, GPT4VAdapter, DalleAdapter, BaseAdapter]

    @staticmethod
    def get_adapter(conf: dict):
        """ Method to instantiate the adapter class: [claude3, gpt4v, dalle, base]

        :param conf: Adapter configuration. Example:  {"adapter":"claude3"}
        """
        for adapter in ManagerAdapters.ADAPTERS_TYPES:
            adapter_type = conf.get('adapter')
            if adapter.is_adapter_type(adapter_type):
                conf.pop('adapter')
                return adapter(**conf)
        raise PrintableGenaiError(400, f"Adapter type doesnt exist {conf}. "
                         f"Possible values: {ManagerAdapters.get_possible_adapters()}")

    @staticmethod
    def get_possible_adapters() -> List:
        """ Method to list the adapters: [claude3, gpt4v, dalle, base]

        :param conf: Adapter configuration. Example:  {"adapter":"claude3"}
        """
        return [message.ADAPTER_FORMAT for message in ManagerAdapters.ADAPTERS_TYPES]
