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
from common.utils import resize_image


class BaseAdapter(ABC):
    ADAPTER_FORMAT = "base"

    # Adapter for the models with only text type
    def __init__(self, message, max_img_size_mb=0.00):
        """ Constructor for the BaseAdapter class

        :param message: Message like class
        """
        logger_handler = LoggerHandler(GENAI_LLM_ADAPTERS, level=os.environ.get('LOG_LEVEL', "INFO"))
        self.logger = logger_handler.logger
        self.message = message
        self.encoding = tiktoken.get_encoding("cl100k_base")
        self.encoding = tiktoken.encoding_for_model("gpt-3.5-turbo")
        self.max_img_size_mb = max_img_size_mb
        self.available_img_formats = ["JPEG", "PNG", "GIF", "WEBP"]

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
                    if item['type'] in ["image_url", "image_b64"]:
                        self._adapt_image(item)
                    elif item['type'] == "text":
                        self._adapt_text(item)


    def _adapt_text(self, text):
        """ Method to adapt text to gpt4v format
        
        :param text: Text to adapt
        """
        if text.get('n_tokens'):
            return
        else:
            text['n_tokens'] = len(self.encoding.encode(text['text']))


    def _adapt_image(self, image_dict):
        """ Method to get the number of tokens from an image

        :param image_dict: Image to get the number of tokens from
        """
        pass


    def _get_base64_image(self, image_dict):
        """ Method to get the base64 image and the size of the image

        :param image_dict: Image to get the base64 from
        
        :return: str - Base64 image
        :return: float - Size of the image
        :return: str - Route of the resized image
        :return: Image - Image object
        """
        if image_dict['type'] == "image_url":
            content = image_dict['image']['url']
            downloaded_image = requests.get(content)
            if downloaded_image.status_code != 200:
                raise PrintableGenaiError(downloaded_image.status_code, f"Error downloading the image: {downloaded_image.reason}")
            try:
                img = Image.open(io.BytesIO(downloaded_image.content))
            except Exception as ex:
                print(ex)
                raise PrintableGenaiError(400, "Error, downloaded image content (url) must be valid")
        elif image_dict['type'] == "image_b64":
            content = image_dict['image']['base64']
            try:
                img = Image.open(io.BytesIO(base64.decodebytes(bytes(content, "utf-8"))))
            except:
                raise PrintableGenaiError(400, "Image must be a valid base64 format")

        media_type = img.format
        if media_type not in self.available_img_formats:
            raise PrintableGenaiError(400, f"Image must be in format {self.available_img_formats}")

        # Resize the image and get the base64
        resized_image_route = "image." + media_type.lower()
        img.save(resized_image_route, quality=95)
        size, _ = resize_image(resized_image_route, max_size_mb=self.max_img_size_mb)
        with open(resized_image_route, "rb") as f: # Faster to read from file than from Image and BytesIO
            base64_img = base64.b64encode(f.read()).decode("utf-8")
        img.close()
        
        return base64_img, size, resized_image_route, Image.open(resized_image_route)

    
    @staticmethod
    def _get_image_tokens(width, height, width_resize, height_resize) -> int:
        """ Resize an image to have the correct format and get the tokens

        :param width: Width of the image
        :param height: Height of the image
        :param width_resize: Width to resize to
        :param height_resize: Height to resize to
        :return: int - Number of tokens
        """
        pass

    @classmethod
    def is_adapter_type(cls, adapter_type: str):
        """It will chech if a adapter_type coincides with the model format
        """
        return adapter_type == cls.ADAPTER_FORMAT


class GPT4VAdapter(BaseAdapter):
    ADAPTER_FORMAT = "gpt4v"

    def __init__(self, message, max_img_size_mb=20.00):
        """ Constructor for the ClaudeAdapter class

        :param message: Message like class
        """
        super().__init__(message, max_img_size_mb)

    def _adapt_messages(self, messages):
        """ Method to add the number of tokens to the messages

        :param messages: List of messages
        """
        for message in messages:
            if isinstance(message['content'], str) and not message.get('n_tokens'):
                message['n_tokens'] = len(self.encoding.encode(message['content']))
            elif isinstance(message['content'], list):
                for item in message['content']:
                    if item['type'] in ["image_url", "image_b64"]:
                        self._adapt_image(item)
                    elif item['type'] == "text":
                        self._adapt_text(item)


    def _adapt_text(self, text):
        """ Method to adapt text to gpt4v format
        
        :param text: Text to adapt
        """
        if text.get('n_tokens'):
            return
        else:
            text['n_tokens'] = len(self.encoding.encode(text['text']))


    @staticmethod
    def _get_image_tokens(width, height, width_resize, height_resize) -> int:
        """ Resize an image to have the correct format and get the tokens

        :param width: Width of the image
        :param height: Height of the image
        :param width_resize: Width to resize to
        :param height_resize: Height to resize to
        :return: int - Number of tokens
        """
        if width > width_resize or height > height_resize:
            if width > height:
                height = int(height * height_resize / width)
                width = width_resize
            else:
                width = int(width * width_resize / height)
                height = height_resize
        return 85 + 170 * ceil(height / 512) * ceil(width / 512)


    def _adapt_image(self, image_dict):
        """ Method to get tokens from an image_dict and adapt it to gpt4v format

        :param image_dict: Image to get tokens from
        :return: int - Number of tokens
        """
        # Get the base64 image resized and the size
        base64_img, size, resized_image_route, resized_image  = self._get_base64_image(image_dict)
        
        if not image_dict.get('n_tokens'):
            if image_dict['image'].get('detail') == "low":
                total = 85
            else:
                total = self._get_image_tokens(resized_image.width, resized_image.height, 1024, 1024)
        else:
            total = image_dict['n_tokens']

        # Always in base64 format for compatibility with resize
        content = "data:image/" + resized_image.format.lower() + ";base64," + base64_img
        image_dict['type'] = "image_url"
        if image_dict['image'].get('detail'):
            image_dict['image_url'] = {'url': content, 'detail': image_dict['image']['detail']}
        else:
            image_dict['image_url'] = {'url': content}
        image_dict.pop('image')
        image_dict['n_tokens'] = total
        
        # Remove the resized image and close the opened image
        resized_image.close()
        os.remove(resized_image_route)


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

    def _adapt_messages(self, messages):
        for message in messages:
            if not message.get('n_tokens'):
                # Dalle is non-vision so it will be only text
                message['n_tokens'] = len(self.encoding.encode(message['content']))

    @classmethod
    def is_adapter_type(cls, adapter_type: str):
        """It will chech if a adapter_type coincides with the model format

        :param adapter_type: Adapter type
        return: bool - True if the adapter type is the same as the model format
        """
        return adapter_type == cls.ADAPTER_FORMAT


class Claude3Adapter(BaseAdapter):
    ADAPTER_FORMAT = "claude"

    def __init__(self, message, max_img_size_mb=5.00):
        """ Constructor for the ClaudeAdapter class

        :param message: Message like class
        """
        super().__init__(message, max_img_size_mb)
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
                    if item['type'] == "text":
                        self._adapt_text(item)
                    elif item['type'] in ["image_url", "image_b64"]:
                        self._adapt_image(item)

    def _adapt_text(self, text):
        """ Method to adapt text to claude format
        
        :param text: Text to adapt
        """
        if text.get('n_tokens'):
            return
        else:
            text['n_tokens'] = len(self.encoding.encode(text['text']))


    @staticmethod
    def _get_image_tokens(width, height, width_resize, height_resize) -> int:
        """ Resize an image to have the correct format and get the tokens

        :param width: Width of the image
        :param height: Height of the image
        :param width_resize: Width to resize to
        :param height_resize: Height to resize to
        :return: int - Number of tokens
        """
        if width > width_resize or height > height_resize:
            if width > height:
                height = int(height * height_resize / width)
                width = width_resize
            else:
                width = int(width * width_resize / height)
                height = height_resize
        return int((height * width) / 750)  # Not accurate


    def _adapt_image(self, image_dict):
        """ Method to get tokens from an image and adapt it to claude3 format

        :param image_dict: Image to get tokens from
        """
        if image_dict['image'].get('detail'):
            raise PrintableGenaiError(400, "Detail parameter not allowed in Claude vision model")

        # Get the base64 image resized and the size
        base64_img, size, resized_image_route, resized_image  = self._get_base64_image(image_dict)

        #TODO - Check what are they doing with image tokens calculation (this way appears in the api)
        if not image_dict.get('n_tokens'):
            total = self._get_image_tokens(resized_image.width, resized_image.height, 1568, 1568)
        else:
            total = image_dict['n_tokens']

        # To change format to claude
        image_dict['type'] = "image"
        image_dict['source'] = {"type": "base64", "media_type": "image/" + resized_image.format.lower(), "data": base64_img}
        image_dict.pop('image')
        image_dict['n_tokens'] = total

        # Remove the resized image and close the opened image
        resized_image.close()
        os.remove(resized_image_route)



class NovaAdapter(BaseAdapter):
    ADAPTER_FORMAT = "nova"

    def __init__(self, message, max_img_size_mb=20.00):
        """ Constructor for the NovaAdapter class

        :param message: Message like class
        """
        super().__init__(message, max_img_size_mb)
        self.message.substituted_query = self.message.preprocess()[-2:]

    def _adapt_messages(self, messages):
        """ Method to add the number of tokens to the messages

        :param messages: List of messages
        """
        for message in messages:
            # Is always a list (forced in the model)
            for item in message['content']:
                if not item.get('type') or item['type'] == "text":
                    # Non-vision case when item does not have type
                    self._adapt_text(item)
                elif item['type'] in ["image_url", "image_b64"]:
                    item = self._adapt_image(item)


    def _adapt_text(self, text):
        """ Method to adapt text to nova-v format"""
        text.pop('type', None) # In nova no type param is needed
        if not text.get('n_tokens'):
            text['n_tokens'] = len(self.encoding.encode(text['text']))


    @staticmethod
    def _get_image_tokens(width, height, width_resize, height_resize) -> int:
        """ Resize an image to have the correct format and get the tokens

        :param width: Width of the image
        :param height: Height of the image
        :param width_resize: Width to resize to
        :param height_resize: Height to resize to
        :return: int - Number of tokens
        """
        if width > width_resize or height > height_resize:
            if width > height:
                height = int(height * height_resize / width)
                width = width_resize
            else:
                width = int(width * width_resize / height)
                height = height_resize
        #TODO - Check the formula for tokens calculation (nothing found in first iteration). SAME as claude3 one
        return int((height * width) / 750)  # Not accurate
    

    def _adapt_image(self, image_dict):
        """ Method to get tokens from an image and adapt it to nova-v format

        :param image_dict: Image to get tokens from
        :return: int - Number of tokens
        """
        if image_dict['image'].get('detail'):
            raise PrintableGenaiError(400, "Detail parameter not allowed in Nova vision model")

        # Get the base64 image resized and the size
        base64_img, size, resized_image_route, resized_image  = self._get_base64_image(image_dict)

        #TODO - Check the formula for tokens calculation (nothing found in first iteration). SAME as claude3 one
        if not image_dict.get('n_tokens'):
            total = self._get_image_tokens(resized_image.width, resized_image.height, 1568, 1568)
        else:
            total = image_dict['n_tokens']

        # To change format to nova (no type param is needed)
        image_dict['image'] = {
                "format": resized_image.format.lower(),
                "source": {
                    "bytes": base64_img
                }
        }
        image_dict['n_tokens'] = total
        image_dict.pop('type')

        # Remove the resized image and close the opened image
        resized_image.close()
        os.remove(resized_image_route)



class ManagerAdapters(object):
    ADAPTERS_TYPES = [Claude3Adapter, GPT4VAdapter, DalleAdapter, BaseAdapter, NovaAdapter]

    @staticmethod
    def get_adapter(conf: dict):
        """ Method to instantiate the adapter class: [claude3, gpt4v, dalle, base, nova]

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
        """ Method to list the adapters: [claude3, gpt4v, dalle, base, nova]

        :param conf: Adapter configuration. Example:  {"adapter":"claude3"}
        """
        return [message.ADAPTER_FORMAT for message in ManagerAdapters.ADAPTERS_TYPES]
