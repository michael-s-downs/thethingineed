### This code is property of the GGAO ###


# Native imports
import os
import re
import copy
import json
from typing import List
from abc import ABC, abstractmethod

#Installed imports
import tiktoken

# Local imports
from common.services import GENAI_LLM_GENERATIVES
from common.logging_handler import LoggerHandler
from common.errors.genaierrors import PrintableGenaiError
from common.models_manager import BaseModelConfigManager
from messages import ManagerMessages
from limiters import ManagerQueryLimiter

DEFAULT_STOP_MSG = "<|endoftext|>"

NOT_FOUND_MSG = "Not found"

EMPTY_TEMPLATE_MSG = "Template is empty"

USER_AND_SYSTEM_KEY_ALLOWED_MSG = "Template can only have user and system key"

USER_KEY_MANDATORY_MSG = "Template must contain the user key"

TEMPLATE_IS_NOT_DICT_MSG = "Template is not well formed, must be a dict {} structure"
NO_RESPONSE_FROM_MODEL = "There is no response from the model for the request"

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
        query_limiter = ManagerQueryLimiter.get_limiter({"message": message, "model": self.MODEL_MESSAGE,
                                                        "max_tokens": self.max_input_tokens,
                                                        "bag_tokens": self.bag_tokens,
                                                        "persistence": message.persistence, "querylimiter": "azure"})
        self.message = query_limiter.get_message()

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



class GPTModel(GenerativeModel):
    MODEL_MESSAGE = None

    # Not contains default params, because is an encapsulator for GPTModels, so the default are in there
    def __init__(self, model, model_type, pool_name, max_input_tokens, max_tokens, bag_tokens, zone, api_version,
                 temperature, n, functions, function_call, stop, models_credentials, top_p, seed, response_format):
        """It is the object in charge of modifying whether the inputs and the outputs of the gpt models

        :param model: Model name used in
        :param model_type: Model type used
        :param pool_name: Pool name used
        :param max_input_tokens: Max number of tokens desired in the input
        :param max_tokens: Max number of tokens desired in the output
        :param bag_tokens: Number of tokens reserved to generative models.
        :param zone: openai domain used in azure
        :param api_version: azure api version used
        :param temperature: Higher values like 0.8 will make the output more random,
                            while lower values like 0.2 will make it more focused and deterministic
        :param n: How many completions to generate for each prompt.
        :param functions: List of functions to be used in the model.
        :param function_call: Function call to be used in the model. Possible values: "auto", "none",
                                                                                        {"name": "my_function"}
        :param stop: Up to 4 sequences where the API will stop generating further tokens.
        :param top_p: Top p value to use in the model
        :param models_credentials: Credentials to use the model
        :param seed: Seed to use in the model
        :param response_format: The format in which the generated text is returned.
        """

        super().__init__(models_credentials, zone)
        self.model_name = model
        self.model_type = model_type
        self.pool_name = pool_name
        self.max_input_tokens = max_input_tokens
        self.max_tokens = max_tokens
        self.bag_tokens = bag_tokens
        self.zone = zone
        self.api_version = api_version
        if temperature >= 0.0:
            if temperature >= 0.0 and temperature <= 2.0:
                self.temperature = temperature
            else:
                raise ValueError(f"Temperature must be between 0.0 and 2.0 for the model {self.model_name}")
        self.n = n
        self.functions = functions
        self.function_call = function_call
        self.stop = stop
        self.top_p = top_p
        self.seed = seed
        self.response_format = response_format
        self.encoding = tiktoken.get_encoding("cl100k_base")
        self.encoding = tiktoken.encoding_for_model("gpt-3.5-turbo")

    def parse_data(self) -> json:
        """ Convert message and model data into json format.

        Returns: Query in json format to be sent.
        """
        data = dict(model=self.model_name,
                    temperature=self.temperature,
                    n=self.n,
                    stop=self.stop,
                    top_p=self.top_p,
                    messages=self.message.preprocess())
        if self.functions:
            data['functions'] = self.functions
            data['function_call'] = self.function_call
        if self.max_tokens > 0:
            data['max_tokens'] = self.max_tokens
        if self.seed:
            data['seed'] = self.seed
        if self.response_format:
            if self.response_format == "json_object":
                data['response_format'] = {"type": "json_object"}
            else:
                raise PrintableGenaiError(400, f"Response format {self.response_format} not supported for model {self.model_name} "
                                    f"(only 'json_object' supported)")
        return json.dumps(data)

    def get_result(self, response: dict) -> dict:
        """ Method to format the model response.

        :param response: Dict returned by  LLM endpoint.
        :return: Dict with the answer, tokens used and logprobs.
        """
        # Check status code
        if 'status_code' in response and response['status_code'] != 200:
            return {
                'status': 'error',
                'error_message': str(response['msg']),
                'status_code': response['status_code']
            }
        # get answer
        choice = response.get('choices', [{}])[0]

        # if finish_reason is content_filter, return error
        if 'finish_reason' in choice and choice['finish_reason'] == 'content_filter':
            return {
                'status': 'error',
                'error_message': 'content_filter',
                'status_code': 400
            }
        # if query has made with a function_call, return answer
        elif 'finish_reason' in choice and choice['finish_reason'] == 'function_call':
            answer = choice.get('message', {}).get('function_call', {}).get('arguments', '')

        else:
            # check if message is in the response
            if 'message' not in choice:
                raise PrintableGenaiError(400, f"Azure format is not as expected: {choice}.")

            # check if content is in the message
            text = choice.get('message', {}).get('content', '')

            # get text
            if re.search(NOT_FOUND_MSG, text, re.I):
                answer = NOT_FOUND_MSG
                self.logger.info("Answer not found.")
            else:
                answer = text.strip()

        self.logger.info(f"LLM response: {answer}.")
        text_generated = {
            'answer': answer,
            'logprobs': [],
            'n_tokens': response['usage']['total_tokens'],
            'query_tokens': self.message.user_query_tokens,
            'input_tokens': response['usage']['prompt_tokens'],
            'output_tokens': response['usage']['completion_tokens']
        }
        result = {
            'status': "finished",
            'result': text_generated,
            'status_code': 200
        }
        return result


    def __repr__(self):
        """Return the model representation."""
        return f'{{model:{self.model_name}, ' \
               f'max_tokens:{self.max_tokens}, ' \
               f'temperature:{self.temperature}, ' \
               f'n:{self.n}}}'


class DalleModel(GPTModel):
    MODEL_MESSAGE = "dalle"

    def __init__(self,
                 max_input_tokens: int = 4000,
                 model: str = 'genai-dalle3-sweden',
                 model_type: str = "",
                 pool_name: str = None,
                 n: int = 1,
                 quality: str = "standard",
                 response_format: str = "b64_json",
                 size: str = "1024x1024",
                 style: str = "vivid",
                 user: str = "",
                 zone: str = "sweden",
                 api_version: str = "2023-08-01-preview",
                 models_credentials: dict = None):
        """It is the object in charge of modifying whether the inputs and the outputs of the gpt models
        :param max_input_tokens: Max number of tokens desired in the input
        :param model: The model to use for image generation.
        :param model_type: The type of model to use for image generation.
        :param pool_name: The pool name to use for image generation.
        :param n: The number of images to generate. Must be between 1 and 10. For dall-e-3, only n=1 is supported.
        :param quality: The quality of the image that will be generated. hd creates images with finer details and greater consistency across the image. This param is only supported for dall-e-3.
        :param response_format: The format in which the generated images are returned. Must be one of url or b64_json.
        :param size: The size of the generated images. Must be one of 256x256, 512x512, or 1024x1024 for dall-e-2. Must be one of 1024x1024, 1792x1024, or 1024x1792 for dall-e-3 models.
        :param style: The style of the generated images. Must be one of vivid or natural. Vivid causes the model to lean towards generating hyper-real and dramatic images. Natural causes the model to produce more natural, less hyper-real looking images. This param is only supported for dall-e-3.
        :param user: A unique identifier representing your end-user, which can help OpenAI to monitor and detect abuse. Learn more.
        :param zone: openai domain used in azure
        :param api_version: azure api version used
        :param models_credentials: Credentials to use the model
        """
        super().__init__(model, model_type, pool_name, max_input_tokens, max_input_tokens, 0, zone, api_version,
                         0, n, [], "none", [DEFAULT_STOP_MSG], models_credentials,
                         0, None, response_format)
        self.quality = quality
        self.size = size
        self.style = style
        self.user = user
        self.is_vision = False

    def parse_data(self) -> json:
        """ Convert message and model data into json format.

        :return: Query in json format to be sent.
        """
        # top_p and seed currently not supported
        if self.model_type == "dalle3":
            data = dict(model=self.model_name,
                        prompt=str(self.message.preprocess()),
                        n=self.n,
                        quality=self.quality,
                        response_format=self.response_format,
                        size=self.size,
                        style=self.style,
                        user=self.user)
        elif self.model_type == "dalle2":
            data = dict(model=self.model_name,
                        prompt=str(self.message.preprocess()),
                        n=self.n,
                        response_format=self.response_format,
                        size=self.size,
                        user=self.user)
        else:
            return None

        return json.dumps(data)

    def get_result(self, response: dict) -> dict:
        """ Method to format the model response.

        :param response: Dict returned by  LLM endpoint.
        :return: Dict with the answer, tokens used and logprobs.
        """

        # Check status code
        if 'status_code' in response and response['status_code'] != 200:
            return {
                'status': 'error',
                'error_message': str(response['msg']),
                'status_code': response['status_code']
            }
        # get answer
        choice = response.get('data', [{}])[0]

        # if finish_reason is content_filter, return error
        if 'finish_reason' in choice and choice['finish_reason'] == 'content_filter':
            return {
                'status': 'error',
                'error_message': 'content_filter',
                'status_code': 400
            }
        else:
            # check if message is in the response
            if self.response_format == 'b64_json':
                if 'b64_json' not in choice:
                    raise PrintableGenaiError(400, f"Azure format is not as expected: {choice}.")
                answer = choice.get('b64_json', "b64 not found")
            elif self.response_format == 'url':
                if 'url' not in choice:
                    raise PrintableGenaiError(400, f"Azure format is not as expected: {choice}.")
                answer = choice.get('url', "url not found")
            else:
                raise PrintableGenaiError(400, f"Azure format is not as expected: {choice}.")

        self.logger.info(f"LLM response: {answer}.")
        text_generated = {
            'answer': answer,
            'logprobs': [],
            'n_tokens': 0,
            'input_tokens': self.message.get_query_tokens(self.message.preprocess()),
            'query_tokens': self.message.user_query_tokens,
            'output_tokens': 0
        }
        result = {
            'status': "finished",
            'result': text_generated,
            'status_code': 200
        }
        return result

    def __repr__(self):
        """Return the model representation."""
        return f'{{model:{self.model_name}, ' \
               f'max_input_tokens:{self.max_input_tokens}, ' \
               f'size:{self.size}, ' \
               f'n:{self.n}}}'


class ChatGPTModel(GPTModel):
    # AKA GPT3.5 // GPT4

    MODEL_MESSAGE = "chatGPT"

    def __init__(self, model: str = 'gpt-3.5-pool-europe',
                 model_type: str = "",
                 pool_name: str = None,
                 max_input_tokens: int = 4096,
                 max_tokens: int = -1,
                 bag_tokens: int = 500,
                 zone: str = "genai",
                 api_version: str = "2023-08-01-preview",
                 temperature: float = 0,
                 n: int = 1,
                 functions: List = [],
                 function_call: str = "none",
                 stop: List = [DEFAULT_STOP_MSG],
                 models_credentials: dict = None,
                 top_p: int = 0,
                 seed: int = None,
                 response_format: str = None):
        """It is the object in charge of modifying whether the inputs and the outputs of the gpt models

        :param model: Model name used
        :param model_type: Model type used
        :param pool_name: The pool name to use for image generation.
        :param max_input_tokens: Max number of tokens desired in the input
        :param max_tokens: Max number of tokens desired in the output
        :param bag_tokens: Number of tokens reserved to generative models.
        :param zone: openai domain used in azure
        :param api_version: azure api version used
        :param temperature: Higher values like 0.8 will make the output more random,
                            while lower values like 0.2 will make it more focused and deterministic
        :param n: How many completions to generate for each prompt.
        :param functions: List of functions to be used in the model.
        :param function_call: Function call to be used in the model. Possible values: "auto", "none",
                                                                                        {"name": "my_function"}
        :param stop: Up to 4 sequences where the API will stop generating further tokens.
        :param models_credentials: Credentials to use the model
        :param top_p: Top p value to use in the model
        :param seed: Seed to use in the model
        :param response_format: The format in which the generated text is returned.
        """

        super().__init__(model, model_type, pool_name, max_input_tokens, max_tokens, bag_tokens, zone, api_version,
                         temperature, n, functions, function_call, stop, models_credentials, top_p, seed, response_format)
        self.is_vision = False


class ChatGPTVision(GPTModel):
    MODEL_MESSAGE = "chatGPT-v"
    DEFAULT_TEMPLATE_NAME = "system_query_v"

    def __init__(self, model: str = 'genai-gpt4V-sweden',
                 model_type: str = "",
                 pool_name: str = None,
                 max_input_tokens: int = 32768,
                 max_tokens: int = 1000,  # -1 does not work in vision models
                 bag_tokens: int = 500,
                 zone: str = "genai-sweden",
                 api_version: str = "2024-02-15-preview",
                 temperature: float = 0,
                 n: int = 1,
                 functions: List = [],
                 function_call: str = "none",
                 stop: List = [DEFAULT_STOP_MSG],
                 models_credentials: dict = None,
                 top_p : int = 0,
                 seed: int = None,
                 response_format: str = None,
                 max_img_size_mb: float = 20.00):
        """It is the object in charge of modifying whether the inputs and the outputs of the gpt models

        :param model: Model name used in the gpt3_5 endpoint
        :param model_type: Model type used in the gpt3_5 endpoint
        :param pool_name: The pool name to use for image generation.
        :param max_input_tokens: Max number of tokens desired in the input
        :param max_tokens: Max number of tokens desired in the output
        :param bag_tokens: Number of tokens reserved to generative models.
        :param zone: openai domain used in azure
        :param api_version: azure api version used
        :param temperature: Higher values like 0.8 will make the output more random,
                            while lower values like 0.2 will make it more focused and deterministic
        :param n: How many completions to generate for each prompt.
        :param functions: List of functions to be used in the model.
        :param function_call: Function call to be used in the model. Possible values: "auto", "none",
                                                                                        {"name": "my_function"}
        :param stop: Up to 4 sequences where the API will stop generating further tokens.
        :param models_credentials: Credentials to use the model
        :param top_p: Top p value to use in the model
        :param seed: Seed to use in the model
        :param response_format: The format in which the generated text is returned.
        """

        super().__init__(model, model_type, pool_name, max_input_tokens, max_tokens, bag_tokens, zone, api_version,
                         temperature, n, functions, function_call, stop, models_credentials, top_p, seed, response_format)
        self.is_vision = True
        self.max_img_size_mb = max_img_size_mb

class ClaudeModel(GenerativeModel):
    MODEL_MESSAGE = None

    # Not contains default params, because is an encapsulator for ClaudeModels, so the default are in there
    def __init__(self, model, model_id, model_type, pool_name, max_input_tokens, max_tokens, bag_tokens, zone, api_version,
                 temperature, stop, models_credentials):
        """It is the object in charge of modifying whether the inputs and the outputs of the gpt models

        :param model: Model name used to specify model
        :param model_id: Model name used in the bedrock endpoint
        :param model_type: Model type used in the bedrock endpoint
        :param pool_name: The pool name to use for image generation.
        :param max_input_tokens: Max number of tokens used
        :param max_tokens: Max number of tokens used
        :param bag_tokens: Number of tokens reserved to generative models.
        :param zone: openai domain used in azure
        :param api_version: azure api version used
        :param temperature: Higher values like 0.8 will make the output more random,
                            while lower values like 0.2 will make it more focused and deterministic
        :param stop: Up to 4 sequences where the API will stop generating further tokens.
        :param models_credentials: Credentials to use the model
        """

        super().__init__(models_credentials, zone)
        self.model_name = model
        self.model_id = model_id
        self.model_type = model_type
        self.pool_name = pool_name
        self.max_input_tokens = max_input_tokens
        self.max_tokens = max_tokens
        self.bag_tokens = bag_tokens
        self.zone = zone
        self.api_version = api_version
        if temperature >= 0.0:
            if temperature >= 0.0 and temperature <= 1.0:
                self.temperature = temperature
            else:
                raise ValueError(f"Temperature must be between 0.0 and 1.0 for the model {self.model_name}")
        self.stop_sequences = stop

    def set_message(self, config: dict):
        """Sets the message as an argument of the class
           It also modifies the message taking into account the number of tokens.

        :param config: Dict with the message to be used
        """
        config['message'] = self.MODEL_MESSAGE
        if hasattr(self, 'max_img_size_mb'):
            config['max_img_size_mb'] = self.max_img_size_mb
        message = ManagerMessages().get_message(config)
        query_limiter = ManagerQueryLimiter.get_limiter({"message": message, "model": self.MODEL_MESSAGE,
                                                        "max_tokens": self.max_input_tokens,
                                                        "bag_tokens": self.bag_tokens,
                                                        "persistence": message.persistence, "querylimiter": "bedrock"})
        self.message = query_limiter.get_message()

    def parse_data(self) -> json:
        """ Convert message and model data into json format.

        :return: Query in json format to be sent.
        """
        messages = self.message.preprocess()
        system = messages.pop(-2).get('content') # Remove system message from the list
        body = json.dumps({"messages": messages,
                           "temperature": self.temperature,
                           "stop_sequences": self.stop_sequences,
                           "anthropic_version": self.api_version,
                           "max_tokens": self.max_tokens,
                           "system": system
                           })
        return body

    def get_result(self, response: dict) -> dict:
        """ Method to format the model response.

        :param response: Dict returned by  LLM endpoint.
        :return: Dict with the answer, tokens used and logprobs.
        """
        if 'ResponseMetadata' in response and response['ResponseMetadata']['HTTPStatusCode'] != 200:
            return {
                'status': 'error',
                'error_message': json.loads(response.get('body').read()),
                'status_code': response['ResponseMetadata']['HTTPStatusCode']
            }
        elif 'status_code' in response and response['status_code'] != 200:
            return {
                'status': 'error',
                'error_message': str(response['msg']),
                'status_code': response['status_code']
            }
        response_body = json.loads(response.get('body').read())
        answer = response_body.get('content')
        if len(answer) == 0:
            return {
                'status': 'error',
                'error_message': NO_RESPONSE_FROM_MODEL,
                'status_code': 400
            }
        self.logger.info(f"LLM response: {answer}.")

        text_generated = {
            'answer': answer[0].get('text'),
            'n_tokens': response_body.get('usage').get('input_tokens') + response_body.get('usage').get(
                'output_tokens'),
            'query_tokens': self.message.user_query_tokens,
            'output_tokens': response_body.get('usage').get('output_tokens'),
            'input_tokens': response_body.get('usage').get('input_tokens')
        }

        result = {
            'status': "finished",
            'result': text_generated,
            'status_code': 200
        }
        return result

    def __repr__(self):
        """Return the model representation."""
        return f'{{model:{self.model_name}, ' \
               f'max_tokens:{self.max_tokens}, ' \
               f'temperature:{self.temperature}}}'


class ChatClaudeModel(ClaudeModel):
    MODEL_MESSAGE = "chatClaude"

    def __init__(self, model: str = 'anthropic.claude-v2',
                 model_id: str = 'anthropic.claude-v2',
                 model_type: str = "",
                 pool_name: str = None,
                 max_input_tokens: int = 100000,
                 max_tokens: int = 4000,
                 bag_tokens: int = 500,
                 zone: str = "eu-central-1",
                 api_version: str = "",
                 temperature: float = 0,
                 stop: List = ["end_turn"],
                 models_credentials: dict = None):
        """It is the object in charge of modifying whether the inputs and the outputs of the gpt models

        :param model: Model name used to specify the model
        :param model_id: Model name used in the bedrock endpoint
        :param model_type: Model type used in the bedrock endpoint
        :param pool_name: The pool name to use for image generation.
        :param max_input_tokens: Max number of tokens used
        :param max_tokens: Max number of tokens used
        :param bag_tokens: Number of tokens reserved to generative models.
        :param zone: openai domain used in azure
        :param api_version: azure api version used
        :param temperature: Higher values like 0.8 will make the output more random,
                            while lower values like 0.2 will make it more focused and deterministic
        :param stop: Up to 4 sequences where the API will stop generating further tokens.
        :param models_credentials: Credentials to use the model
        """

        super().__init__(model, model_id, model_type, pool_name, max_input_tokens, max_tokens, bag_tokens, zone, api_version,
                         temperature, stop, models_credentials)
        self.is_vision = False


class ChatClaudeVision(ClaudeModel):
    MODEL_MESSAGE = "chatClaude-v"
    DEFAULT_TEMPLATE_NAME = "system_query_v"

    def __init__(self, model: str = 'anthropic.claude-3-sonnet-20240229-v1:0',
                 model_id: str = 'anthropic.claude-3-sonnet-20240229-v1:0',
                 model_type: str = "",
                 pool_name: str = None,
                 max_input_tokens: int = 100000,
                 max_tokens: int = 4000,
                 bag_tokens: int = 500,
                 zone: str = "",
                 api_version: str = "",
                 temperature: float = 0,
                 stop: List = ["end_turn"],
                 models_credentials: dict = None,
                 max_img_size_mb: float = 5.00):
        """It is the object in charge of modifying whether the inputs and the outputs of the gpt models

        :param model: Model name used to verify the model
        :param model_id: Model name used in the bedrock endpoint
        :param model_type: Model type used in the bedrock endpoint
        :param pool_name: The pool name to use for image generation.
        :param max_input_tokens: Max number of tokens used
        :param max_tokens: Max number of tokens used
        :param bag_tokens: Number of tokens reserved to generative models.
        :param zone: openai domain used in azure
        :param api_version: azure api version used
        :param temperature: Higher values like 0.8 will make the output more random,
                            while lower values like 0.2 will make it more focused and deterministic
        :param stop: Up to 4 sequences where the API will stop generating further tokens.
        :param models_credentials: Credentials to use the model
        """

        super().__init__(model, model_id, model_type, pool_name, max_input_tokens, max_tokens, bag_tokens, zone, api_version,
                         temperature, stop, models_credentials)
        self.is_vision = True
        self.max_img_size_mb = max_img_size_mb


class LlamaModel(GenerativeModel):
    MODEL_MESSAGE = "chatLlama3"

    def __init__(self,
                 model: str = 'meta-llama-3-8b-NorthVirginiaEast',
                 model_id: str = 'meta.llama3-8b-instruct-v1:0',
                 model_type: str = "",
                 pool_name: str = None,
                 max_input_tokens: int = 7000,
                 max_tokens: int = 1000,
                 bag_tokens: int = 500,
                 zone: str = "us-east-1",
                 temperature: float = 0,
                 stop: str = "<|end_of_text|>",
                 models_credentials: dict = None):
        """It is the object in charge of modifying whether the inputs and the outputs of the gpt models

        :param model: Model name used to specify model
        :param model_id: Model name used in the bedrock endpoint
        :param model_type: Model type used in the bedrock endpoint
        :param pool_name: The pool name to use for image generation.
        :param max_input_tokens: Max number of tokens used
        :param max_tokens: Max number of tokens used
        :param bag_tokens: Number of tokens reserved to generative models.
        :param zone: openai domain used in azure
        :param temperature: Higher values like 0.8 will make the output more random,
                            while lower values like 0.2 will make it more focused and deterministic
        :param stop: Up to 4 sequences where the API will stop generating further tokens.
        :param models_credentials: Credentials to use the model
        """

        super().__init__(models_credentials, zone)
        self.model_name = model
        self.model_id = model_id
        self.model_type = model_type
        self.pool_name = pool_name
        self.max_input_tokens = max_input_tokens
        self.max_tokens = max_tokens
        self.bag_tokens = bag_tokens
        self.zone = zone
        if temperature >= 0.0:
            if temperature >= 0.0 and temperature <= 1.0:
                self.temperature = temperature
            else:
                raise ValueError(f"Temperature must be between 0.0 and 1.0 for the model {self.model_name}")
        self.stop_sequences = stop
        self.is_vision = False


    def set_message(self, config: dict):
        """Sets the message as an argument of the class
           It also modifies the message taking into account the number of tokens.

        :param config: Dict with the message to be used
        """
        config['message'] = self.MODEL_MESSAGE
        message = ManagerMessages().get_message(config)
        query_limiter = ManagerQueryLimiter.get_limiter({"message": message, "model": self.MODEL_MESSAGE,
                                                        "max_tokens": self.max_input_tokens,
                                                        "bag_tokens": self.bag_tokens,
                                                        "persistence": message.persistence, "querylimiter": "bedrock"})
        self.message = query_limiter.get_message()

    def parse_data(self) -> json:
        """ Convert message and model data into json format.

        :return: Query in json format to be sent.
        """
        message = self.message.preprocess()
        system_content = message[0].get('content')
        user_content = message[-1].get('content')
        persistence = message[1:-1]
        persistence = self._adapt_persistence_llama(persistence)
        prompt = (f"<|begin_of_text|><|start_header_id|>system<|end_header_id|>{system_content}<|eot_id|>"
                  + persistence +
                  f"<|begin_of_text|><|start_header_id|>user<|end_header_id|>{user_content}<|eot_id|>"
                  f"<|start_header_id|>assistant<|end_header_id|>")
        body = json.dumps({"prompt": prompt,
                           "temperature": self.temperature,
                           "max_gen_len": self.max_tokens
                           })
        return body

    def _adapt_persistence_llama(self, persistence):
        llama_persistence = ""
        for el in persistence:
            if el.get('role') == "user":
                llama_persistence += "|><|start_header_id|>user<|end_header_id|>" + el.get('content') + "<|eot_id|>"
            if el.get('role') == "assistant":
                llama_persistence += "<|start_header_id|>assistant<|end_header_id|>" + el.get('content') + "<|eot_id|>"
        return llama_persistence

    def get_result(self, response: dict) -> dict:
        """ Method to format the model response.

        :param response: Dict returned by  LLM endpoint.
        :return: Dict with the answer, tokens used and logprobs.
        """
        if 'ResponseMetadata' in response and response['ResponseMetadata']['HTTPStatusCode'] != 200:
            return {
                'status': 'error',
                'error_message': json.loads(response.get('body').read()),
                'status_code': response['ResponseMetadata']['HTTPStatusCode']
            }
        elif 'status_code' in response and response['status_code'] != 200:
            return {
                'status': 'error',
                'error_message': str(response['msg']),
                'status_code': response['status_code']
            }
        answer = json.loads(response.get('body').read())
        if len(answer) == 0:
            return {
                'status': 'error',
                'error_message': NO_RESPONSE_FROM_MODEL,
                'status_code': 400
            }
        self.logger.info(f"LLM response: {answer}.")

        text_generated = {
            'answer': answer.get('generation'),
            'n_tokens': answer.get('generation_token_count') + answer.get('prompt_token_count'),
            'query_tokens': self.message.user_query_tokens,
            'output_tokens': answer.get('generation_token_count'),
            'input_tokens': answer.get('prompt_token_count')
        }

        result = {
            'status': "finished",
            'result': text_generated,
            'status_code': 200
        }
        return result

    def __repr__(self):
        """Return the model representation."""
        return f'{{model:{self.model_name}, ' \
               f'max_tokens:{self.max_tokens}, ' \
               f'temperature:{self.temperature}}}'

class NovaModel(GenerativeModel):
    MODEL_MESSAGE = None

    # Not contains default params, because is an encapsulator for ClaudeModels, so the default are in there
    def __init__(self, model, model_id, model_type, pool_name, max_input_tokens, max_tokens, bag_tokens, zone, top_p,
                 top_k, temperature, stop, models_credentials):
        """It is the object in charge of modifying whether the inputs and the outputs of the gpt models

        :param model: Model name used to specify model
        :param model_id: Model name used in the bedrock endpoint
        :param model_type: Model type used in the bedrock endpoint
        :param pool_name: The pool name to use for image generation.
        :param max_input_tokens: Max number of tokens used
        :param max_tokens: Max number of tokens used
        :param bag_tokens: Number of tokens reserved to generative models.
        :param zone: openai domain used in azure
        :param api_version: azure api version used
        :param temperature: Higher values like 0.8 will make the output more random,
                            while lower values like 0.2 will make it more focused and deterministic
        :param stop: Up to 4 sequences where the API will stop generating further tokens.
        :param top_p: Top p value to use in the model
        :param top_k: Top k value to use in the model
        :param models_credentials: Credentials to use the model
        """

        super().__init__(models_credentials, zone)
        self.model_name = model
        self.model_id = model_id
        self.model_type = model_type
        self.pool_name = pool_name
        self.max_input_tokens = max_input_tokens
        self.max_tokens = max_tokens
        self.bag_tokens = bag_tokens
        self.zone = zone
        self.stop = stop
        self.top_p = top_p
        self.top_k = top_k
        if temperature >= 0.0:
            if temperature >= 0.0 and temperature <= 1.0:
                self.temperature = temperature
            else:
                raise ValueError(f"Temperature must be between 0.0 and 1.0 for the model {self.model_name}")    
            
    def set_message(self, config: dict):
        """Sets the message as an argument of the class
           It also modifies the message taking into account the number of tokens.

        :param config: Dict with the message to be used
        """
        config['message'] = self.MODEL_MESSAGE
        if hasattr(self, 'max_img_size_mb'):
            config['max_img_size_mb'] = self.max_img_size_mb
        message = ManagerMessages().get_message(config)
        query_limiter = ManagerQueryLimiter.get_limiter({"message": message, "model": self.MODEL_MESSAGE,
                                                        "max_tokens": self.max_input_tokens,
                                                        "bag_tokens": self.bag_tokens,
                                                        "persistence": message.persistence, "querylimiter": "nova"})
        self.message = query_limiter.get_message()

    def parse_data(self) -> json:
        """ Convert message and model data into json format.

        :return: Query in json format to be sent.
        """
        messages = self.message.preprocess()
        system = messages.pop(-2).get('content') # Remove system message from the list
        body = {"system": system,
                           "messages": messages,
                           "inferenceConfig": {
                                "max_new_tokens": self.max_tokens,
                                "temperature": self.temperature,
                                "top_p": self.top_p,
                                "stopSequences": self.stop
                           }}
        if self.top_k:
            body['inferenceConfig']['top_k'] = self.top_k
        return json.dumps(body)

    def get_result(self, response: dict) -> dict:
        """ Method to format the model response.

        :param response: Dict returned by  LLM endpoint.
        :return: Dict with the answer, tokens used and logprobs.
        """
        if 'ResponseMetadata' in response and response['ResponseMetadata']['HTTPStatusCode'] != 200:
            return {
                'status': 'error',
                'error_message': json.loads(response.get('body').read()),
                'status_code': response['ResponseMetadata']['HTTPStatusCode']
            }
        elif 'status_code' in response and response['status_code'] != 200:
            return {
                'status': 'error',
                'error_message': str(response['msg']),
                'status_code': response['status_code']
            }
        response_body = json.loads(response.get('body').read())
        answer = response_body.get('output').get('message').get('content')
        if len(answer) == 0:
            return {
                'status': 'error',
                'error_message': NO_RESPONSE_FROM_MODEL,
                'status_code': 400
            }
        self.logger.info(f"LLM response: {answer}.")

        text_generated = {
            'answer': answer[0].get('text'),
            'n_tokens': response_body.get('usage').get('totalTokens'),
            'query_tokens': self.message.user_query_tokens,
            'output_tokens': response_body.get('usage').get('outputTokens'),
            'input_tokens': response_body.get('usage').get('inputTokens')
        }

        result = {
            'status': "finished",
            'result': text_generated,
            'status_code': 200
        }
        return result

    def __repr__(self):
        """Return the model representation."""
        return f'{{model:{self.model_name}, ' \
               f'max_tokens:{self.max_tokens}, ' \
               f'temperature:{self.temperature}}}'


class ChatNova(NovaModel):
    MODEL_MESSAGE = "chatNova"

    def __init__(self, 
                 model, 
                 model_id,
                 model_type, 
                 pool_name,
                 top_k: int = None, 
                 max_input_tokens: int = 128000, 
                 max_tokens: int = 5000, 
                 bag_tokens: int = 500, 
                 zone:str = "us-east-1", 
                 top_p: float = 0.9, 
                 temperature: float = 0.7, 
                 stop: List = ["end_turn"], 
                 models_credentials: dict = None):
            """It is the object in charge of modifying whether the inputs and the outputs of the gpt models

            :param model: Model name used to specify model
            :param model_id: Model name used in the bedrock endpoint
            :param model_type: Model type used in the bedrock endpoint
            :param pool_name: The pool name to use for image generation.
            :param max_input_tokens: Max number of tokens used
            :param max_tokens: Max number of tokens used
            :param bag_tokens: Number of tokens reserved to generative models.
            :param zone: openai domain used in azure
            :param api_version: azure api version used
            :param temperature: Higher values like 0.8 will make the output more random,
                                while lower values like 0.2 will make it more focused and deterministic
            :param stop: Up to 4 sequences where the API will stop generating further tokens.
            :param top_p: Top p value to use in the model
            :param top_k: Top k value to use in the model
            :param models_credentials: Credentials to use the model
            """

            super().__init__(model, model_id, model_type, pool_name, max_input_tokens, max_tokens, bag_tokens, zone, top_p,
                             top_k, temperature, stop, models_credentials)
            self.is_vision = False


class ChatNovaVision(NovaModel):
    MODEL_MESSAGE = "chatNova-v"
    DEFAULT_TEMPLATE_NAME = "system_query_v"
    
    def __init__(self, 
                 model, 
                 model_id, 
                 model_type, 
                 pool_name: str = None,
                 top_k: int = None, 
                 max_input_tokens: int = 128000, 
                 max_tokens: int = 5000, 
                 bag_tokens: int = 500, 
                 zone:str = "us-east-1", 
                 top_p: float = 0.9, 
                 temperature: float = 0.7, 
                 stop: List = ["end_turn"], 
                 models_credentials: dict = None,
                 max_img_size_mb: float = 20.00):
            """It is the object in charge of modifying whether the inputs and the outputs of the gpt models

            :param model: Model name used to specify model
            :param model_id: Model name used in the bedrock endpoint
            :param model_type: Model type used in the bedrock endpoint
            :param pool_name: The pool name to use for image generation.
            :param max_input_tokens: Max number of tokens used
            :param max_tokens: Max number of tokens used
            :param bag_tokens: Number of tokens reserved to generative models.
            :param zone: openai domain used in azure
            :param api_version: azure api version used
            :param temperature: Higher values like 0.8 will make the output more random,
                                while lower values like 0.2 will make it more focused and deterministic
            :param stop: Up to 4 sequences where the API will stop generating further tokens.
            :param top_p: Top p value to use in the model
            :param top_k: Top k value to use in the model
            :param models_credentials: Credentials to use the model
            """

            super().__init__(model, model_id, model_type, pool_name, max_input_tokens, max_tokens, bag_tokens, zone, top_p,
                             top_k, temperature, stop, models_credentials)
            self.is_vision = True
            self.max_img_size_mb = max_img_size_mb


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
