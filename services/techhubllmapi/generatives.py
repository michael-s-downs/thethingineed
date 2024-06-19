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
from messages import ManagerMessages
from limiters import ManagerQueryLimiter


class GenerativeModel(ABC):
    MODEL_MESSAGE = None

    def __init__(self, models_credentials, zone):
        """It is the object in charge of modifying whether the inputs and the outputs of the gpt models


        :param models_credentials: Credentials to use the model
        :param zone: zone in where the model is located
        """
        logger_handler = LoggerHandler(GENAI_LLM_GENERATIVES, level=os.environ.get('LOG_LEVEL', "INFO"))
        self.logger = logger_handler.logger
        self.default_template_name = "system_query"
        self.api_key = models_credentials.get(zone, None)

    def set_message(self, config: dict):
        """Sets the message as an argument of the class
           It also modifies the message taking into account the number of tokens.

        :param config: Dict with the message to be used
        """
        config['message'] = self.MODEL_MESSAGE
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

    def template_ok(self, template: dict):
        """Check if the template is correct.

        :param template: Template to be checked
        """
        if not isinstance(template, dict):
            raise ValueError("Template is not a dict {} structure")


class PromptGPTModel(GenerativeModel):
    MODEL_MESSAGE = "promptGPT"

    def __init__(self, model: str = 'gpt35turbo-sweden-instruct',
                 model_type: str = "",
                 max_input_tokens: int = 4096,
                 max_tokens: int = 3096,
                 bag_tokens: int = 500,
                 zone: str = "dolffia",
                 api_version: str = "2023-08-01-preview",
                 temperature: float = 0,
                 n: int = 1,
                 logprobs: int = 0,
                 stop: List = ['<|endoftext|>'],
                 models_credentials: dict = None):
        """It is the object in charge of modifying whether the inputs and the outputs of the gpt models

        :param model: Model name used in the gpt endpoint
        :model_type: Model type used in the gpt endpoint
        :param max_input_tokens: Max number of tokens desired in the input
        :param max_tokens: Max number of tokens desired in the output
        :param bag_tokens: Number of tokens reserved to generative models.
        :param zone: openai domain used in azure
        :param api_version: azure api version used
        :param temperature: Higher values like 0.8 will make the output more random,
                            while lower values like 0.2 will make it more focused and deterministic
        :param n: How many completions to generate for each prompt.
        :param logprobs: Include the log probabilities on the logprobs most likely tokens
        :param stop: Up to 4 sequences where the API will stop generating further tokens.
        :param models_credentials: Credentials to use the model
        """
        super().__init__(models_credentials, zone)
        self.model_name = model
        self.model_type = model_type
        self.max_input_tokens = max_input_tokens
        self.max_tokens = max_tokens
        self.bag_tokens = bag_tokens
        self.zone = zone
        self.api_version = api_version
        self.temperature = temperature
        self.n = n
        self.logprobs = logprobs
        self.stop = stop
        self.is_vision = False

    def parse_data(self) -> json:
        """ Convert message and model data into json format.

        :return: Query in json format to be sent.
        """
        data = dict(model=self.model_name,
                    max_tokens=self.max_tokens,
                    temperature=self.temperature,
                    logprobs=self.logprobs,
                    stop=self.stop,
                    prompt=self.message.preprocess(),
                    top_p=1)
        return json.dumps(data)

    def get_result(self, response: dict) -> dict:
        """ Method to format the model response.

        :param response: Dict returned by  LLM endpoint.

        :return: Dict with the answer, tokens used and logprobs.
        """
        choice = response["choices"][0]

        text = choice['text']
        if re.search("Not found", text, re.I):
            answer = ""
            self.logger.info("Answer not found.")
        else:
            answer = text.strip()
            self.logger.info(f"LLM response: {answer}.")

        # Logporbs
        logprobs_response = choice.get('logprobs')
        logprobs = []
        if logprobs_response:
            tokens = logprobs_response['tokens']
            token_logporb = logprobs_response['token_logprobs']

            for token, token_logporb in zip(tokens, token_logporb):
                # if token == self.stop[0]:
                #    break
                logprobs.append([token, token_logporb])

        text_generated = {
            'answer': answer,
            'logprobs': logprobs,
            'n_tokens': response['usage']['total_tokens'],
            'query_tokens': response['usage']['prompt_tokens'],
            'input_tokens': response['usage']['prompt_tokens'],
            'output_tokens': response['usage']['completion_tokens']
        }
        result = {
            'status': "finished",
            'result': text_generated,
            'status_code': 200
        }
        return result


class GPTModel(GenerativeModel):
    MODEL_MESSAGE = None

    # Not contains default params, because is an encapsulator for GPTModels, so the default are in there
    def __init__(self, model, model_type, max_input_tokens, max_tokens, bag_tokens, zone, api_version,
                 temperature, n, functions, function_call, stop, models_credentials, top_p, seed):
        """It is the object in charge of modifying whether the inputs and the outputs of the gpt models

        :param model: Model name used in
        :param model_type: Model type used
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
        """

        super().__init__(models_credentials, zone)
        self.model_name = model
        self.model_type = model_type
        self.max_input_tokens = max_input_tokens
        self.max_tokens = max_tokens
        self.bag_tokens = bag_tokens
        self.zone = zone
        self.api_version = api_version
        self.temperature = temperature
        self.n = n
        self.functions = functions
        self.function_call = function_call
        self.stop = stop
        self.top_p = top_p
        self.seed = seed
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

        return json.dumps(data)

    def get_result(self, response: dict) -> dict:
        """ Method to format the model response.

        :param response: Dict returned by  LLM endpoint.
        :return: Dict with the answer, tokens used and logprobs.
        """
        # Check status code
        if 'status_code' in response and response['status_code'] in [400, 401, 404, 408, 500, 502, 503]:
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
            if not 'message' in choice:
                raise ValueError(f"Azure format is not as expected: {choice}.")

            # check if content is in the message
            text = choice.get('message', {}).get('content', '')

            # get text
            if re.search("Not found", text, re.I):
                answer = ""
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

    def template_ok(self, template: dict):
        """Check if the template is correct.

        :param template: Template to be checked
        :return: True if the template is correct, False otherwise
        """
        if template:
            try:
                template_dict = eval(template) if not isinstance(template, dict) else template
            except SyntaxError:
                raise ValueError("Template is not well formed, must be a dict {} structure")
            if not template_dict.get("user"):
                raise ValueError("Template must contain the user key")
            if len(template_dict.keys()) != 2:
                raise ValueError("Template can only have user and system key")
            if "$query" not in template_dict.get("user"):
                raise ValueError("Template must contain $query to be replaced")
        else:
            raise ValueError("Template is empty")

    def __repr__(self):
        """Return the model representation."""
        return f'{{model:{self.model_name}, ' \
               f'max_tokens:{self.max_tokens}, ' \
               f'temperature:{self.temperature}, ' \
               f'n:{self.n}}}'


class DalleModel(GPTModel):
    MODEL_MESSAGE = "dalle"

    def __init__(self,
                 max_input_tokens,
                 model: str = 'dolffia-dalle3-sweden',
                 model_type: str = "",
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
        super().__init__(model, model_type, max_input_tokens, max_input_tokens, 0, zone, api_version,
                         0, n, [], "none", ['<|endoftext|>'], models_credentials,
                         0, None)
        self.quality = quality
        self.response_format = response_format
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
        if 'status_code' in response and response['status_code'] in [400, 401, 404, 408, 500, 502, 503]:
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
        # if query has made with a function_call, return answer
        elif 'finish_reason' in choice and choice['finish_reason'] == 'function_call':
            answer = choice.get('message', {}).get('function_call', {}).get('arguments', '')

        else:
            # check if message is in the response
            if self.response_format == 'b64_json':
                if 'b64_json' not in choice:
                    raise ValueError(f"Azure format is not as expected: {choice}.")
                answer = choice.get('b64_json', "b64 not found")
            elif self.response_format == 'url':
                if 'url' not in choice:
                    raise ValueError(f"Azure format is not as expected: {choice}.")
                answer = choice.get('url', "url not found")
            else:
                raise ValueError(f"Azure format is not as expected: {choice}.")

        self.logger.info(f"LLM response: {answer}.")
        text_generated = {
            'answer': answer,
            'logprobs': [],
            'n_tokens': 0,
            'input_tokens': self.message.get_query_tokens(self.message.preprocess()),
            'query_tokens': self.message.user_query_tokens,
            'output_tokens': 0,
            'n': self.n
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
                 max_input_tokens: int = 4096,
                 max_tokens: int = -1,
                 bag_tokens: int = 500,
                 zone: str = "dolffia",
                 api_version: str = "2023-08-01-preview",
                 temperature: float = 0,
                 n: int = 1,
                 functions: List = [],
                 function_call: str = "none",
                 stop: List = ['<|endoftext|>'],
                 models_credentials: dict = None,
                 top_p: int = 0,
                 seed: int = None):
        """It is the object in charge of modifying whether the inputs and the outputs of the gpt models

        :param model: Model name used
        :param model_type: Model type used
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
        """

        super().__init__(model, model_type, max_input_tokens, max_tokens, bag_tokens, zone, api_version,
                         temperature, n, functions, function_call, stop, models_credentials, top_p, seed)
        self.is_vision = False


class ChatGPTvModel(GPTModel):
    MODEL_MESSAGE = "chatGPT-v"

    def __init__(self, model: str = 'dolffia-gpt4V-sweden',
                 model_type: str = "",
                 max_input_tokens: int = 32768,
                 max_tokens: int = 1000,  # -1 does not work in vision models
                 bag_tokens: int = 500,
                 zone: str = "dolffia-sweden",
                 api_version: str = "2024-02-15-preview",
                 temperature: float = 0,
                 n: int = 1,
                 functions: List = [],
                 function_call: str = "none",
                 stop: List = ['<|endoftext|>'],
                 models_credentials: dict = None,
                 top_p : int = 0,
                 seed: int = None):
        """It is the object in charge of modifying whether the inputs and the outputs of the gpt models

        :param model: Model name used in the gpt3_5 endpoint
        :param model_type: Model type used in the gpt3_5 endpoint
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
        """

        super().__init__(model, model_type, max_input_tokens, max_tokens, bag_tokens, zone, api_version,
                         temperature, n, functions, function_call, stop, models_credentials, top_p, seed)
        self.default_template_name = "system_query_v"
        self.is_vision = True

    def template_ok(self, template: dict):
        """Check if the template is correct.

        :param template: Template to be checked
        """
        if template:
            try:
                template_dict = eval(template) if not isinstance(template, dict) else template
            except SyntaxError:
                raise ValueError("Template is not well formed, must be a dict {} structure")
            if not template_dict.get("user"):
                raise ValueError("Template must contain the user key")
            if isinstance(template_dict.get("user"), list):
                self._check_multimodal(template_dict.get("user"))
            elif isinstance(template_dict.get("user"), str):
                pass
            else:
                raise ValueError("The template must be for vision models. (a list for the 'user' key with multimodal format)")
            if len(template_dict.keys()) != 2:
                raise ValueError("Template can only have user and system key")
            if "$query" not in template_dict.get("user"):
                raise ValueError("Template must contain $query to be replaced")
        else:
            raise ValueError("Template is empty")

    @staticmethod
    def _check_multimodal(content_list: list):
        """Check if the multimodal template is correct.

        :param content_list: List of content to be checked
        """
        for el in content_list:
            if isinstance(el, dict):
                if el.get('type') == 'text':
                    if not el.get('text') or not isinstance(el.get('text'), str):
                        raise ValueError("For type 'text' there must be a key 'text' containing a string")
                elif el.get('type') == 'image_url':
                    image_url = el.get('image_url')
                    if not image_url:
                        raise ValueError("'image_url' param in type 'image_url' is mandatory")
                    if not isinstance(image_url.get('url'), str):
                        raise ValueError("Type 'image_url' must contain a 'image_url' key with a string")
                    if image_url.get('detail') and image_url.get('detail') not in ["high", "low", "auto"]:
                        raise ValueError("Detail parameter must be one in ['high', 'low', 'auto']")
                else:
                    raise ValueError("Key must be 'type' and its value must be one in ['text', 'image_url']")
            elif "$query" in el:
                continue
            else:
                raise ValueError("Elements of the content must be dict {} or a string containing \"$query\"")


class ClaudeModel(GenerativeModel):
    MODEL_MESSAGE = None

    # Not contains default params, because is an encapsulator for GPTModels, so the default are in there
    def __init__(self, model, model_id, model_type, max_input_tokens, max_tokens, bag_tokens, zone, api_version,
                 temperature, stop, models_credentials):
        """It is the object in charge of modifying whether the inputs and the outputs of the gpt models

        :param model: Model name used to specify model
        :param model_id: Model name used in the bedrock endpoint
        :param model_type: Model type used in the bedrock endpoint
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
        self.max_input_tokens = max_input_tokens
        self.max_tokens = max_tokens
        self.bag_tokens = bag_tokens
        self.zone = zone
        self.api_version = api_version
        self.temperature = temperature
        self.stop_sequences = stop

    def set_message(self, config: dict):
        """Sets the message as an argument of the class
           It also modifies the message taking into account the number of tokens.

        :param config: Dict with the message to be used
        """
        config['message'] = self.MODEL_MESSAGE
        message = ManagerMessages().get_message(config)
        queryLimiter = ManagerQueryLimiter.get_limiter({"message": message, "model": self.MODEL_MESSAGE,
                                                        "max_tokens": self.max_input_tokens,
                                                        "bag_tokens": self.bag_tokens,
                                                        "persistence": message.persistence, "querylimiter": "bedrock"})
        self.message = queryLimiter.get_message()

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
        if 'status_code' in response and response['ResponseMetadata']['HTTPStatusCode'] in [400, 401, 404, 408, 500,
                                                                                            502, 503]:
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
                'error_message': "There is no response from the model for the request",
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

        super().__init__(model, model_id, model_type, max_input_tokens, max_tokens, bag_tokens, zone, api_version,
                         temperature, stop, models_credentials)
        self.is_vision = False

    def template_ok(self, template: dict):
        """Check if the template is correct.

        :param template: Template to be checked
        """
        if template:
            try:
                template_dict = eval(template) if not isinstance(template, dict) else template
            except SyntaxError:
                raise ValueError("Template is not well formed, must be a dict {} structure")
            if not template_dict.get("user"):
                raise ValueError("Template must contain the user key")
            if len(template_dict.keys()) != 2:
                raise ValueError("Template must have user and system key once")
            if "$query" not in template_dict.get("user"):
                raise ValueError("Template must contain $query to be replaced")
        else:
            raise ValueError("Template is empty")


class ChatClaude3Model(ClaudeModel):
    MODEL_MESSAGE = "chatClaude3"

    def __init__(self, model: str = 'anthropic.claude-3-sonnet-20240229-v1:0',
                 model_id: str = 'anthropic.claude-3-sonnet-20240229-v1:0',
                 model_type: str = "",
                 max_input_tokens: int = 100000,
                 max_tokens: int = 4000,
                 bag_tokens: int = 500,
                 zone: str = "",
                 api_version: str = "",
                 temperature: float = 0,
                 stop: List = ["end_turn"],
                 models_credentials: dict = None):
        """It is the object in charge of modifying whether the inputs and the outputs of the gpt models

        :param model: Model name used to verify the model
        :param model_id: Model name used in the bedrock endpoint
        :param model_type: Model type used in the bedrock endpoint
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

        super().__init__(model, model_id, model_type, max_input_tokens, max_tokens, bag_tokens, zone, api_version,
                         temperature, stop, models_credentials)
        self.default_template_name = "system_query_v"
        self.is_vision = True

    def template_ok(self, template: dict):
        """Check if the template is correct.

        :param template: Template to be checked
        """
        if template:
            try:
                template_dict = eval(template) if not isinstance(template, dict) else template
            except SyntaxError:
                raise ValueError("Template is not well formed, must be a dict {} structure")
            if not template_dict.get("user"):
                raise ValueError("Template must contain the user key")
            if isinstance(template_dict.get("user"), list):
                self._check_multimodal(template_dict.get("user"))
            elif isinstance(template_dict.get("user"), str):
                pass
            else:
                raise ValueError("Template user must be a list or a string")
            if len(template_dict.keys()) != 2:
                raise ValueError("Template can only have user and system key")
            if "$query" not in template_dict.get("user"):
                raise ValueError("Template must contain $query to be replaced")
        else:
            raise ValueError("Template is empty")

    @staticmethod
    def _check_multimodal(content_list: list):
        for el in content_list:
            if isinstance(el, dict):
                if el.get('type') == 'text':
                    if not el.get('text') or not isinstance(el.get('text'), str):
                        raise ValueError("For type 'text' there must be a key 'text' containing a string")
                elif el.get('type') == 'image_url':
                    image_url = el.get('image_url')
                    if not image_url:
                        raise ValueError("'image_url' param in type 'image_url' is mandatory")
                    if not isinstance(image_url.get('url'), str):
                        raise ValueError("Type 'image_url' must contain a 'image_url' key with a string")
                    if image_url.get('detail'):
                        raise ValueError("'detail' parameter not allowed in claude3 models")
                else:
                    raise ValueError("Key must be 'type' and its value must be one in ['text', 'image_url']")
            elif "$query" in el:
                continue
            else:
                raise ValueError("Elements of the content must be dict {} or a string containing \"$query\"")


class ManagerModel(object):
    MODEL_TYPES = [PromptGPTModel, ChatGPTModel, ChatClaudeModel, DalleModel, ChatClaude3Model, ChatGPTvModel]

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
    def find_pull_model_in_available_models(model_in: str, available_models: List[dict]) -> dict:
        """ Method to find the model in the available models.

        :param model_in: Model name.
        :param available_models: List of available models.
        :return: Model configuration.
        """
        selected_model = None
        available_models = [model for model in available_models if model_in in model['model_pool']]
        if available_models:
            selected_model = copy.deepcopy(random.choice(available_models))
        return selected_model

    @staticmethod
    def delete_not_used_keys(keys: List[str], model_conf: dict) -> dict:
        """ Method to delete keys from the model configuration.

        :param keys: List of keys to be removed.
        :param model_conf: Model configuration.
        :return: Model configuration without the keys.
        """
        for key in keys:
            model_conf.pop(key, None)
        return model_conf

    @staticmethod
    def get_model(conf: dict, platform_name: str, available_models: dict) -> GenerativeModel:
        """ Method to instantiate the model: [gpt3, gpt-3.5-turbo, gpt4]

        :param conf: Model configuration. Example:  {"max_tokens": 1000,"model": "gpt-3.5-turbo"}
        :param platform_name: Platform name.
        :param available_models: List of available models.
        :return: Model object.
        """
        model_in = conf.get('model', 'gpt-3.5-pool-europe')
        ## backward compatibility        model_in = model_in.replace('pull', 'pool')

        available_models = available_models.get(platform_name, [])
        selected_model = ManagerModel.find_model_in_available_models(model_in, available_models)

        if not selected_model and 'pool' in model_in:
            selected_model = ManagerModel.find_pull_model_in_available_models(model_in, available_models)

        if selected_model:
            ## check model message: chatGPT, promptGPT,....
            for model in ManagerModel.MODEL_TYPES:
                if model.get_message_type(selected_model.get('message')):
                    conf.pop('model', None)
                    selected_model.pop('message', None)
                    selected_model.pop('model_pool', None)
                    selected_model.update(conf)
                    try:
                        return model(**selected_model)
                    except TypeError as e:
                        raise ValueError(f"Parameter:{str(e).split('argument')[1]} not supported in model: "
                                         f"'{selected_model.get('model')}'")
        raise ValueError(f"Model: '{conf.get('model')}' model is not supported in platform {platform_name}.")
