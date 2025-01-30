
# Native imports
import json
from typing import List
import re

# Installed imports
import tiktoken

# Local imports
from generatives import GenerativeModel
from common.errors.genaierrors import PrintableGenaiError

DEFAULT_STOP_MSG = "<|endoftext|>"

NOT_FOUND_MSG = "Not found"

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
        self.tools = None

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
        if self.tools:
            data['tools'] = self.tools

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

    def adapt_tools(self, tools):
        adapted_tools = []

        for tool in tools:
            adapted_tool = {
                "type": "function",
                "function": {
                    "name": tool["name"],
                    "description": tool["description"],
                    "parameters": tool["input_schema"]
                }
            }

            adapted_tools.append(adapted_tool)

        return adapted_tools

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
                 response_format: str = None,
                 tools: list = None):
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
        if tools:
            self.tools = self.adapt_tools(tools)


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
                 max_img_size_mb: float = 20.00,
                 tools: list = None):
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
        if tools:
            self.tools = self.adapt_tools(tools)