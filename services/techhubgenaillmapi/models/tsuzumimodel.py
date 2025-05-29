# Native imports
import json
from typing import List
import re

# Installed imports
import tiktoken

# Local imports
from generatives import GenerativeModel
from common.errors.genaierrors import PrintableGenaiError
from limiters import ManagerQueryLimiter
from message.messagemanager import ManagerMessages

DEFAULT_STOP_MSG = "<|endoftext|>"

NOT_FOUND_MSG = "Not found"

class TsuzumiModel(GenerativeModel):
    MODEL_MESSAGE = "chatTsuzumi"
    GENERATIVE_MODELS = ["tsuzumi-7b-v1_2-8k-instruct"]
    MODEL_QUERY_LIMITER = "tsuzumi"

    def __init__(self, model: str = 'tsuzumi-7b-v1_2-8k-instruct',
                 model_type: str = "",
                 pool_name: str = None,
                 max_input_tokens: int = 4096,
                 max_tokens: int = -1,
                 bag_tokens: int = 500,
                 temperature: float = 1.0,
                 n: int = 1,
                 stop: List = [DEFAULT_STOP_MSG],
                 models_credentials: dict = None,
                 top_p: int = 1,
                 seed: int = None,
                 response_format: str = None,):
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
        :param stop: Up to 4 sequences where the API will stop generating further tokens.
        :param top_p: Top p value to use in the model
        :param models_credentials: Credentials to use the model
        :param seed: Seed to use in the model
        :param response_format: The format in which the generated text is returned.
        """

        super().__init__(models_credentials, "")
        self.model_name = model
        self.model_type = model_type
        self.pool_name = pool_name
        self.max_input_tokens = max_input_tokens
        self.max_tokens = max_tokens
        self.bag_tokens = bag_tokens
        if temperature >= 0.0:
            if temperature >= 0.0 and temperature <= 2.0:
                self.temperature = temperature
            else:
                raise ValueError(f"Temperature must be between 0.0 and 2.0 for the model {self.model_name}")
        self.n = n
        self.stop = stop
        self.top_p = top_p
        self.seed = seed
        self.response_format = response_format
        self.encoding = tiktoken.get_encoding("cl100k_base")
        self.encoding = tiktoken.encoding_for_model("gpt-3.5-turbo")
        self.is_vision = False
        self.tools = False

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
        """Method to format the model response.

        :param response: Dict returned by LLM endpoint.
        :return: Dict with the answer, tokens used and logprobs.
        """
        if 'status_code' in response and response['status_code'] != 200:
            try:
                error_info = json.loads(response.get('msg', '{}')).get('error', {})
                content_filter_info = error_info.get('innererror', {}).get('content_filter_result', {})

                if error_info.get('code') == 'content_filter':
                    triggered_filters = {
                        category: details
                        for category, details in content_filter_info.items()
                        if details.get('filtered')
                    }

                    triggered_str = ', '.join(
                        f"{category}({details.get('severity', 'unknown')})"
                        for category, details in triggered_filters.items()
                    )

                    return {
                        'status': 'error',
                        'error_message': f"Content filter triggered due to the following categories: {triggered_str}.",
                        'status_code': 400
                    }
            except (json.JSONDecodeError, KeyError, TypeError):
                pass

            return {
                'status': 'error',
                'error_message': str(response.get('msg', 'Unknown error')),
                'status_code': response['status_code']
            }

        choice = response.get('choices', [{}])[0]

        if 'message' not in choice:
            raise PrintableGenaiError(400, f"Tsuzumi format is not as expected: {choice}.")

        message = choice.get('message', {})

        if 'tool_calls' in message:
            tool_calls = []
            for tool_call in message.get('tool_calls', []):
                tool_calls.append({
                    "name": tool_call.get('function', {}).get('name', ''),
                    "id": tool_call.get('id', ''),
                    "inputs": json.loads(tool_call.get('function', {}).get('arguments', '{}'))
                })

            return {
                "status": "finished",
                "result": {
                    "answer": message.get('content') or "",
                    "tool_calls": tool_calls,
                    "input_tokens": response['usage']['prompt_tokens'],
                    "n_tokens": response['usage']['total_tokens'],
                    "output_tokens": response['usage']['completion_tokens'],
                    "query_tokens": self.message.user_query_tokens,
                    "cached_tokens": response.get('usage', {}).get('prompt_tokens_details', {}).get('cached_tokens', 0)
                },
                "status_code": 200
            }

        text = message.get('content', '')

        if re.search(NOT_FOUND_MSG, text, re.I):
            answer = NOT_FOUND_MSG
            self.logger.info("Answer not found.")
        else:
            answer = text.strip()

        self.logger.info(f"LLM response: {answer}.")
        return {
            'status': "finished",
            'result': {
                'answer': answer,
                'logprobs': [],
                'n_tokens': response['usage']['total_tokens'],
                'query_tokens': self.message.user_query_tokens,
                'input_tokens': response['usage']['prompt_tokens'],
                'output_tokens': response['usage']['completion_tokens'],
                "cached_tokens": response.get('usage', {}).get('prompt_tokens_details', {}).get('cached_tokens', 0)
            },
            'status_code': 200
        }

    def adapt_tools(self, tools):

        if not tools:
            return tools

        adapted_tools = []

        for tool in tools:
            adapted_tool = {
                "type": "function",
                "function": {
                    "name": tool['name'],
                    "description": tool['description'],
                    "parameters": tool['input_schema']
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
