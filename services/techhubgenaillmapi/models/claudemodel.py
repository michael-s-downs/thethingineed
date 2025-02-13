
# Native imports
import json
from typing import List

# Local imports
from generatives import GenerativeModel
from message.messagemanager import ManagerMessages
from limiters import ManagerQueryLimiter

class ClaudeModel(GenerativeModel):
    MODEL_MESSAGE = None
    MODEL_QUERY_LIMITER = "bedrock"

    # Not contains default params, because is an encapsulator for ClaudeModels, so the default are in there
    def __init__(self, model, model_id, model_type, pool_name, max_input_tokens, max_tokens, bag_tokens, zone, api_version,
                 temperature, stop, models_credentials, tools):
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
        if tools:
            self.tools = tools


    def parse_data(self) -> json:
        """ Convert message and model data into json format.

        :return: Query in json format to be sent.
        """
        messages = self.message.preprocess()
        system = messages.pop(-2).get('content') # Remove system message from the list
        body = {"messages": messages,
                "temperature": self.temperature,
                "stop_sequences": self.stop_sequences,
                "anthropic_version": self.api_version,
                "max_tokens": self.max_tokens,
                "system": system
                }
        if self.tools:
            body['tools'] = self.tools

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
    GENERATIVE_MODELS = []
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
    GENERATIVE_MODELS = ["claude-3-5-sonnet-v1:0", "claude-3-5-haiku-v1:0", "claude-3-5-sonnet-v2:0", "claude-3-haiku-v1:0", "claude-3-sonnet-v1:0"]

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