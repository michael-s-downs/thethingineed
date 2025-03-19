# Native imports
import json
import re
from typing import List

# Local imports
from generatives import GenerativeModel
from message.messagemanager import ManagerMessages
from limiters import ManagerQueryLimiter

class GeminiModel(GenerativeModel):
    MODEL_MESSAGE = None
    MODEL_QUERY_LIMITER = "vertex"

    # Not contains default params, because is an encapsulator for ClaudeModels, so the default are in there
    def __init__(self, model, model_id, model_type, pool_name, max_input_tokens, max_tokens, bag_tokens, zone, top_p,
                 top_k, temperature, stop, models_credentials):
        """It is the object in charge of modifying whether the inputs and the outputs of the gemini models

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
        self.tools = None


    def parse_data(self) -> json:
        """ Convert message and model data into json format.

        :return: Query in json format to be sent.
        """
        messages = self.message.preprocess()
        body = {
                "contents": messages,
                "generationConfig": {
                    "maxOutputTokens": self.max_tokens,
                    "temperature": self.temperature,
                    "topP": self.top_p,
                    "stopSequences": self.stop
                }}
        if self.top_k:
            body['generationConfig']['topK'] = self.top_k
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
        answer = response_body.get('output').get('message').get('content')
        if len(answer) == 0:
            return {
                'status': 'error',
                'error_message': "There is no response from the model for the request",
                'status_code': 400
            }
        self.logger.info(f"LLM response: {answer}.")

        stop_reason = response_body.get("stopReason", "")
        if stop_reason == "tool_use":
            content = response_body.get('output', {}).get('message', {}).get('content', [])
            thinking_text = ""
            answer_text = ""
            tool_calls = []

            for item in content:
                if "text" in item:
                    text = item['text']
                    thinking_match = re.search(r"<thinking>(.*?)</thinking>", text, re.DOTALL)
                    if thinking_match:
                        thinking_text = thinking_match.group(1).strip()
                        answer_text = text.replace(thinking_match.group(0), "").strip()
                    else:
                        answer_text = text.strip()
                elif "toolUse" in item:
                    tool_calls.append({
                        "name": item["toolUse"].get('name', ''),
                        "id": item["toolUse"].get('toolUseId', ''),
                        "inputs": item["toolUse"].get('input', {})
                    })

            result = {
                "status": "finished",
                "result": {
                    "thinking": thinking_text,
                    "answer": answer_text,
                    "tool_calls": tool_calls,
                    "input_tokens": response_body.get('usage', {}).get('inputTokens', 0),
                    "n_tokens": response_body.get('usage', {}).get('totalTokens', 0),
                    "output_tokens": response_body.get('usage', {}).get('outputTokens', 0),
                    "query_tokens": self.message.user_query_tokens
                },
                "status_code": 200
            }
            return result
        elif self.tools is not None:
            content = response_body.get('output', {}).get('message', {}).get('content', [])
            thinking_text = ""
            answer_text = ""
            for item in content:
                text = item.get('text', '')
                if text:
                    thinking_match = re.search(r"<thinking>(.*?)</thinking>", text, re.DOTALL)
                    if thinking_match:
                        thinking_text = thinking_match.group(1).strip()
                        answer_text = text.replace(thinking_match.group(0), "").strip()
                    else:
                        answer_text = text.strip()
            result = {
                "status": "finished",
                "result": {
                    "thinking": thinking_text,
                    "answer": answer_text,
                    "input_tokens": response_body.get('usage', {}).get('inputTokens', 0),
                    "n_tokens": response_body.get('usage', {}).get('totalTokens', 0),
                    "output_tokens": response_body.get('usage', {}).get('outputTokens', 0),
                    "query_tokens": self.message.user_query_tokens
                },
                "status_code": 200
            }
            return result


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

    def adapt_tools(self, tools): #TODO adapt this fuction for gemini models

        if not tools:
            return tools
        adapted_tools = []

        for tool in tools:
            adapted_tool = {
                "toolSpec": {
                    "name": tool['name'],
                    "description": tool['description'],
                    "inputSchema": {
                        "json": tool['input_schema']
                    }
                }
            }

            adapted_tools.append(adapted_tool)

        return {"tools": adapted_tools}

class ChatGemini(GeminiModel):
    MODEL_MESSAGE = "chatGemini"
    GENERATIVE_MODELS = []

    def __init__(self,
                 model,
                 model_id,
                 model_type,
                 pool_name,
                 top_k: int = None,
                 max_input_tokens: int = 128000,
                 max_tokens: int = 5000,
                 bag_tokens: int = 500,
                 zone: str = "us-east-1",
                 top_p: float = 0.9,
                 temperature: float = 0.7,
                 stop: List = ["end_turn"],
                 models_credentials: dict = None,
                 tools: List = None):
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
        self.tools = self.adapt_tools(tools)


class ChatGeminiVision(GeminiModel):
    MODEL_MESSAGE = "chatGemini-v"
    DEFAULT_TEMPLATE_NAME = "system_query_v"
    GENERATIVE_MODELS = ["gemini-1.5-pro", "gemini-2.0-flash"]

    def __init__(self,
                 model,
                 model_id,
                 model_type,
                 pool_name: str = None,
                 top_k: int = None,
                 max_input_tokens: int = 128000,
                 max_tokens: int = 5000,
                 bag_tokens: int = 500,
                 zone: str = "us-east-1",
                 top_p: float = 0.9,
                 temperature: float = 0.7,
                 stop: List = ["end_turn"],
                 models_credentials: dict = None,
                 max_img_size_mb: float = 20.00,
                 tools: List = None):
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
        self.tools = self.adapt_tools(tools)