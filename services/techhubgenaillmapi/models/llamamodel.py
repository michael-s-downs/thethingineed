
# Native imports
import json

# Local imports
from generatives import GenerativeModel
from message.messagemanager import ManagerMessages
from limiters import ManagerQueryLimiter

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
        queryLimiter = ManagerQueryLimiter.get_limiter({"message": message, "model": self.MODEL_MESSAGE,
                                                        "max_tokens": self.max_input_tokens,
                                                        "bag_tokens": self.bag_tokens,
                                                        "persistence": message.persistence, "querylimiter": "bedrock"})
        self.message = queryLimiter.get_message()

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
                'error_message': "There is no response from the model for the request",
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