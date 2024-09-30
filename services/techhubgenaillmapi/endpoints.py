### This code is property of the GGAO ###


# Native imports
import re
import os
import time
import random
import requests
from typing import List
from string import Template
from abc import ABC, abstractmethod

# Installed imports
import boto3

# Custom imports
from generatives import GenerativeModel
from common.logging_handler import LoggerHandler
from common.genai_controllers import provider
from common.services import GENAI_LLM_ENDPOINTS
from common.errors.genaierrors import PrintableGenaiError


class Platform(ABC):
    MODEL_FORMAT = "Platform"

    def __init__(self, aws_credentials, models_credentials, timeout: int = 30):
        """Platform that sustains the model to be used

        :param aws_credentials: AWS credentials
        :param models_credentials: Models credentials
        :param timeout: Timeout for the request
        """

        self.generativeModel = None
        logger_handler = LoggerHandler(GENAI_LLM_ENDPOINTS, level=os.environ.get('LOG_LEVEL', "INFO"))
        self.logger = logger_handler.logger

        self.url: str = None
        self.headers: dict = None
        self.timeout = timeout

        self.aws_credentials = aws_credentials
        self.models_credentials = models_credentials

    @abstractmethod
    def parse_response(self, answer: dict) -> dict:
        """ Test if response is correct (token number issue)

        :param answer: Dict response by the endpoint
        :return: Validated response
        """

    @abstractmethod
    def set_model(self, generativeModel: GenerativeModel):
        """Set the model and configure urls.

        :param generativeModel: Model used to make the query
        """
        self.generativeModel = generativeModel

    @classmethod
    def is_platform_type(cls, model_type):
        """Checks if a given model type is equel to the model format and thus it must be the one to use.
        """
        return model_type == cls.MODEL_FORMAT


class GPTPlatform(Platform):
    MODEL_FORMAT = "GPTPlatform"

    def __init__(self, aws_credentials, models_credentials, timeout: int = 30):
        """Platform that sustains the model to be used

        :param aws_credentials: AWS credentials
        :param models_credentials: Models credentials
        :param timeout: Timeout for the request
        """

        super().__init__(aws_credentials, models_credentials, timeout)

    def call_model(self, delta=0, max_retries=3) -> dict:
        """ Method to send the query to the endpoint

        :param delta: Number of retries
        :param max_retries: Maximum number of retries
        :return: Endpoint response
        """
        try:
            self.logger.info(f"Calling {self.MODEL_FORMAT} service with data {self.generativeModel.parse_data()}")
            answer = requests.post(url=self.url, headers=self.headers,
                                   data=self.generativeModel.parse_data(), timeout=self.timeout)

            if delta < max_retries:
                if answer.status_code == 429:
                    self.logger.warning(f"OpenAI rate limit exceeded, retrying, try {delta + 1}/{max_retries}")
                    raise ConnectionError("OpenAI rate limit exceeded")

                if answer.status_code == 500 and "Internal server error" in answer.text:
                    self.logger.warning(f"Internal server error, retrying, try {delta + 1}/{max_retries}")
                    raise ConnectionError("Internal server error")

            if answer.status_code in [503, 502, 500, 404, 400]:
                self.logger.warning(f"Error: {answer.text}")
                return {"error": answer.text,
                        "msg": str(answer.text),
                        "status_code": answer.status_code}

            self.logger.info(f"LLM response: {answer}.")
            answer = self.parse_response(answer.json())
            return answer

        except requests.exceptions.Timeout:
            self.logger.error(f"The request timed out.")
            return {"error": "The request timed out.", "msg": "The request timed out.", "status_code": 408}
        except requests.exceptions.RequestException as e:
            self.logger.error(f"LLM response: {str(e)}.")
            return {"error": e, "msg": str(e), "status_code": 500}
        except ConnectionError:
            time.sleep(random.random())
            return self.call_model(delta + 1, max_retries)


class OpenAIPlatform(GPTPlatform):
    MODEL_FORMAT = "openai"

    def __init__(self, aws_credentials, models_credentials, timeout: int = 30):
        """Platform that sustains the model to be used

        :param aws_credentials: AWS credentials
        :param models_credentials: Models credentials
        :param timeout: Timeout for the request
        """
        super().__init__(aws_credentials, models_credentials, timeout)

    def parse_response(self, answer):
        """ Test if response is correct (token number issue)

        :param answer: Dict response by the endpoint
        :return: Validated response
        """
        self.logger.debug("Parsing response.")
        if 'error' in answer and 'code' in answer['error'] and answer['error']['code'] == 400:
            return {"error": answer, "msg": str(answer['error']['message']), "status_code": answer['error']['code']}
        elif 'error' in answer and 'code' in answer['error'] and answer['error']['code'] == 'invalid_api_key':
            return {"error": answer, "msg": str(answer['error']['message']), "status_code": 401}
        elif 'error' in answer and answer['error']['status'] == 500:
            message = str(answer['error']['message'])
            match = re.search("(?:you requested )(\d{3,})(?: tokens)", message, re.I)
            if match is None:
                self.logger.error(f"match is None for message {message}.")
                return {"error": answer, "msg": "Match is None", "status_code": 500}
            else:
                new_limit = int(match.group(1)) - self.generativeModel.max_tokens
                message = self.generativeModel.limit_message_tokens(self.generativeModel.message,
                                                                    self.generativeModel.max_tokens - new_limit)
                self.generativeModel.set_message(message)
                answer = self.call_model()
                self.logger.info(f"LLM response: {answer}.")

        self.logger.info("Message processed.")
        return answer

    def build_url(self, generativeModel: GenerativeModel):
        """ Build the url to make the request

        :param generativeModel: Model used to make the query
        :return: Url to make the request
        """
        self.logger.debug("Building url.")
        if generativeModel.MODEL_MESSAGE == "chatGPT":
            url = self.models_credentials.get('OPENAI_GPT_CHAT_URL')
        elif generativeModel.MODEL_MESSAGE == "promptGPT":
            url = self.models_credentials.get('OPENAI_GPT_PROMPT_URL')
        else:
            raise PrintableGenaiError(400, f"Model message {generativeModel.MODEL_MESSAGE} not supported.")

        self.logger.debug("url: %s", url)
        return url

    def set_model(self, generativeModel: GenerativeModel):
        """ Set the model and configure headers and urls.

        :param generativeModel: Model used to make the query
        """
        self.logger.debug("Setting model to use.")
        super().set_model(generativeModel)

        self.headers = {'Authorization': "Bearer " + generativeModel.api_key, 'Content-Type': "application/json"}
        self.url = self.build_url(generativeModel)


class AzurePlatform(GPTPlatform):
    MODEL_FORMAT = "azure"

    def __init__(self, aws_credentials, models_credentials, timeout: int = 60):
        """Platform that sustains the model to be used

        :param aws_credentials: AWS credentials
        :param models_credentials: Models credentials
        :param timeout: Timeout for the request
        """
        super().__init__(aws_credentials, models_credentials, timeout)

    def parse_response(self, answer):
        """ Test if response is correct (token number issue)

        :param answer: Dict response by the endpoint
        :return: Validated response
        """
        self.logger.debug("Parsing response.")
        if 'error' in answer and 'code' in answer['error'] and answer['error']['code'] == '401':
            return {"error": answer, "msg": str(answer['error']['message']),
                    "status_code": int(answer['error']['code'])}
        elif 'error' in answer and 'status' in answer['error'] and answer['error']['status'] == 400:
            return {"error": answer, "msg": str(answer['error']['message']),
                    "status_code": int(answer['error']['status'])}
        elif 'error' in answer:
            message = str(answer['error']['message'])
            match = re.search("(?:you requested )(\d{3,})(?: tokens)", message, re.I)
            if match is None:
                self.logger.error(f"match is None for message {message}.")
                return {"error": answer, "msg": "Match is None", "status_code": 500}
            else:
                new_limit = int(match.group(1)) - self.generativeModel.max_tokens
                message = self.generativeModel.limit_message_tokens(self.generativeModel.message,
                                                                    self.generativeModel.max_tokens - new_limit)
                self.generativeModel.set_message(message)
                answer = self.call_model()
                self.logger.info(f"LLM response: {answer}.")
        self.logger.info("Message processed.")
        return answer

    def build_url(self, generativeModel: GenerativeModel):
        """ Build the url to make the request
        :param generativeModel: Model used to make the query
        :return: Url to make the request
        """
        self.logger.debug("Building url.")
        if generativeModel.MODEL_MESSAGE in ["chatGPT", "chatGPT-v"]:
            template = Template(self.models_credentials.get('AZURE_GPT_CHAT_URL'))
        elif generativeModel.MODEL_MESSAGE == "promptGPT":
            template = Template(self.models_credentials.get('AZURE_GPT_PROMPT_URL'))
        elif generativeModel.MODEL_MESSAGE == "dalle":
            template = Template(self.models_credentials.get('AZURE_DALLE_URL'))
        else:
            raise PrintableGenaiError(400, f"Model message {generativeModel.MODEL_MESSAGE} not supported.")

        url = template.safe_substitute(MODEL=generativeModel.model_name,
                                       ZONE=generativeModel.zone,
                                       API=generativeModel.api_version)
        self.logger.debug("url: %s", url)
        return url

    def set_model(self, generativeModel: GenerativeModel):
        """ Set the model and configure urls.

        :param generativeModel: Model used to make the query
        """
        self.logger.debug("Setting model to use.")
        super().set_model(generativeModel)

        if generativeModel.api_key is not None:
            self.headers = {'api-key': generativeModel.api_key, 'Content-Type': "application/json"}
            self.url = self.build_url(generativeModel)
        else:
            raise PrintableGenaiError(400, f"Model message {generativeModel.MODEL_MESSAGE} not implemented in Azure Platform")


class BedrockPlatform(Platform):
    MODEL_FORMAT = "bedrock"

    def __init__(self, aws_credentials, models_credentials, timeout: int = 30):
        """Platform that sustains the model to be used

        :param aws_credentials: AWS credentials
        :param models_credentials: Models credentials
        :param timeout: Timeout for the request
        """
        super().__init__(aws_credentials, models_credentials, timeout)

    def parse_response(self, answer):
        """ Test if response is correct (token number issue)

        :param answer: Dict response by the endpoint
        :return: Validated response
        """
        self.logger.debug("Parsing response.")
        # TODO Parse response
        self.logger.info("Message processed.")
        return answer

    def set_model(self, generativeModel: GenerativeModel):
        """ Set the model and configure urls.

        :param generativeModel: Model used to make the query
        """
        self.logger.debug("Setting model to use.")
        super().set_model(generativeModel)

    def call_model(self, delta=0, max_retries=3) -> dict:
        """ Method to send the query to the endpoint

        :param delta: Number of retries
        :param max_retries: Maximum number of retries
        :return: Endpoint response
        """
        try:
            self.logger.info(f"Calling {self.MODEL_FORMAT} service with data {self.generativeModel.parse_data()}")
            if provider == "azure":
                bedrock = boto3.client(service_name="bedrock-runtime", region_name=self.generativeModel.zone,
                                       aws_access_key_id=self.aws_credentials['access_key'],
                                       aws_secret_access_key=self.aws_credentials['secret_key'])
            else:
                bedrock = boto3.client(service_name="bedrock-runtime", region_name=self.generativeModel.zone)
            answer = bedrock.invoke_model(body=self.generativeModel.parse_data(),
                                          modelId=self.generativeModel.model_id)
            self.logger.info(f"LLM response: {answer}.")
            answer = self.parse_response(answer)
            return answer

        except requests.exceptions.Timeout:
            self.logger.error(f"The request timed out.")
            return {"error": "The request timed out.", "msg": "The request timed out.", "status_code": 408}
        except requests.exceptions.RequestException as e:
            self.logger.error(f"LLM response: {str(e)}.")
            return {"error": e, "msg": str(e), "status_code": 500}
        except ConnectionError:
            time.sleep(random.random())
            return self.call_model(delta + 1, max_retries)


class ManagerPlatform(object):
    MODEL_TYPES = [OpenAIPlatform, AzurePlatform, BedrockPlatform]

    @staticmethod
    def get_platform(conf: dict) -> Platform:
        """ Method to instantiate the endpoint class: [azure, openai, google]

        :param conf: Model configuration. Example:  {"platform":"openai"}
        """
        for platform in ManagerPlatform.MODEL_TYPES:
            platform_type = conf.get('platform')
            if platform.is_platform_type(platform_type):
                conf.pop('platform')
                return platform(**conf)
        raise PrintableGenaiError(400, f"Platform type doesnt exist: '{conf.get('platform')}'. "
                         f"Possible values: {ManagerPlatform.get_possible_platforms()}")

    @staticmethod
    def get_possible_platforms() -> List:
        """ Method to list the endpoints: [azure, openai]

        :param conf: Model configuration. Example:  {"platform":"openai"}
        """
        return [platform.MODEL_FORMAT for platform in ManagerPlatform.MODEL_TYPES]
