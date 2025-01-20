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
import botocore
from botocore.config import Config
import urllib3

# Custom imports
from generatives import GenerativeModel
from common.logging_handler import LoggerHandler
from common.genai_controllers import provider
from common.services import GENAI_LLM_ENDPOINTS
from common.errors.genaierrors import PrintableGenaiError
from common.models_manager import BaseModelConfigManager

SETTING_MODEL_MSG = "Setting model to use."

MESSAGE_PROCESSED_MSG = "Message processed."

PARSING_RESPONSE_MSG = "Parsing response."

REQUEST_TIMED_OUT_MSG = "The request timed out."


class Platform(ABC):
    MODEL_FORMAT = "Platform"

    def __init__(self, aws_credentials, models_urls, timeout: int = 30, num_retries=3, models_config_manager: BaseModelConfigManager = None):
        """Platform that sustains the model to be used

        :param aws_credentials: AWS credentials
        :param models_urls: Models credentials
        :param timeout: Timeout for the request
        """

        self.generativeModel = None
        logger_handler = LoggerHandler(GENAI_LLM_ENDPOINTS, level=os.environ.get('LOG_LEVEL', "INFO"))
        self.logger = logger_handler.logger

        self.url = None
        self.headers = None
        self.timeout = timeout
        self.num_retries = num_retries

        self.aws_credentials = aws_credentials
        self.models_urls = models_urls
        self.models_config_manager = models_config_manager

    @abstractmethod
    def parse_response(self, answer: dict) -> dict:
        """ Test if response is correct (token number issue)

        :param answer: Dict response by the endpoint
        :return: Validated response
        """

    @abstractmethod
    def call_model(self, delta=0) -> dict:
        """ Method to send the query to the endpoint
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

    def __init__(self, aws_credentials, models_urls, timeout: int = 30, num_retries=3, models_config_manager: BaseModelConfigManager = None):
        """Platform that sustains the model to be used

        :param aws_credentials: AWS credentials
        :param models_urls: Models credentials
        :param timeout: Timeout for the request
        """

        super().__init__(aws_credentials, models_urls, timeout, num_retries, models_config_manager)

    def set_model_retry(self):
        """Set the model and configure urls when a retry has to be done.
        """
        if hasattr(self, "generativeModel"):
            selected_model = self.models_config_manager.get_different_model_from_pool(self.generativeModel.pool_name, 
                                                                                      self.generativeModel.model_name,
                                                                                      self.MODEL_FORMAT)
            if selected_model:
                self.generativeModel.model_name = selected_model.get('model')
                self.generativeModel.zone = selected_model.get('zone')
                self.generativeModel.api_version = selected_model.get('api_version')
                self.generativeModel.api_key = self.models_config_manager.get_model_api_key_by_zone(self.generativeModel.zone,
                                                                                                    self.MODEL_FORMAT)
                self.set_model(self.generativeModel)
            else:
                self.logger.debug(f"Model not found in the pool for retry, retrying with: {self.generativeModel.model_name}")
        else:
            self.logger.error("Not 'generativeModel' param the model cannot be set for retry.")
            raise PrintableGenaiError(400, "Not 'generativeModel' param the model cannot be set for retry.")

    def call_model(self, delta=0) -> dict:
        """ Method to send the query to the endpoint

        :param delta: Number of retries
        :return: Endpoint response
        """
        try:
            data_call = self.generativeModel.parse_data()
            self.logger.info(f"Calling {self.MODEL_FORMAT} service with data {data_call}")

            answer = requests.post(url=self.url, headers=self.headers, data=data_call, timeout=self.timeout)

            if delta < self.num_retries:
                if answer.status_code == 429:
                    self.logger.warning(f"OpenAI rate limit exceeded, retrying, try {delta + 1}/{self.num_retries}")
                    raise ConnectionError("OpenAI rate limit exceeded")

                if answer.status_code == 500 and "Internal server error" in answer.text:
                    self.logger.warning(f"Internal server error, retrying, try {delta + 1}/{self.num_retries}")
                    raise ConnectionError("Internal server error")

            if answer.status_code in [503, 502, 500, 404, 400, 429]:
                self.logger.warning(f"Error: {answer.text}")
                return {"error": answer.text,
                        "msg": str(answer.text),
                        "status_code": answer.status_code}

            self.logger.info(f"LLM response: {answer}.")
            answer = self.parse_response(answer.json())
            return answer

        except requests.exceptions.Timeout:
            self.logger.error(REQUEST_TIMED_OUT_MSG)
            if delta < self.num_retries:
                self.logger.warning(f"Timeout, retrying, try {delta + 1}/{self.num_retries}")
                try:
                    self.set_model_retry()
                    time.sleep(random.randrange(1, 10))
                    return self.call_model(delta + 1)
                except:
                    return {"error": REQUEST_TIMED_OUT_MSG, "msg": REQUEST_TIMED_OUT_MSG, "status_code": 408}
            else:
                return {"error": REQUEST_TIMED_OUT_MSG, "msg": REQUEST_TIMED_OUT_MSG, "status_code": 408}
        except requests.exceptions.RequestException as e:
            self.logger.error(f"LLM response: {str(e)}.")
            return {"error": e, "msg": str(e), "status_code": 500}
        except ConnectionError:
            try:
                self.set_model_retry()
                time.sleep(random.randrange(1, 10))
                return self.call_model(delta + 1)
            except:
                return {"error": "Internal server error", "msg": "Internal server error", "status_code": 500}


class OpenAIPlatform(GPTPlatform):
    MODEL_FORMAT = "openai"

    def __init__(self, aws_credentials, models_urls, timeout: int = 30, num_retries=3, models_config_manager: BaseModelConfigManager = None):
        """Platform that sustains the model to be used

        :param aws_credentials: AWS credentials
        :param models_urls: Models credentials
        :param timeout: Timeout for the request
        """
        super().__init__(aws_credentials, models_urls, timeout, num_retries, models_config_manager)

    def parse_response(self, answer):
        """ Test if response is correct (token number issue)

        :param answer: Dict response by the endpoint
        :return: Validated response
        """
        self.logger.debug(PARSING_RESPONSE_MSG)
        if 'error' in answer and 'code' in answer['error'] and answer['error']['code'] == 400:
            return {"error": answer, "msg": str(answer['error']['message']), "status_code": answer['error']['code']}
        elif 'error' in answer and 'code' in answer['error'] and answer['error']['code'] == 'invalid_api_key':
            return {"error": answer, "msg": str(answer['error']['message']), "status_code": 401}
        elif 'error' in answer:
            return {"error": answer, "msg": str(answer['error']['message']), "status_code": 500}

        self.logger.info(MESSAGE_PROCESSED_MSG)
        return answer

    def build_url(self, generativeModel: GenerativeModel):
        """ Build the url to make the request

        :param generativeModel: Model used to make the query
        :return: Url to make the request
        """
        self.logger.debug("Building url.")
        if generativeModel.MODEL_MESSAGE == "chatGPT":
            url = self.models_urls.get('OPENAI_GPT_CHAT_URL')
        else:
            raise PrintableGenaiError(400, f"Model message {generativeModel.MODEL_MESSAGE} not supported.")

        self.logger.debug("url: %s", url)
        return url

    def set_model(self, generativeModel: GenerativeModel):
        """ Set the model and configure headers and urls.

        :param generativeModel: Model used to make the query
        """
        self.logger.debug(SETTING_MODEL_MSG)
        super().set_model(generativeModel)

        self.headers = {'Authorization': "Bearer " + generativeModel.api_key, 'Content-Type': "application/json"}
        self.url = self.build_url(generativeModel)


class AzurePlatform(GPTPlatform):
    MODEL_FORMAT = "azure"

    def __init__(self, aws_credentials, models_urls, timeout: int = 60, num_retries=3, models_config_manager: BaseModelConfigManager = None):
        """Platform that sustains the model to be used

        :param aws_credentials: AWS credentials
        :param models_urls: Models credentials
        :param timeout: Timeout for the request
        """
        super().__init__(aws_credentials, models_urls, timeout, num_retries, models_config_manager)

    def parse_response(self, answer):
        """ Test if response is correct (token number issue)

        :param answer: Dict response by the endpoint
        :return: Validated response
        """
        self.logger.debug(PARSING_RESPONSE_MSG)
        if 'error' in answer and 'code' in answer['error'] and answer['error']['code'] == '401':
            return {"error": answer, "msg": str(answer['error']['message']),
                    "status_code": int(answer['error']['code'])}
        elif 'error' in answer and 'status' in answer['error'] and answer['error']['status'] == 400:
            return {"error": answer, "msg": str(answer['error']['message']),
                    "status_code": int(answer['error']['status'])}
        elif 'error' in answer:
            return {"error": answer, "msg": answer['error']['message'], "status_code": 500}

        self.logger.info(MESSAGE_PROCESSED_MSG)
        return answer

    def build_url(self, generativeModel: GenerativeModel):
        """ Build the url to make the request
        :param generativeModel: Model used to make the query
        :return: Url to make the request
        """
        self.logger.debug("Building url.")
        if generativeModel.MODEL_MESSAGE in ["chatGPT", "chatGPT-v"]:
            template = Template(self.models_urls.get('AZURE_GPT_CHAT_URL'))
        elif generativeModel.MODEL_MESSAGE == "dalle":
            template = Template(self.models_urls.get('AZURE_DALLE_URL'))
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
            raise PrintableGenaiError(400, f"Api key not found for model {generativeModel.model_name} in Azure Platform")


class BedrockPlatform(Platform):
    MODEL_FORMAT = "bedrock"

    def __init__(self, aws_credentials, models_urls, timeout: int = 30, num_retries=3, models_config_manager: BaseModelConfigManager = None):
        """Platform that sustains the model to be used

        :param aws_credentials: AWS credentials
        :param models_urls: Models credentials
        :param timeout: Timeout for the request
        """
        super().__init__(aws_credentials, models_urls, timeout, num_retries, models_config_manager)

    def set_model_retry(self):
        """Set the model and configure urls when a retry has to be done.
        """
        if hasattr(self, "generativeModel"):
            selected_model = self.models_config_manager.get_different_model_from_pool(self.generativeModel.pool_name, 
                                                                                      self.generativeModel.model_name,
                                                                                      self.MODEL_FORMAT)
            if selected_model:
                self.generativeModel.model_name = selected_model.get('model')
                self.generativeModel.model_id = selected_model.get('model_id')
                self.generativeModel.zone = selected_model.get('zone')
                self.generativeModel.api_version = selected_model.get('api_version')
                self.generativeModel.api_key = self.models_config_manager.get_model_api_key_by_zone(self.generativeModel.zone,
                                                                                                    self.MODEL_FORMAT)
                self.set_model(self.generativeModel)
            else:
                self.logger.debug(f"Model not found in the pool for retry, retrying with: {self.generativeModel.model_id}")
        else:
            self.logger.error("Not 'generativeModel' param the model cannot be set for retry.")
            raise PrintableGenaiError(400, "Not 'generativeModel' param the model cannot be set for retry.")

    def parse_response(self, answer):
        """ Test if response is correct (token number issue)

        :param answer: Dict response by the endpoint
        :return: Validated response
        """
        self.logger.debug(PARSING_RESPONSE_MSG)
        # TODO Parse response
        self.logger.info(MESSAGE_PROCESSED_MSG)
        return answer

    def set_model(self, generativeModel: GenerativeModel):
        """ Set the model and configure urls.

        :param generativeModel: Model used to make the query
        """
        self.logger.debug(SETTING_MODEL_MSG)
        super().set_model(generativeModel)

    def call_model(self, delta=0) -> dict:
        """ Method to send the query to the endpoint

        :param delta: Number of retries
        :return: Endpoint response
        """
        try:
            if delta > self.num_retries:
                return {"error": "Max retries reached", "msg": "Max retries reached", "status_code": 500}
            data_call = self.generativeModel.parse_data()
            self.logger.info(f"Calling {self.MODEL_FORMAT} service with data {data_call}")
            config = Config(read_timeout=self.timeout, connect_timeout=self.timeout, region_name=self.generativeModel.zone)
            if provider == "azure":
                bedrock = boto3.client(service_name="bedrock-runtime",
                                       aws_access_key_id=self.aws_credentials['access_key'],
                                       aws_secret_access_key=self.aws_credentials['secret_key'],
                                       config=config)
            else:
                bedrock = boto3.client(service_name="bedrock-runtime",
                                       config=config)
            answer = bedrock.invoke_model(body=data_call,
                                          modelId=self.generativeModel.model_id)
            self.logger.info(f"LLM response: {answer}.")
            answer = self.parse_response(answer)
            return answer

        except urllib3.exceptions.ReadTimeoutError:
            self.logger.error(REQUEST_TIMED_OUT_MSG)
            if delta < self.num_retries:
                self.logger.warning(f"Timeout, retrying, try {delta + 1}/{self.num_retries}")
                try:
                    self.set_model_retry()
                    time.sleep(random.randrange(1, 10))
                    return self.call_model(delta + 1)
                except:
                    return {"error": REQUEST_TIMED_OUT_MSG, "msg": REQUEST_TIMED_OUT_MSG, "status_code": 408}
            else:
                return {"error": REQUEST_TIMED_OUT_MSG, "msg": REQUEST_TIMED_OUT_MSG, "status_code": 408}
        except requests.exceptions.RequestException as e:
            self.logger.error(f"LLM response: {str(e)}.")
            return {"error": e, "msg": str(e), "status_code": 500}
        except botocore.exceptions.ClientError as error:
            self.logger.error(f"Error calling botocore: {error}")
            message = error.response['Error']['Message']
            status_code = error.response['ResponseMetadata']['HTTPStatusCode']
            return {"error": error, "msg": message, "status_code": status_code}
        except ConnectionError:
            try:
                self.set_model_retry()
                time.sleep(random.randrange(1, 10))
                return self.call_model(delta + 1)
            except:
                return {"error": "Internal server error", "msg": "Internal server error", "status_code": 500}


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
