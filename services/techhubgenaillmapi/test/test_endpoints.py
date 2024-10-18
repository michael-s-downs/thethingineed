### This code is property of the GGAO ###


# Native imports
import re, copy, os

import botocore.exceptions
# Installed imports
import pytest
from unittest.mock import MagicMock, patch
import requests

# Local imports
from endpoints import ManagerPlatform, Platform, GPTPlatform, OpenAIPlatform, AzurePlatform, BedrockPlatform
from common.errors.genaierrors import PrintableGenaiError
from common.utils import load_secrets
from generatives import ChatGPTModel, GenerativeModel, DalleModel, ChatGPTvModel, ChatClaudeModel, LlamaModel

models_credentials, aws_credentials = load_secrets(vector_storage_needed=False)
models_urls = {
        "AZURE_DALLE_URL": "https://$ZONE.openai.azure.com/openai/deployments/$MODEL/images/generations?api-version=$API",
        "AZURE_GPT_CHAT_URL": "https://$ZONE.openai.azure.com/openai/deployments/$MODEL/chat/completions?api-version=$API",
		"AZURE_EMBEDDINGS_URL": "https://$ZONE.openai.azure.com/",
        "OPENAI_GPT_CHAT_URL": "https://api.openai.com/v1/chat/completions"
}

model = {
    "model": "techhubinc-EastUS2-gpt-35-turbo-16k-0613",
    "model_type": "gpt-3.5-turbo-16k",
    "max_input_tokens": 16384,
    "zone": "techhubinc-EastUS2",
    "api_version": "2024-02-15-preview",
    'models_credentials': models_credentials.get('api-keys').get('azure', {})
}

claude_model = {
    "model": "claude-v2:1-NorthVirginiaEast",
    "model_id": "anthropic.claude-v2:1",
    "model_type": "claude-v2.1",
    "max_input_tokens": 200000,
    "zone": "us-east-1",
    "api_version": "bedrock-2023-05-31",
    'models_credentials': models_credentials.get('api-keys').get('bedrock', {})
}

llama3_model = {
    "model": "meta-llama-3-70b-NorthVirginiaEast",
    "model_id": "meta.llama3-70b-instruct-v1:0",
    "model_type": "llama3-v1-70b",
    "max_input_tokens": 8000,
    "zone": "us-east-1",
    "models_credentials": models_credentials.get('api-keys').get('bedrock', {})
}

# Message that uses BaseAdapter:
message_dict = {"query": "Hello, how are you?", "template": {
                        "system": "You are a helpful assistant.",
                        "user": "Answer me gently the query: $query"
                        },
                "persistence": []
                }



class TestManagerPlatform:
    conf = {'aws_credentials': aws_credentials, 'models_urls': models_urls, 'platform': ''}

    def test_get_possible_platforms(self):
        platforms = ManagerPlatform.get_possible_platforms()
        assert platforms == ["openai", "azure", "bedrock"]

    def test_all_platforms(self):
        self.conf['platform'] = "openai"
        platform_openai = ManagerPlatform.get_platform(self.conf)
        self.conf['platform'] = "azure"
        platform_azure = ManagerPlatform.get_platform(self.conf)
        self.conf['platform'] = "bedrock"
        platform_bedrock = ManagerPlatform.get_platform(self.conf)

        assert isinstance(platform_openai, OpenAIPlatform)
        assert isinstance(platform_azure, AzurePlatform)
        assert isinstance(platform_bedrock, BedrockPlatform)

    def test_wrong_platform(self):
        self.conf['platform'] = "nonexistent"
        with pytest.raises(PrintableGenaiError, match=re.escape(f"Error 400: Platform type doesnt exist: '{self.conf.get('platform')}'. "
                         f"Possible values: {ManagerPlatform.get_possible_platforms()}")):
            ManagerPlatform.get_platform(self.conf)

class TestAzurePlatform:
    def setup_method(self):
        self.azure_platform = AzurePlatform(aws_credentials, models_urls, timeout=60)  # Initialize your class here

    def test_init(self):
        assert self.azure_platform.aws_credentials == aws_credentials
        assert self.azure_platform.models_urls == models_urls
        assert self.azure_platform.timeout == 60

    def test_parse_response(self):
        # Call the method with a mock response
        mock_response = {"error": {"code": "401", "message": "Unauthorized"}}
        result = self.azure_platform.parse_response(mock_response)

        expected_result = {"error": mock_response, "msg": "Unauthorized", "status_code": 401}
        assert result == expected_result

        mock_response = {"error": {"status": 400, "message": "Not Found"}}
        result = self.azure_platform.parse_response(mock_response)
        expected_result = {"error": mock_response, "msg": "Not Found", "status_code": 400}
        assert result == expected_result

        mock_response = {"error": {"status": "500", "message": "Bad Request"}}
        result = self.azure_platform.parse_response(mock_response)
        expected_result = {"error": mock_response, "msg": "Bad Request", "status_code": 500}
        assert result == expected_result

        mock_response = {"result": {"status_code": 200, "message": "asdf"}}
        result = self.azure_platform.parse_response(mock_response)
        assert result == mock_response

    def test_build_url(self):
        generativeModel = ChatGPTModel(**model)
        result_gpt = self.azure_platform.build_url(generativeModel)
        generativeModel = DalleModel(**model)
        result_dalle = self.azure_platform.build_url(generativeModel)
        assert result_gpt == "https://techhubinc-EastUS2.openai.azure.com/openai/deployments/techhubinc-EastUS2-gpt-35-turbo-16k-0613/chat/completions?api-version=2024-02-15-preview"
        assert result_dalle == "https://techhubinc-EastUS2.openai.azure.com/openai/deployments/techhubinc-EastUS2-gpt-35-turbo-16k-0613/images/generations?api-version=2024-02-15-preview"

        generativeModel = ChatClaudeModel(**claude_model)
        with pytest.raises(PrintableGenaiError, match=f"Model message {generativeModel.MODEL_MESSAGE} not supported."):
            self.azure_platform.build_url(generativeModel)
    def test_set_model(self):
        generativeModel = ChatGPTvModel(**model)
        generativeModel.api_key = "mock_api_key"
        self.azure_platform.set_model(generativeModel)

        # Assert the expected output
        assert self.azure_platform.generativeModel == generativeModel
        assert self.azure_platform.headers == {'api-key': "mock_api_key", 'Content-Type': "application/json"}

        # Call the method with a mock generativeModel
        gptv_model = copy.deepcopy(model)
        gptv_model['zone'] = "mock_zone"
        generativeModel = ChatGPTvModel(**gptv_model)
        with pytest.raises(PrintableGenaiError, match=f"Model message {generativeModel.MODEL_MESSAGE} not implemented in Azure Platform"):
            self.azure_platform.set_model(generativeModel)


    def test_call_model(self):
        generativeModel = ChatGPTModel(**model)
        generativeModel.set_message(message_dict)
        self.azure_platform.set_model(generativeModel)
        result = generativeModel.get_result(self.azure_platform.call_model())
        assert result['status_code'] == 200


    def test_call_model_errors(self):
        generativeModel = ChatGPTModel(**model)
        generativeModel.set_message(message_dict)
        self.azure_platform.set_model(generativeModel)
        with patch('requests.post') as mock_func:
            mock_func.side_effect = requests.exceptions.Timeout
            response = self.azure_platform.call_model()
            result = generativeModel.get_result(response)
        assert result['status_code'] == 408

        with patch('requests.post') as mock_func:
            mock_func.side_effect = requests.exceptions.RequestException
            response = self.azure_platform.call_model()
            result = generativeModel.get_result(response)
        assert result['status_code'] == 500

        with patch('requests.post') as mock_func:
            mock_func.return_value.status_code = 500
            mock_func.return_value.text = "Internal server error"
            response = self.azure_platform.call_model()
            result = generativeModel.get_result(response)
        assert result['status_code'] == 500
        assert result['error_message'] == "Internal server error"

        with patch('requests.post') as mock_func:
            mock_func.return_value.status_code = 429
            mock_func.return_value.text = "OpenAI rate limit exceeded"
            response = self.azure_platform.call_model()
            result = generativeModel.get_result(response)
        assert result['status_code'] == 429
        assert result['error_message'] == "OpenAI rate limit exceeded"

class TestBedrockPlatform:
    def setup_method(self):
        self.bedrock_platform = BedrockPlatform(aws_credentials, models_urls, timeout=60)

    def test_init(self):
        assert self.bedrock_platform.aws_credentials == aws_credentials
        assert self.bedrock_platform.models_urls == models_urls
        assert self.bedrock_platform.timeout == 60

    def test_parse_response(self):
        mock_response = {"error": {"code": "401", "message": "Unauthorized"}}
        result = self.bedrock_platform.parse_response(mock_response)

        assert result == {'error': {'code': '401', 'message': 'Unauthorized'}}


    def test_set_model(self):
        generativeModel = ChatGPTvModel(**model)
        generativeModel.api_key = "mock_api_key"
        self.bedrock_platform.set_model(generativeModel)
        assert self.bedrock_platform.generativeModel == generativeModel

    @patch("endpoints.provider", "azure")
    def test_call_model_bedrock(self):
        generativeModel = ChatClaudeModel(**claude_model)
        generativeModel.set_message(message_dict)
        self.bedrock_platform.set_model(generativeModel)
        result = generativeModel.get_result(self.bedrock_platform.call_model())
        assert result['status_code'] == 200

    @patch("endpoints.provider", "aws")
    def test_call_model_aws(self):
        generativeModel = LlamaModel(**llama3_model)
        generativeModel.set_message(message_dict)
        self.bedrock_platform.set_model(generativeModel)
        result = generativeModel.get_result(self.bedrock_platform.call_model())
        assert result['status_code'] == 200


    def test_call_model_errors(self):
        generativeModel = ChatClaudeModel(**claude_model)
        generativeModel.set_message(message_dict)
        self.bedrock_platform.set_model(generativeModel)
        with patch('boto3.client') as mock_func:
            mock_func.side_effect = requests.exceptions.Timeout
            response = self.bedrock_platform.call_model()
            result = generativeModel.get_result(response)
        assert result['status_code'] == 408

        with patch('boto3.client') as mock_func:
            mock_func.side_effect = requests.exceptions.RequestException
            response = self.bedrock_platform.call_model()
            result = generativeModel.get_result(response)
        assert result['status_code'] == 500

        client_error_response = {
            'Error': {
                'Code': '400',
                'Message': 'Bad Request'
            },
            'message': 'Bad Request',
            'ResponseMetadata': {
                'HTTPStatusCode': 400
            }
        }
        with patch('boto3.client') as mock_func:
            mock_func.side_effect = botocore.exceptions.ClientError(client_error_response, "Call model")
            response = self.bedrock_platform.call_model()
            result = generativeModel.get_result(response)
        assert result['status_code'] == 400


        with patch('boto3.client') as mock_func:
            mock_func.side_effect = ConnectionError
            response = self.bedrock_platform.call_model()
            result = generativeModel.get_result(response)
        assert result['status_code'] == 500
        assert result['error_message'] == "Max retries reached"


class TestOpenAIPlatform:
    def setup_method(self):
        self.openai_platform = OpenAIPlatform(aws_credentials, models_urls, timeout=60)  # Initialize your class here

    def test_init(self):
        assert self.openai_platform.aws_credentials == aws_credentials
        assert self.openai_platform.models_urls == models_urls
        assert self.openai_platform.timeout == 60

    def test_parse_response(self):
        # Call the method with a mock response
        mock_response = {"error": {"code": 400, "message": "Unauthorized"}}
        result = self.openai_platform.parse_response(mock_response)
        expected_result = {"error": mock_response, "msg": "Unauthorized", "status_code": 400}
        assert result == expected_result

        mock_response = {"error": {"code": "invalid_api_key", "message": "Not Found"}}
        result = self.openai_platform.parse_response(mock_response)
        expected_result = {"error": mock_response, "msg": "Not Found", "status_code": 401}
        assert result == expected_result

        mock_response = {"error": {"status": 500, "message": "Bad Request"}}
        result = self.openai_platform.parse_response(mock_response)
        expected_result = {"error": mock_response, "msg": "Bad Request", "status_code": 500}
        assert result == expected_result

        mock_response = {"result": {"status_code": 200, "message": "asdf"}}
        result = self.openai_platform.parse_response(mock_response)
        assert result == mock_response

    def test_build_url(self):
        generativeModel = ChatGPTModel(**model)
        result_gpt = self.openai_platform.build_url(generativeModel)
        assert result_gpt == "https://api.openai.com/v1/chat/completions"

        generativeModel = ChatClaudeModel(**claude_model)
        with pytest.raises(PrintableGenaiError, match=f"Model message {generativeModel.MODEL_MESSAGE} not supported."):
            self.openai_platform.build_url(generativeModel)

    def test_set_model(self):
        generativeModel = ChatGPTModel(**model)
        generativeModel.api_key = "mock_api_key"
        self.openai_platform.set_model(generativeModel)

        # Assert the expected output
        assert self.openai_platform.generativeModel == generativeModel
        assert self.openai_platform.headers == {'Authorization': "Bearer mock_api_key", 'Content-Type': "application/json"}
