### This code is property of the GGAO ###


# Native imports
import re, copy, json

import botocore.exceptions
# Installed imports
import urllib3
import pytest
from unittest.mock import MagicMock, patch, mock_open
import requests

# Local imports
from endpoints import ManagerPlatform, Platform, GPTPlatform, OpenAIPlatform, AzurePlatform, BedrockPlatform
from common.errors.genaierrors import PrintableGenaiError
from common.utils import load_secrets
from generatives import ChatGPTModel, GenerativeModel, DalleModel, ChatGPTVision, ChatClaudeModel, LlamaModel, ChatNova, ChatNovaVision

aws_credentials = {"access_key": "346545", "secret_key": "87968"}
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
    'models_credentials': {"techhubinc-EastUS2": "mock_api"}
}

claude_model = {
    "model": "claude-v2:1-NorthVirginiaEast",
    "model_id": "anthropic.claude-v2:1",
    "model_type": "claude-v2.1",
    "max_input_tokens": 200000,
    "zone": "us-east-1",
    "api_version": "bedrock-2023-05-31",
    'models_credentials': {"us-east-1": "mock_api"}
}

llama3_model = {
    "model": "meta-llama-3-70b-NorthVirginiaEast",
    "model_id": "meta.llama3-70b-instruct-v1:0",
    "model_type": "llama3-v1-70b",
    "max_input_tokens": 8000,
    "zone": "us-east-1",
    "models_credentials": {"us-east-1": "mock_api"}
}

nova_model = {
    "model": "techhubdev-amazon.nova-micro-v1-NorthVirginia",
    "model_id": "amazon.nova-micro-v1:0",
	"model_type": "nova-micro-v1",
	"max_input_tokens": 128000,
	"zone": "us-east-1",
    "models_credentials": {},
    "top_k":100
}

# Message that uses BaseAdapter:
message_dict = {"query": "Hello, how are you?", "template": {
                        "system": "You are a helpful assistant.",
                        "user": "Answer me gently the query: $query"
                        },
                "persistence": []
                }
message_dict_vision = {"query": [
            {
            "type": "text",
            "text": "What color are the cats in both images?"
            },
            {
            "type": "image_url",
            "image":{
                "url": "https://th.bing.com/th/id/R.964f5f4e167f7a4c4391260dd5231e6b?rik=fl5nyYBrK1WNHA&riu=http%3a%2f%2fwww.mundogatos.com%2fUploads%2fmundogatos.com%2fImagenesGrandes%2ffotos-de-gatitos-7.jpg&ehk=muO2GWmBzRiibqUFezM7Nza3wi4TvK6pfesyysMvvYs%3d&risl=&pid=ImgRaw&r=0"
                }
            }
        ],
        "template": {
                        "system": "You are a helpful assistant.",
                        "user": "Answer me gently the query: $query"
                        },
        "persistence": [
            [{
                "role": "user",
                "content":
                [{
                    "type": "image_url",
                        "image": {"url": "https://th.bing.com/th/id/OIP.BEIceF9sNPUL_vM9N3_S_wHaDO?rs=1&pid=ImgDetMain"}
                }]
            },
            {
                "role": "assistant",
                "content": "The cat is my favorite pet in that image."
            }]
        ]}



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
        models_config_manager = MagicMock()
        models_config_manager.get_different_model_from_pool.return_value = model
        models_config_manager.get_model_api_key_by_zone.return_value = "mock_api"
        self.azure_platform = AzurePlatform(aws_credentials, models_urls, timeout=60,num_retries=1,models_config_manager=models_config_manager)  # Initialize your class here

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
        generative_model = ChatGPTModel(**model)
        result_gpt = self.azure_platform.build_url(generative_model)
        generative_model = DalleModel(**model)
        result_dalle = self.azure_platform.build_url(generative_model)
        assert result_gpt == "https://techhubinc-EastUS2.openai.azure.com/openai/deployments/techhubinc-EastUS2-gpt-35-turbo-16k-0613/chat/completions?api-version=2024-02-15-preview"
        assert result_dalle == "https://techhubinc-EastUS2.openai.azure.com/openai/deployments/techhubinc-EastUS2-gpt-35-turbo-16k-0613/images/generations?api-version=2024-02-15-preview"

        generative_model = ChatClaudeModel(**claude_model)
        with pytest.raises(PrintableGenaiError, match=f"Model message {generative_model.MODEL_MESSAGE} not supported."):
            self.azure_platform.build_url(generative_model)
    def test_set_model(self):
        generative_model = ChatGPTVision(**model)
        generative_model.api_key = "mock_api_key"
        self.azure_platform.set_model(generative_model)

        # Assert the expected output
        assert self.azure_platform.generative_model == generative_model
        assert self.azure_platform.headers == {'api-key': "mock_api_key", 'Content-Type': "application/json"}

        # Call the method with a mock generative_model
        gptv_model = copy.deepcopy(model)
        gptv_model['zone'] = "mock_zone"
        generative_model = ChatGPTVision(**gptv_model)
        with pytest.raises(PrintableGenaiError, match=f"Api key not found for model {generative_model.model_name} in Azure Platform"):
            self.azure_platform.set_model(generative_model)


    def test_call_model(self):
        with patch('requests.post') as mock_post:
            mock_object = MagicMock()
            mock_object.json.return_value = {"choices": [{"message": {"content": "asdf"}}],
                                             "status_code": 200,
                                             "usage":{"total_tokens": 1000, "completion_tokens": 501, "prompt_tokens": 154}}
            mock_post.return_value = mock_object
            generative_model = ChatGPTModel(**model)
            generative_model.set_message(message_dict)
            self.azure_platform.set_model(generative_model)
            result = generative_model.get_result(self.azure_platform.call_model())
            assert result['status_code'] == 200
            assert result['result']['answer'] == "asdf"
            assert result['result']['n_tokens'] == 1000
            assert result['result']['output_tokens'] == 501
            assert result['result']['input_tokens'] == 154


    def test_call_model_errors(self):
        generative_model = ChatGPTModel(**model)
        generative_model.set_message(message_dict)
        self.azure_platform.set_model(generative_model)
        with patch('requests.post') as mock_func:
            mock_func.side_effect = requests.exceptions.Timeout
            response = self.azure_platform.call_model()
            result = generative_model.get_result(response)
        assert result['status_code'] == 408

        with patch('requests.post') as mock_func:
            mock_func.side_effect = requests.exceptions.RequestException
            response = self.azure_platform.call_model()
            result = generative_model.get_result(response)
        assert result['status_code'] == 500

        with patch('requests.post') as mock_func:
            mock_func.return_value.status_code = 500
            mock_func.return_value.text = "Internal server error"
            response = self.azure_platform.call_model()
            result = generative_model.get_result(response)
        assert result['status_code'] == 500
        assert result['error_message'] == "Internal server error"

        with patch('requests.post') as mock_func:
            mock_func.return_value.status_code = 429
            mock_func.return_value.text = "OpenAI rate limit exceeded"
            response = self.azure_platform.call_model()
            result = generative_model.get_result(response)
        assert result['status_code'] == 429
        assert result['error_message'] == "OpenAI rate limit exceeded"

class TestBedrockPlatform:
    def setup_method(self):
        models_config_manager = MagicMock()
        models_config_manager.get_different_model_from_pool.return_value = claude_model
        models_config_manager.get_model_api_key_by_zone.return_value = "mock_api"
        self.bedrock_platform = BedrockPlatform(aws_credentials, models_urls, timeout=60, num_retries=1, models_config_manager=models_config_manager)  # Initialize your class here

    def test_init(self):
        assert self.bedrock_platform.aws_credentials == aws_credentials
        assert self.bedrock_platform.models_urls == models_urls
        assert self.bedrock_platform.timeout == 60

    def test_parse_response(self):
        mock_response = {"error": {"code": "401", "message": "Unauthorized"}}
        result = self.bedrock_platform.parse_response(mock_response)

        assert result == {'error': {'code': '401', 'message': 'Unauthorized'}}


    def test_set_model(self):
        generative_model = ChatGPTVision(**model)
        generative_model.api_key = "mock_api_key"
        self.bedrock_platform.set_model(generative_model)
        assert self.bedrock_platform.generative_model == generative_model

    @patch("endpoints.provider", "azure")
    def test_call_model_bedrock(self):
        with patch('boto3.client') as mock_post:
            body = MagicMock()
            body.read.return_value = json.dumps({"content": [{"text": "asdf"}], "usage":{"input_tokens":454, "output_tokens":5454}})
            mock_post.return_value.invoke_model.return_value = {"body": body}
            generative_model = ChatClaudeModel(**claude_model)
            generative_model.set_message(message_dict)
            self.bedrock_platform.set_model(generative_model)
            result = generative_model.get_result(self.bedrock_platform.call_model())
            assert result['status_code'] == 200
            assert result['result']['answer'] == "asdf"
            assert result['result']['input_tokens'] == 454
            assert result['result']['output_tokens'] == 5454

    @patch("endpoints.provider", "azure")
    def test_call_model_bedrock_nova(self):
        with patch('boto3.client') as mock_post:
            body = MagicMock()
            body.read.return_value = json.dumps({"output": {"message": {"content": [{"text": "asdf"}]}}, "usage": {"totalTokens": 1000, "input_tokens": 454, "output_tokens": 5454}})
            mock_post.return_value.invoke_model.return_value = {"body": body}
            model = copy.deepcopy(nova_model)
            model['pool_name'] = None
            generativeModel = ChatNova(**model)
            generativeModel.set_message(message_dict)
            self.bedrock_platform.set_model(generativeModel)
            result = generativeModel.get_result(self.bedrock_platform.call_model())
            assert result['status_code'] == 200
            assert result['result']['answer'] == "asdf"
    @patch("endpoints.provider", "azure")
    def test_call_model_bedrock_nova_vision(self):
        with patch('boto3.client') as mock_post:
            body = MagicMock()
            body.read.return_value = json.dumps({"output": {"message": {"content": [{"text": "asdf"}]}}, "usage": {"totalTokens": 1000, "input_tokens": 454, "output_tokens": 5454}})
            mock_post.return_value.invoke_model.return_value = {"body": body}
            generative_model = ChatNovaVision(**nova_model)
            generative_model.set_message(message_dict_vision)
            self.bedrock_platform.set_model(generative_model)
            result = generative_model.get_result(self.bedrock_platform.call_model())
            assert result['status_code'] == 200
            assert result['result']['answer'] == "asdf"


    @patch("endpoints.provider", "aws")
    def test_call_model_aws(self):
        with patch('boto3.client') as mock_post:
            body = MagicMock()
            body.read.return_value = json.dumps({"generation": "asdf", "generation_token_count": 454, "prompt_token_count": 5454})
            mock_post.return_value.invoke_model.return_value = {"body": body}
            generative_model = LlamaModel(**llama3_model)
            generative_model.set_message(message_dict)
            self.bedrock_platform.set_model(generative_model)
            result = generative_model.get_result(self.bedrock_platform.call_model())
            assert result['status_code'] == 200
            assert result['result']['answer'] == "asdf"
            assert result['result']['output_tokens'] == 454
            assert result['result']['input_tokens'] == 5454



    def test_call_model_errors(self):
        generative_model = ChatClaudeModel(**claude_model)
        generative_model.set_message(message_dict)
        self.bedrock_platform.set_model(generative_model)
        with patch('boto3.client') as mock_func:
            mock_func.side_effect = urllib3.exceptions.ReadTimeoutError(MagicMock(), "test", "test")
            response = self.bedrock_platform.call_model()
            result = generative_model.get_result(response)
        assert result['status_code'] == 408

        with patch('boto3.client') as mock_func:
            mock_func.side_effect = requests.exceptions.RequestException
            response = self.bedrock_platform.call_model()
            result = generative_model.get_result(response)
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
            result = generative_model.get_result(response)
        assert result['status_code'] == 400


        with patch('boto3.client') as mock_func:
            mock_func.side_effect = ConnectionError
            response = self.bedrock_platform.call_model()
            result = generative_model.get_result(response)
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
        generative_model = ChatGPTModel(**model)
        result_gpt = self.openai_platform.build_url(generative_model)
        assert result_gpt == "https://api.openai.com/v1/chat/completions"

        generative_model = ChatClaudeModel(**claude_model)
        with pytest.raises(PrintableGenaiError, match=f"Model message {generative_model.MODEL_MESSAGE} not supported."):
            self.openai_platform.build_url(generative_model)

    def test_set_model(self):
        generative_model = ChatGPTModel(**model)
        generative_model.api_key = "mock_api_key"
        self.openai_platform.set_model(generative_model)

        # Assert the expected output
        assert self.openai_platform.generative_model == generative_model
        assert self.openai_platform.headers == {'Authorization': "Bearer mock_api_key", 'Content-Type': "application/json"}
