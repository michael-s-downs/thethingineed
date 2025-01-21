# Native imports
import re, copy, json, io

# Installed imports
import pytest
from unittest.mock import MagicMock, patch

# Local imports
from common.errors.genaierrors import PrintableGenaiError
from common.utils import load_secrets
from common.models_manager import BaseModelConfigManager
from generatives import (ManagerModel, GenerativeModel, ChatGPTModel, GenerativeModel, DalleModel,
                         ChatGPTVision, ChatClaudeModel, ChatClaudeVision, LlamaModel, ChatNova, ChatNovaVision, NovaModel)

gpt_model = {
    "model": "techhubinc-EastUS2-gpt-35-turbo-16k-0613",
    "model_type": "gpt-3.5-turbo-16k",
    "max_input_tokens": 16384,
    "zone": "techhubinc-EastUS2",
    "message": "chatGPT",
    "api_version": "2024-02-15-preview",
    "model_pool": ["techhubinc-pool-us-gpt-3.5-turbo-16k", "techhubinc-pool-world-gpt-3.5-turbo-16k"],
    "models_credentials": {}
}

gpt_v_model = {
    "model": "techhubinc-AustraliaEast-gpt-4o-2024-05-13",
    "model_type": "gpt-4o",
    "max_input_tokens": 128000,
    "zone": "techhubinc-AustraliaEast",
    "message": "chatGPT-v",
    "api_version": "2024-02-15-preview",
    "model_pool": ["techhubinc-pool-world-gpt-4o"],
    "models_credentials": {}
}

claude_model = {
    "model": "claude-v2:1-NorthVirginiaEast",
    "model_id": "anthropic.claude-v2:1",
    "model_type": "claude-v2.1",
    "max_input_tokens": 200000,
    "zone": "us-east-1",
    "message": "chatClaude",
    "api_version": "bedrock-2023-05-31",
    "model_pool": ["anthropic-pool-us-claude-v2.1", "anthropic-pool-world-claude-v2.1"],
    "models_credentials": {}
}

llama3_model = {
    "model": "meta-llama-3-70b-NorthVirginiaEast",
    "model_id": "meta.llama3-70b-instruct-v1:0",
    "model_type": "llama3-v1-70b",
    "max_input_tokens": 8000,
    "zone": "us-east-1",
    "message": "chatLlama3",
    "model_pool": ["llama-v3-70b-pool-world"],
    "models_credentials": {}
}

claude3_model = {
    "model": "claude-3-haiku-ParisWest",
    "model_id": "anthropic.claude-3-haiku-20240307-v1:0",
    "model_type": "claude-v3-haiku",
    "max_input_tokens": 200000,
    "zone": "eu-west-3",
    "message": "chatClaude-v",
    "api_version": "bedrock-2023-05-31",
    "model_pool": ["claude-v3-haiku-pool-europe", "claude-v3-haiku-pool-world"],
    "models_credentials": {}
}

dalle_model = {
    "model": "techhubinc-AustraliaEast-dall-e-3",
    "model_type": "dalle3",
    "max_input_tokens": 4000,
    "zone": "techhubinc-AustraliaEast",
    "message": "dalle",
    "api_version": "2023-12-01-preview",
    "model_pool": ["techhubinc-pool-world-dalle3"],
    "models_credentials": {}
}

nova_model = {
    "model": "techhubdev-amazon.nova-micro-v1-NorthVirginia",
    "model_id": "amazon.nova-micro-v1:0",
	"model_type": "nova-micro-v1",
	"max_input_tokens": 128000,
	"zone": "us-east-1",
	"message": "chatNova",
	"model_pool": [	"techhubdev-pool-us-nova-micro-1:0","techhubdev-pool-world-nova-micro-1:0","techhub-pool-world-nova-micro-1:0"],
    "models_credentials": {}
}

nova_v_model = {
    "model": "techhubdev-amazon.nova-lite-v1-NorthVirginia",
	"model_id": "amazon.nova-lite-v1:0",
	"model_type": "nova-lite-v1",
	"max_input_tokens": 300000,
	"zone": "us-east-1",
	"message": "chatNova-v",
	"model_pool": ["techhubdev-pool-us-nova-lite-1:0","techhubdev-pool-world-nova-lite-1:0","techhub-pool-world-nova-lite-1:0"],
    "models_credentials": {}
}

available_models = {
    "azure": [copy.deepcopy(gpt_model), copy.deepcopy(dalle_model), copy.deepcopy(gpt_v_model)],
    "bedrock": [copy.deepcopy(claude_model), copy.deepcopy(llama3_model), copy.deepcopy(claude3_model), copy.deepcopy(nova_model), copy.deepcopy(nova_v_model)]
}

available_pools = {
    "llama-v3-70b-pool-world": [copy.deepcopy(llama3_model)],
    "anthropic-pool-us-claude-v2.1": [copy.deepcopy(claude_model)],
    "anthropic-pool-world-claude-v2.1": [copy.deepcopy(claude_model)],
    "claude-v3-haiku-pool-europe": [copy.deepcopy(claude3_model)],
    "claude-v3-haiku-pool-world": [copy.deepcopy(claude3_model)],
    "techhubinc-pool-us-gpt-3.5-turbo-16k": [copy.deepcopy(gpt_model)],
    "techhubinc-pool-world-gpt-3.5-turbo-16k": [copy.deepcopy(gpt_model)],
    "techhubinc-pool-world-dalle3": [copy.deepcopy(dalle_model)],
    "techhubinc-pool-world-gpt-4o": [copy.deepcopy(gpt_v_model)],
    "techhubdev-pool-world-nova-micro-1:0": [copy.deepcopy(nova_model)],
    "techhubdev-pool-world-nova-lite-1:0": [copy.deepcopy(nova_v_model)],
    "techhubdev-pool-world-nova-pro-1:0": [copy.deepcopy(nova_v_model)]
}

message_dict = {"query": "Hello, how are you?", "template": {
                        "system": "You are a helpful assistant.",
                        "user": "Answer me gently the query: $query"
                        },
                "persistence": []
                }

message_dict_nova = {
    'query': 'Hello, how are you?',
    'template': {
        'system': 'You are a helpful assistant.',
        'user': 'Answer me gently the query: $query'
    },
    'persistence': [],
    'message': 'chatNova'
}


class TestManagerGeneratives:

    def test_all_messages(self):
        manager_models_config = MagicMock()
        manager_models_config.get_model.return_value = copy.deepcopy(gpt_v_model)
        gptv = ManagerModel.get_model({"model": gpt_v_model['model']}, "azure", available_pools, manager_models_config)
        assert isinstance(gptv, ChatGPTVision)
        manager_models_config.get_model.return_value = copy.deepcopy(gpt_model)
        gpt = ManagerModel.get_model({"model": gpt_model['model']}, "azure", available_pools, manager_models_config)
        assert isinstance(gpt, ChatGPTModel)
        manager_models_config.get_model.return_value = copy.deepcopy(claude_model)
        claude = ManagerModel.get_model({"model": claude_model['model']}, "bedrock", available_pools, manager_models_config)
        assert isinstance(claude, ChatClaudeModel)
        manager_models_config.get_model.return_value = copy.deepcopy(dalle_model)
        dalle = ManagerModel.get_model({"model": dalle_model['model']}, "azure", available_pools, manager_models_config)
        assert isinstance(dalle, DalleModel)
        manager_models_config.get_model.return_value = copy.deepcopy(llama3_model)
        llama3 = ManagerModel.get_model({"model": llama3_model['model']}, "bedrock", available_pools, manager_models_config)
        assert isinstance(llama3, LlamaModel)
        manager_models_config.get_model.return_value = copy.deepcopy(claude3_model)
        claude3 = ManagerModel.get_model({"model": claude3_model['model']}, "bedrock", available_pools, manager_models_config)
        assert isinstance(claude3, ChatClaudeVision)
        manager_models_config.get_model.return_value = copy.deepcopy(nova_model)
        nova = ManagerModel.get_model({"model": nova_model['model']}, "bedrock", available_pools, manager_models_config)
        assert isinstance(nova, ChatNova)
        manager_models_config.get_model.return_value = copy.deepcopy(nova_v_model)
        novav = ManagerModel.get_model({"model": nova_v_model['model']}, "bedrock", available_pools, manager_models_config)
        assert isinstance(novav, ChatNovaVision)


    def test_pool_model(self):
        manager_models_config = MagicMock()
        manager_models_config.get_model.return_value = copy.deepcopy(gpt_v_model)
        gptv = ManagerModel.get_model({"model": "techhubinc-pool-world-gpt-4o"}, "azure", available_pools, manager_models_config)
        assert isinstance(gptv, ChatGPTVision)



class TestGPTModel:

    def test_parse_data(self):
        conf = {
            "model": gpt_model['model'],
            "functions": [
                {
                    "name": "get_age",
                    "description": "Get the age of the user that use the app.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "age": {
                                "type": "string",
                                "description": "user age"
                            }

                        }
                    }
                }
            ],
            "function_call": "auto",
            "seed": 42,
            "response_format": "json_object",
            "max_tokens": 100
        }
        manager_models_config = MagicMock()
        manager_models_config.get_model.return_value = copy.deepcopy(gpt_model)
        gpt = ManagerModel.get_model(conf, "azure", available_pools, manager_models_config)
        gpt.set_message(message_dict)
        gpt.parse_data()
        assert gpt.functions == conf["functions"]

        conf['response_format'] = "dd"
        conf['model'] = gpt_model['model']
        manager_models_config.get_model.return_value = copy.deepcopy(gpt_model)
        with pytest.raises(PrintableGenaiError, match=re.escape(f"Error 400: Response format {conf['response_format']} not "
                                                                f"supported for model {conf['model']} "
                                                                f"(only 'json_object' supported)")):

            gpt = ManagerModel.get_model(conf, "azure", available_pools, manager_models_config)
            gpt.set_message(message_dict)
            gpt.parse_data()


    def test_get_result(self):
        mock_response = {
            'status_code': 200,
            'choices': [{"finish_reason": "content_filter"}],
            'usage': {
                'prompt_tokens': 0,
                'completion_tokens': 0,
                'total_tokens': 0,
            }
        }
        conf = copy.deepcopy(gpt_model)
        conf.pop('model_pool')
        conf.pop('message')
        conf['temperature'] = 0
        conf['max_tokens'] = 100
        conf['n'] = 1
        gpt = ChatGPTModel(**conf)
        gpt.set_message(message_dict)
        result = gpt.get_result(mock_response)
        assert result['status_code'] == 400 and result['error_message'] == "content_filter"

        mock_response['choices'] = [{"finish_reason": "function_call", "message": {"function_call": {"arguments": "age=20"}}}]
        result = gpt.get_result(mock_response)
        assert result['result']['answer'] == "age=20"

        mock_response['choices'] = [{"finish_reason": "ss"}]
        with pytest.raises(PrintableGenaiError, match=re.escape("Error 400: Azure format is not as expected: "
                                                                "{'finish_reason': 'ss'}.")):
            gpt.get_result(mock_response)

        mock_response['choices'] = [{"message": {"content": "Not found"}}]
        result = gpt.get_result(mock_response)
        assert result['result']['answer'] == "Not found"

    def test_temperature_ok(self):
        conf = copy.deepcopy(gpt_model)
        conf.pop('model_pool')
        conf.pop('message')
        conf['temperature'] = 2.5
        with pytest.raises(ValueError, match=re.escape(f"Temperature must be between 0.0 and 2.0 for the model {conf['model']}")):
            ChatGPTModel(**conf)

    def test_repr(self):
        conf = copy.deepcopy(gpt_model)
        conf.pop('model_pool')
        conf.pop('message')
        conf['temperature'] = 0
        conf['max_tokens'] = 100
        conf['n'] = 1
        gpt = ChatGPTModel(**conf)
        expected_output = (f"{{model:{conf['model']}, max_tokens:{conf['max_tokens']}, "
                           f"temperature:{conf['temperature']}, n:{conf['n']}}}")
        assert repr(gpt) == expected_output


class TestClaudeModel:

    def test_get_result(self):
        mock_response = {
            'ResponseMetadata': {'HTTPStatusCode': 200},
            'body': io.StringIO(json.dumps({'content': ''}))
        }
        conf = copy.deepcopy(claude_model)
        conf.pop('model_pool')
        conf.pop('message')
        claude = ChatClaudeModel(**conf)
        result = claude.get_result(mock_response)
        assert result['error_message'] == "There is no response from the model for the request"

        mock_response = {
            'ResponseMetadata': {'HTTPStatusCode': 400},
            'body': io.StringIO(json.dumps({'content': ''}))
        }
        result = claude.get_result(mock_response)
        assert result['error_message'] == {'content': ''}

    def test_temperature_ok(self):
        conf = copy.deepcopy(claude_model)
        conf.pop('model_pool')
        conf.pop('message')
        conf['temperature'] = 2.5
        with pytest.raises(ValueError, match=re.escape(f"Temperature must be between 0.0 and 1.0 for the model {conf['model']}")):
            ChatClaudeModel(**conf)

    def test_repr(self):
        conf = copy.deepcopy(claude_model)
        conf.pop('model_pool')
        conf.pop('message')
        conf['temperature'] = 0
        conf['max_tokens'] = 100
        gpt = ChatClaudeModel(**conf)
        expected_output = (f"{{model:{conf['model']}, max_tokens:{conf['max_tokens']}, "
                           f"temperature:{conf['temperature']}}}")
        assert repr(gpt) == expected_output

class TestLlamaModel:
    def test_persistence(self):
        conf = copy.deepcopy(llama3_model)
        conf.pop('model_pool')
        conf.pop('message')
        llama = LlamaModel(**conf)
        persistence = [{"role": "user", "content": "How are you"}, {"role": "assistant", "content": "I am fine"}]
        llama_persistence = llama._adapt_persistence_llama(persistence)
        assert llama_persistence == "|><|start_header_id|>user<|end_header_id|>How are you<|eot_id|><|start_header_id|>assistant<|end_header_id|>I am fine<|eot_id|>"

    def test_temperature_ok(self):
        conf = copy.deepcopy(llama3_model)
        conf.pop('model_pool')
        conf.pop('message')
        conf['temperature'] = 2.5
        with pytest.raises(ValueError, match=re.escape(f"Temperature must be between 0.0 and 1.0 for the model {conf['model']}")):
            LlamaModel(**conf)

    def test_repr(self):
        conf = copy.deepcopy(llama3_model)
        conf.pop('model_pool')
        conf.pop('message')
        conf['temperature'] = 0
        conf['max_tokens'] = 100
        llama = LlamaModel(**conf)
        expected_output = (f"{{model:{conf['model']}, max_tokens:{conf['max_tokens']}, "
                           f"temperature:{conf['temperature']}}}")
        assert repr(llama) == expected_output

    def test_get_result(self):
        mock_response = {
            'ResponseMetadata': {'HTTPStatusCode': 200},
            'body': io.StringIO(json.dumps(''))
        }
        conf = copy.deepcopy(llama3_model)
        conf.pop('model_pool')
        conf.pop('message')
        llama = LlamaModel(**conf)
        result = llama.get_result(mock_response)
        assert result['error_message'] == "There is no response from the model for the request"

        mock_response = {
            'ResponseMetadata': {'HTTPStatusCode': 400},
            'body': io.StringIO(json.dumps({'content': ''}))
        }
        result = llama.get_result(mock_response)
        assert result['error_message'] == {'content': ''}

        mock_response = {
            'status_code': 400,
            'msg': 'error'
        }
        result = llama.get_result(mock_response)
        assert result['error_message'] == 'error'


class TestDalle:
    def test_parse_data(self):
        conf = copy.deepcopy(dalle_model)
        conf.pop('model_pool')
        conf.pop('message')
        dalle = DalleModel(**conf)
        dalle.set_message(message_dict)
        parsed_data = json.loads(dalle.parse_data())
        assert parsed_data['model'] == dalle.model_name
        assert parsed_data.get('style')

        conf = copy.deepcopy(dalle_model)
        conf.pop('model_pool')
        conf.pop('message')
        conf['model_type'] = "dalle2"
        dalle = DalleModel(**conf)
        dalle.set_message(message_dict)
        parsed_data = json.loads(dalle.parse_data())
        assert parsed_data['model'] == dalle.model_name
        assert not parsed_data.get('style')

        conf = copy.deepcopy(dalle_model)
        conf.pop('model_pool')
        conf.pop('message')
        conf['model_type'] = "dalle"
        dalle = DalleModel(**conf)
        dalle.set_message(message_dict)
        assert not dalle.parse_data()



    def test_repr(self):
        conf = copy.deepcopy(dalle_model)
        conf.pop('model_pool')
        conf.pop('message')
        dalle = DalleModel(**conf)
        expected_output = (f"{{model:{conf['model']}, max_input_tokens:{conf['max_input_tokens']}, "
                           f"size:1024x1024, n:1}}")
        assert repr(dalle) == expected_output


    def test_get_result(self):
        mock_response = {
            'status_code': 200,
            'data': [{"finish_reason": "content_filter"}],
            'usage': {
                'prompt_tokens': 0,
                'completion_tokens': 0,
                'total_tokens': 0,
            }
        }
        conf = copy.deepcopy(dalle_model)
        conf.pop('model_pool')
        conf.pop('message')
        conf['response_format'] = "b64_json"
        dalle = DalleModel(**conf)
        dalle.set_message(message_dict)
        result = dalle.get_result(mock_response)
        assert result['status_code'] == 400 and result['error_message'] == "content_filter"

        mock_response['status_code'] = 400
        mock_response['msg'] = "aaa"
        result = dalle.get_result(mock_response)
        assert result['status_code'] == 400 and result['error_message'] == "aaa"
        mock_response.pop('msg')

        mock_response['data'] = [{"finish_reason": "ss"}]
        mock_response['status_code'] = 200
        with pytest.raises(PrintableGenaiError, match=re.escape("Error 400: Azure format is not as expected: "
                                                                "{'finish_reason': 'ss'}.")):
            dalle.get_result(mock_response)

        mock_response['data'] = [{"b64_json": "asdf"}]
        result = dalle.get_result(mock_response)
        assert result['result']['answer'] == "asdf"

        mock_response['data'] = [{"wrong": {"content": "Not found"}}]
        conf['response_format'] = "dd"
        dalle = DalleModel(**conf)
        dalle.set_message(message_dict)
        with pytest.raises(PrintableGenaiError, match=re.escape(f"Error 400: Azure format is not as expected: "
                                                                f"{mock_response['data'][0]}.")):
            dalle.get_result(mock_response)

        conf['response_format'] = "url"
        dalle = DalleModel(**conf)
        dalle.set_message(message_dict)
        mock_response['data'] = [{"url": "asdf"}]
        result = dalle.get_result(mock_response)
        assert result['result']['answer'] == "asdf"

        mock_response['data'] = [{"wrong": {"content": "Not found"}}]
        with pytest.raises(PrintableGenaiError, match=re.escape(f"Error 400: Azure format is not as expected: "
                                                                f"{mock_response['data'][0]}.")):
            dalle.get_result(mock_response)

class TestNovaModel:
    def test_temperature_ok(self):
        conf = copy.deepcopy(nova_model)
        conf.pop('model_pool')
        conf.pop('message')
        conf['temperature'] = 2.5
        conf['max_tokens'] = 100
        conf['bag_tokens'] = []
        conf['top_p'] = 0.9
        conf['top_k'] = 50
        conf['stop'] = None
        conf['pool_name'] = "techhubdev-pool-world-nova-micro-1:0"
        with pytest.raises(ValueError, match=re.escape(f"Temperature must be between 0.0 and 1.0 for the model {conf['model']}")):
            NovaModel(**conf)

    def test_get_result(self):
        mock_response = {
            'ResponseMetadata': {'HTTPStatusCode': 200},
            'body': io.StringIO(json.dumps({'output': {'message': {'content': ''}}}))
        }
        conf = copy.deepcopy(nova_model)
        conf.pop('model_pool')
        conf.pop('message')
        conf['temperature'] = 0.5
        conf['max_tokens'] = 100
        conf['bag_tokens'] = []
        conf['top_p'] = 0.9
        conf['top_k'] = 50
        conf['stop'] = None
        conf['pool_name'] = "techhubdev-pool-world-nova-micro-1:0"
        nova = NovaModel(**conf)
        result = nova.get_result(mock_response)
        assert result['error_message'] == "There is no response from the model for the request"

        mock_response = {
            'ResponseMetadata': {'HTTPStatusCode': 400},
            'body': io.StringIO(json.dumps({'content': ''}))
        }
        result = nova.get_result(mock_response)
        assert result['error_message'] == {'content': ''}

        mock_response = {
            'status_code': 400,
            'msg': 'error'
        }
        result = nova.get_result(mock_response)
        assert result['error_message'] == 'error'