# Native imports
import re, copy, json, io

# Installed imports
import pytest
from unittest.mock import MagicMock, patch

# Local imports
from common.errors.genaierrors import PrintableGenaiError
from common.utils import load_secrets
from models.managergeneratives import ManagerModel

from models.gptmodel import ChatGPTModel, DalleModel, ChatGPTVision, ChatGPTOModel, ChatGPTOVisionModel
from models.claudemodel import ChatClaudeModel, ChatClaudeVision
from models.llamamodel import LlamaModel
from models.novamodel import ChatNova, ChatNovaVision, NovaModel
from models.geminimodel import ChatGeminiVision

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
    "model_type": "llama3-70b-v1",
    "max_input_tokens": 8000,
    "zone": "us-east-1",
    "message": "chatLlama3",
    "model_pool": ["llama-v3-70b-pool-world"],
    "models_credentials": {}
}

claude3_model = {
    "model": "claude-3-haiku-ParisWest",
    "model_id": "anthropic.claude-3-haiku-20240307-v1:0",
    "model_type": "claude-3-5-haiku-v1:0",
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

o_model={
    "model": "techhubdev-SwedenCentral-gpt-o3-mini-2025-01-31",
    "model_type": "gpt-o3-mini",
    "max_input_tokens": 200000,
    "zone": "techhubdev-SwedenCentral",
    "api_version": "2024-12-01-preview",
    "model_pool": ["techhubdev-pool-eu-gpt-o3-mini","techhubdev-pool-world-gpt-o3-mini", "techhub-pool-world-gpt-o3-mini"],
    "models_credentials": {}
}

o1_mini_model = {
    "model": "techhubdev-SwedenCentral-gpt-o1-mini-2024-09-12",
    "model_type": "gpt-o1-mini",
    "max_input_tokens": 128000,
    "zone": "techhubdev-SwedenCentral",
    "api_version": "2024-12-01-preview",
    "model_pool": ["techhubdev-pool-eu-gpt-o3-mini","techhubdev-pool-world-gpt-o3-mini", "techhub-pool-world-gpt-o3-mini"],
    "models_credentials": {}
}

o_v_model={
    "model": "techhubdev-SwedenCentral-gpt-o1-2024-12-17",
    "model_type": "gpt-o1",
	"max_input_tokens": 200000,
	"max_img_size_mb": 20.00,
	"zone": "techhubdev-SwedenCentral",
	"api_version": "2024-12-01-preview",
	"model_pool": ["techhubdev-pool-eu-gpt-o1","techhubdev-pool-world-gpt-o1","techhub-pool-world-gpt-o1"],
    "models_credentials": {}
}

gemini_model={
    "model": "techhubdev-gemini-2.0-flash-exp",
    "model_id": "gemini-2.0-flash-exp",
    "model_type": "gemini-2.0-flash",
    "max_input_tokens": 4000,
    "zone": "us-central1",
    "message": "chatGemini-v",
    "model_pool": [],
    "models_credentials": {}
}

available_models = {
    "azure": [copy.deepcopy(gpt_model), copy.deepcopy(dalle_model), copy.deepcopy(gpt_v_model)],
    "bedrock": [copy.deepcopy(claude_model), copy.deepcopy(llama3_model), copy.deepcopy(claude3_model), copy.deepcopy(nova_model), copy.deepcopy(nova_v_model)],
    "vertex": [copy.deepcopy(gemini_model)]
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
        manager_models_config.get_model.return_value = copy.deepcopy(o_model)
        chatgpto = ManagerModel.get_model({"model": o_model['model'], "reasoning_effort": "low"}, "azure", available_pools, manager_models_config)
        assert isinstance(chatgpto, ChatGPTOModel)
        manager_models_config.get_model.return_value = copy.deepcopy(o1_mini_model)
        chatgpt_o1_mini = ManagerModel.get_model({"model": o1_mini_model['model']}, "azure",available_pools, manager_models_config)
        assert isinstance(chatgpt_o1_mini, ChatGPTOModel)
        manager_models_config.get_model.return_value = copy.deepcopy(o_v_model)
        chatgptovision = ManagerModel.get_model({"model": o_v_model['model'], "reasoning_effort": "low"}, "azure", available_pools, manager_models_config)
        assert isinstance(chatgptovision, ChatGPTOVisionModel)
        manager_models_config.get_model.return_value = copy.deepcopy(gemini_model)
        geminiv = ManagerModel.get_model({"model": gemini_model['model']}, "vertex", available_pools, manager_models_config)
        assert isinstance(geminiv, ChatGeminiVision)


    def test_pool_model(self):
        manager_models_config = MagicMock()
        manager_models_config.get_model.return_value = copy.deepcopy(gpt_v_model)
        gptv = ManagerModel.get_model({"model": "techhubinc-pool-world-gpt-4o"}, "azure", available_pools, manager_models_config)
        assert isinstance(gptv, ChatGPTVision)

    def test_get_model_error(self):
        manager_models_config = MagicMock()
        manager_models_config.get_model.return_value = copy.deepcopy(o_model)

        with pytest.raises(PrintableGenaiError) as exc_info:
            ManagerModel.get_model({"model": "techhubdev-SwedenCentral-gpt-o3-mini-2025-01-31", "max_tokens": 1000}, "azure", available_pools, manager_models_config)

        assert "Parameter: max_tokens not supported in model: techhubdev-SwedenCentral-gpt-o3-mini-2025-01-31"

    def test_get_model_reasoning_effort_error(self):
        manager_models_config = MagicMock()
        manager_models_config.get_model.return_value = copy.deepcopy(o1_mini_model)

        with pytest.raises(ValueError) as exc_info:
            ManagerModel.get_model({"model": "techhubdev-SwedenCentral-gpt-o1-mini-2024-09-12", "reasoning_effort": "low"}, "azure", available_pools, manager_models_config)

        assert "Parameter: 'reasoning_effort' not supported in model: 'gpt-o1-mini'."

    def test_get_model_stop_error(self):
        manager_models_config = MagicMock()
        manager_models_config.get_model.return_value = copy.deepcopy(o1_mini_model)

        with pytest.raises(ValueError) as exc_info:
            ManagerModel.get_model({"model": "techhubdev-SwedenCentral-gpt-o1-mini-2024-09-12", "stop": []}, "azure", available_pools, manager_models_config)

        assert "Parameter: 'stop' not supported in model: 'gpt-o1-mini'."

class TestGPTModel:

    def test_parse_data(self):
        conf = {
            "model": gpt_model['model'],
            "seed": 42,
            "response_format": "json_object",
            "max_tokens": 100,
            "tools":[
                {
                    "name": "print_sentiment_scores",
                    "description": "Prints the sentiment scores of a given text.",
                    "input_schema": {
                        "type": "object",
                        "properties": {
                            "positive_score": {"type": "number", "description": "The positive sentiment score, ranging from 0.0 to 1.0."},
                            "negative_score": {"type": "number", "description": "The negative sentiment score, ranging from 0.0 to 1.0."},
                            "neutral_score": {"type": "number", "description": "The neutral sentiment score, ranging from 0.0 to 1.0."}
                        },
                        "required": ["positive_score", "negative_score", "neutral_score"]
                    }
                }
            ]
        }
        manager_models_config = MagicMock()
        manager_models_config.get_model.return_value = copy.deepcopy(gpt_model)
        gpt = ManagerModel.get_model(conf, "azure", available_pools, manager_models_config)
        gpt.set_message(message_dict)
        gpt.parse_data()

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

    def test_tools(self):
        message = {"query": "The product was okay, but the customer service was terrible. I probably won't buy from them again.", "template": {
            "system": "You are a helpful assistant.",
            "user": "Answer me gently the query: $query"
        }
        }
        mock_response = {'choices': [{'content_filter_results': {}, 'finish_reason': 'tool_calls', 'index': 0, 'logprobs': None, 'message': {'content': None, 'refusal': None, 'role': 'assistant', 'tool_calls': [{'function': {'arguments': '{"positive_score":0.1,"negative_score":0.7,"neutral_score":0.2}', 'name': 'print_sentiment_scores'}, 'id': 'call_AzZFOq16VLlCccgHOy8IOnSF', 'type': 'function'}]}}],'usage': {'completion_tokens': 33, 'completion_tokens_details': {'accepted_prediction_tokens': 0, 'audio_tokens': 0, 'reasoning_tokens': 0, 'rejected_prediction_tokens': 0}, 'prompt_tokens': 137, 'prompt_tokens_details': {'audio_tokens': 0, 'cached_tokens': 0}, 'total_tokens': 170}}
        conf = copy.deepcopy(gpt_model)
        conf.pop('model_pool')
        conf.pop('message')
        conf['temperature'] = 0
        conf['max_tokens'] = 100
        conf['n'] = 1
        conf['tools'] = [
            {
                "name": "print_sentiment_scores",
                "description": "Prints the sentiment scores of a given text.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "positive_score": {"type": "number", "description": "The positive sentiment score, ranging from 0.0 to 1.0."},
                        "negative_score": {"type": "number", "description": "The negative sentiment score, ranging from 0.0 to 1.0."},
                        "neutral_score": {"type": "number", "description": "The neutral sentiment score, ranging from 0.0 to 1.0."}
                    },
                    "required": ["positive_score", "negative_score", "neutral_score"]
                }
            }
        ]
        gpt = ChatGPTModel(**conf)
        gpt.set_message(message)
        result = gpt.get_result(mock_response)
        assert result['status_code'] == 200 and 'tool_calls' in result['result']

        conf.pop('temperature')
        conf.pop('max_tokens')
        gpt = ChatGPTOModel(**conf)
        gpt.set_message(message)
        result = gpt.get_result(mock_response)
        assert result['status_code'] == 200 and 'tool_calls' in result['result']

        gpt = ChatGPTOVisionModel(**conf)
        gpt.set_message(message)
        result = gpt.get_result(mock_response)
        assert result['status_code'] == 200 and 'tool_calls' in result['result']


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

    def test_tools(self):
        message = {"query": "The product was okay, but the customer service was terrible. I probably won't buy from them again.", "template": {
            "system": "You are a helpful assistant.",
            "user": "Answer me gently the query: $query"
        }
        }
        mock_response = {'ResponseMetadata': {'RequestId': '07598a2a-f809-43fa-b04a-3fb5134566c2', 'HTTPStatusCode': 200}, 'contentType': 'application/json', 'body': io.StringIO(json.dumps({'id': 'msg_bdrk_01FwnSnRp4ddqxycpffLkK1n', 'type': 'message', 'role': 'assistant', 'model': 'claude-3-5-sonnet-20240620', 'content': [{'type': 'text', 'text': "To analyze the sentiment of this statement, we can use the print_sentiment_scores function. This will help us understand the overall sentiment of your experience with the product and customer service. Let's break it down and use the tool to get a more precise analysis."}, {'type': 'tool_use', 'id': 'toolu_bdrk_013Kn9Tnkh7WBDufhV7tPNhL', 'name': 'print_sentiment_scores', 'input': {'positive_score': 0.1, 'negative_score': 0.6, 'neutral_score': 0.3}}], 'stop_reason': 'tool_use', 'stop_sequence': None, 'usage': {'input_tokens': 491, 'output_tokens': 155}}))}
        conf = copy.deepcopy(claude_model)
        conf.pop('model_pool')
        conf.pop('message')
        conf['temperature'] = 0
        conf['max_tokens'] = 100
        conf['tools'] = [
            {
                "name": "print_sentiment_scores",
                "description": "Prints the sentiment scores of a given text.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "positive_score": {"type": "number", "description": "The positive sentiment score, ranging from 0.0 to 1.0."},
                        "negative_score": {"type": "number", "description": "The negative sentiment score, ranging from 0.0 to 1.0."},
                        "neutral_score": {"type": "number", "description": "The neutral sentiment score, ranging from 0.0 to 1.0."}
                    },
                    "required": ["positive_score", "negative_score", "neutral_score"]
                }
            }
        ]
        claude = ChatClaudeModel(**conf)
        claude.set_message(message)
        result = claude.get_result(mock_response)
        assert result['status_code'] == 200 and 'tool_calls' in result['result']


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

    def test_repr(self):
        conf = copy.deepcopy(nova_model)
        conf.pop('model_pool')
        conf.pop('message')
        conf['temperature'] = 1
        conf['max_tokens'] = 100
        conf['bag_tokens'] = []
        conf['top_p'] = 0.9
        conf['top_k'] = 50
        conf['stop'] = None
        conf['pool_name'] = "techhubdev-pool-world-nova-micro-1:0"
        nova = NovaModel(**conf)
        expected_output = (f"{{model:{conf['model']}, max_tokens:{conf['max_tokens']}, "
                           f"temperature:{conf['temperature']}}}")
        assert repr(nova) == expected_output

    def test_tools(self):
        message = {"query": "The product was okay, but the customer service was terrible. I probably won't buy from them again.", "template": {
            "system": "You are a helpful assistant.",
            "user": "Answer me gently the query: $query"
        }
        }
        mock_response = {'ResponseMetadata': {'RequestId': 'f62372f4-3c07-4dd7-a300-f5d68e787c40', 'HTTPStatusCode': 200}, 'contentType': 'application/json', 'body': io.StringIO(json.dumps({'output': {'message': {'content': [{'text': '<thinking> The user has provided a statement about their experience with a product and customer service. To understand the sentiment behind this statement, I can use the provided tool to calculate sentiment scores. The statement contains both positive and negative sentiments, so I will use the `print_sentiment_scores` tool to get the scores.</thinking>\n'}, {'toolUse': {'name': 'print_sentiment_scores', 'toolUseId': '1c3d7d35-9319-44d8-beb5-1e7a68ef5799', 'input': {'neutral_score': 0.2, 'positive_score': 0.5, 'negative_score': 0.3}}}], 'role': 'assistant'}}, 'stopReason': 'tool_use', 'usage': {'inputTokens': 519, 'outputTokens': 178, 'totalTokens': 697, 'cacheReadInputTokenCount': 0, 'cacheWriteInputTokenCount': 0}}))}
        conf = copy.deepcopy(nova_model)
        conf.pop('model_pool')
        conf.pop('message')
        conf['temperature'] = 1
        conf['max_tokens'] = 100
        conf['bag_tokens'] = 50
        conf['top_p'] = 0.9
        conf['top_k'] = 50
        conf['stop'] = None
        conf['pool_name'] = "techhubdev-pool-world-nova-micro-1:0"
        conf['tools'] = [
            {
                "name": "print_sentiment_scores",
                "description": "Prints the sentiment scores of a given text.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "positive_score": {"type": "number", "description": "The positive sentiment score, ranging from 0.0 to 1.0."},
                        "negative_score": {"type": "number", "description": "The negative sentiment score, ranging from 0.0 to 1.0."},
                        "neutral_score": {"type": "number", "description": "The neutral sentiment score, ranging from 0.0 to 1.0."}
                    },
                    "required": ["positive_score", "negative_score", "neutral_score"]
                }
            }
        ]
        nova = ChatNova(**conf)
        nova.set_message(message)
        result = nova.get_result(mock_response)
        assert result['status_code'] == 200 and 'tool_calls' in result['result']

        message = {
            "query": "hello how ae you?",
            "template": {
                "system": "You are a helpful assistant.",
                "user": "Answer me gently the query: $query"
            }
        }
        mock_response = {'ResponseMetadata': {'RequestId': 'f62372f4-3c07-4dd7-a300-f5d68e787c40', 'HTTPStatusCode': 200}, 'contentType': 'application/json', 'body': io.StringIO(json.dumps({'output': {'message': {'content': [{'text': "<thinking> The User has asked a general question about how I am, but this toolset does not include a way to respond to such a question. Therefore, I should inform the User that I cannot provide a personal response to this type of question.</thinking>\nI'm here to help with any information or tasks you need. How can I assist you today?"}], 'role': 'assistant'}}, 'stopReason': 'end_turn', 'usage': {'inputTokens': 503, 'outputTokens': 74, 'totalTokens': 577, 'cacheReadInputTokenCount': 0, 'cacheWriteInputTokenCount': 0}}))}
        nova = ChatNova(**conf)
        nova.set_message(message)
        result = nova.get_result(mock_response)
        assert result['status_code'] == 200 and 'tool_calls' not in result['result']

class TestGPTOModel:

    def test_parse_data(self):
        conf = {
            "model": o_model['model'],
            "seed": 42,
            "response_format": "json_object",
            "max_completion_tokens": 100,
            "reasoning_effort": "low",
            "tools": [
                {
                    "name": "print_sentiment_scores",
                    "description": "Prints the sentiment scores of a given text.",
                    "input_schema": {
                        "type": "object",
                        "properties": {
                            "positive_score": {"type": "number",
                                               "description": "The positive sentiment score, ranging from 0.0 to 1.0."},
                            "negative_score": {"type": "number",
                                               "description": "The negative sentiment score, ranging from 0.0 to 1.0."},
                            "neutral_score": {"type": "number",
                                              "description": "The neutral sentiment score, ranging from 0.0 to 1.0."}
                        },
                        "required": ["positive_score", "negative_score", "neutral_score"]
                    }
                }
            ]
        }
        manager_models_config = MagicMock()
        manager_models_config.get_model.return_value = copy.deepcopy(o_model)
        gpt_o = ManagerModel.get_model(conf, "azure", available_pools, manager_models_config)
        gpt_o.set_message(message_dict)
        gpt_o.parse_data()

        conf['response_format'] = "dd"
        conf['model'] = o_model['model']
        manager_models_config.get_model.return_value = copy.deepcopy(o_model)
        with pytest.raises(PrintableGenaiError, match=re.escape(f"Error 400: Response format {conf['response_format']} not "
                                                                f"supported for model {conf['model']} "
                                                                f"(only 'json_object' supported)")):

            gpt_o = ManagerModel.get_model(conf, "azure", available_pools, manager_models_config)
            gpt_o.set_message(message_dict)
            gpt_o.parse_data()
    def test_parse_data_o1_mini(self):
        conf = {
            "model": o1_mini_model['model'],
            "seed": 42,
            "response_format": "json_object",
            "max_completion_tokens": 100
        }
        manager_models_config = MagicMock()
        manager_models_config.get_model.return_value = copy.deepcopy(o_model)
        gpt_o1_mini = ManagerModel.get_model(conf, "azure", available_pools, manager_models_config)
        gpt_o1_mini.set_message(message_dict)
        gpt_o1_mini.parse_data()

        conf['response_format'] = "dd"
        conf['model'] = o1_mini_model['model']
        manager_models_config.get_model.return_value = copy.deepcopy(o_model)
        with pytest.raises(PrintableGenaiError, match=re.escape(f"Error 400: Response format {conf['response_format']} not supported for model {gpt_o1_mini.model_name} "
                                                                f"(only 'json_object' supported)")):

            gpt_o1_mini = ManagerModel.get_model(conf, "azure", available_pools, manager_models_config)
            gpt_o1_mini.set_message(message_dict)
            gpt_o1_mini.parse_data()


    def test_get_result(self):
        mock_response = {
            'status_code': 200,
            'choices': [{"finish_reason": "content_filter"}],
            'usage': {
                'prompt_tokens': 0,
                'completion_tokens': 0,
                'total_tokens': 0,
                'completion_tokens_details': {'reasoning_tokens': 0}
            }
        }
        conf = copy.deepcopy(o_model)
        conf.pop('model_pool')
        conf['max_completion_tokens'] = 100
        conf['n'] = 1
        gpt_o = ChatGPTOModel(**conf)
        gpt_o.set_message(message_dict)
        result = gpt_o.get_result(mock_response)
        assert result['status_code'] == 400 and result['error_message'] == "content_filter"


        mock_response['choices'] = [{"finish_reason": "ss"}]
        with pytest.raises(PrintableGenaiError, match=re.escape("Error 400: Azure format is not as expected: "
                                                                "{'finish_reason': 'ss'}.")):
            gpt_o.get_result(mock_response)

        mock_response['choices'] = [{"message": {"content": "Not found"}}]
        result = gpt_o.get_result(mock_response)
        assert result['result']['answer'] == "Not found"


class TestGPTOVisionModel:

    def test_parse_data(self):
        conf = {
            "model": o_v_model['model'],
            "seed": 42,
            "response_format": "json_object",
            "max_completion_tokens": 100,
            "reasoning_effort": "low"
        }
        manager_models_config = MagicMock()
        manager_models_config.get_model.return_value = copy.deepcopy(o_v_model)
        gpt_o_v = ManagerModel.get_model(conf, "azure", available_pools, manager_models_config)
        gpt_o_v.set_message(message_dict)
        gpt_o_v.parse_data()

        conf['response_format'] = "dd"
        conf['model'] = o_v_model['model']
        manager_models_config.get_model.return_value = copy.deepcopy(o_v_model)
        with pytest.raises(PrintableGenaiError, match=re.escape(f"Error 400: Response format {conf['response_format']} not "
                                                                f"supported for model {conf['model']} "
                                                                f"(only 'json_object' supported)")):

            gpt_o_v = ManagerModel.get_model(conf, "azure", available_pools, manager_models_config)
            gpt_o_v.set_message(message_dict)
            gpt_o_v.parse_data()


    def test_get_result(self):
        mock_response = {
            'status_code': 200,
            'choices': [{"finish_reason": "content_filter"}],
            'usage': {
                'prompt_tokens': 0,
                'completion_tokens': 0,
                'total_tokens': 0,
                'completion_tokens_details': {'reasoning_tokens': 0}
            }
        }
        conf = copy.deepcopy(o_v_model)
        conf.pop('model_pool')
        conf['max_completion_tokens'] = 100
        conf['n'] = 1
        gpt_o_v = ChatGPTOVisionModel(**conf)
        gpt_o_v.set_message(message_dict)
        result = gpt_o_v.get_result(mock_response)
        assert result['status_code'] == 400 and result['error_message'] == "content_filter"


        mock_response['choices'] = [{"finish_reason": "ss"}]
        with pytest.raises(PrintableGenaiError, match=re.escape("Error 400: Azure format is not as expected: "
                                                                "{'finish_reason': 'ss'}.")):
            gpt_o_v.get_result(mock_response)

        mock_response['choices'] = [{"message": {"content": "Not found"}}]
        result = gpt_o_v.get_result(mock_response)
        assert result['result']['answer'] == "Not found"

class TestGeminiModel:
    def test_parse_data(self):
        conf = {
            "model": gemini_model['model'],
            "top_k": 100,
            "tools": [
            {
                "name": "print_sentiment_scores",
                "description": "Prints the sentiment scores of a given text.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "positive_score": {
                            "type": "number",
                            "description": "The positive sentiment score, ranging from 0.0 to 1.0."
                        },
                        "negative_score": {
                            "type": "number",
                            "description": "The negative sentiment score, ranging from 0.0 to 1.0."
                        },
                        "neutral_score": {
                            "type": "number",
                            "description": "The neutral sentiment score, ranging from 0.0 to 1.0."
                        }
                    },
                    "required": [
                        "positive_score",
                        "negative_score",
                        "neutral_score"
                    ]
                }
            }
        ]
        }
        manager_models_config = MagicMock()
        manager_models_config.get_model.return_value = copy.deepcopy(gemini_model)
        gemini_v = ManagerModel.get_model(conf, "azure", available_pools, manager_models_config)
        gemini_v.set_message(message_dict)
        gemini_v.parse_data()

        conf['response_format'] = "dd"
        conf['model'] = gemini_model['model']
        conf["response_format"] = "json_object"
        manager_models_config.get_model.return_value = copy.deepcopy(gemini_model)
        with pytest.raises(PrintableGenaiError, match=re.escape("Error 400: Parameter: 'response_format' not supported in model: 'techhubdev-gemini-2.0-flash-exp'")):

            gemini_v = ManagerModel.get_model(conf, "vertex", available_pools, manager_models_config)
            gemini_v.set_message(message_dict)
            gemini_v.parse_data()

    def test_get_result(self):
        manager_models_config = MagicMock()
        manager_models_config.get_model.return_value = copy.deepcopy(gemini_model)
        geminiv = ManagerModel.get_model({"model": gemini_model['model']}, "vertex", available_pools, manager_models_config)

        mock_message = MagicMock()
        mock_message.user_query_tokens = 50
        geminiv.message = mock_message

        response_1 = {
            'ResponseMetadata': {'HTTPStatusCode': 400},
            'body': MagicMock()
        }
        response_1['body'].read.return_value = json.dumps({'error': 'Bad Request'})
        result_1 = geminiv.get_result(response_1)
        assert result_1['status'] == 'error'
        assert result_1['status_code'] == 400
        assert result_1['error_message'] == {'error': 'Bad Request'}

        response_2 = {
            'status_code': 500,
            'msg': 'Internal Server Error'
        }
        result_2 = geminiv.get_result(response_2)
        assert result_2['status'] == 'error'
        assert result_2['status_code'] == 500
        assert result_2['error_message'] == 'Internal Server Error'

        response_3 = {
            'candidates': [{
                'content': {'parts': [{'text': 'Hello, world!'}]}
            }],
            'usageMetadata': {
                'promptTokenCount': 10,
                'totalTokenCount': 20,
                'candidatesTokenCount': 5
            }
        }
        result_3 = geminiv.get_result(response_3)
        assert result_3['status'] == 'finished'
        assert result_3['result']['answer'] == 'Hello, world!'
        assert result_3['result']['input_tokens'] == 10
        assert result_3['result']['n_tokens'] == 20
        assert result_3['result']['output_tokens'] == 5
        assert result_3['result']['query_tokens'] == 50
        assert result_3['status_code'] == 200

        response_4 = {
            'candidates': [{
                'content': {'parts': [{'functionCall': {'name': 'getWeather', 'args': {'location': 'NYC'}}}]}
            }],
            'usageMetadata': {}
        }
        result_4 = geminiv.get_result(response_4)
        assert result_4['status'] == 'finished'
        assert result_4['result']['tool_calls'] == [{
            'id': '',
            'name': 'getWeather',
            'inputs': {'location': 'NYC'}
        }]
        assert result_4['status_code'] == 200
