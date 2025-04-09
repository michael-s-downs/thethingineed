### This code is property of the GGAO ###

import os
os.environ['URL_LLM'] = "test_url"
os.environ['URL_RETRIEVE'] = "test_retrieve"
import pytest
import json
from unittest.mock import patch, MagicMock, AsyncMock
from director import Director  # Assuming your code is in director.py
from compose.streambatch import StreamBatch
from pcutils.persist import PersistDict
from common.errors.genaierrors import PrintableGenaiError

@pytest.fixture
def mock_director():
    director = Director(compose_conf={'compose_flow': 'mock_conf'}, apigw_params={'mock': 'params'})
    
    # Mock attributes and methods that are used within the get_output method
    director.logger = MagicMock()  # Mock the logger
    
    # Mock conf_manager object with required attributes
    director.conf_manager = MagicMock()
    director.conf_manager.session_id = "mock_session"
    director.conf_manager.lang = "mock_lang"
    director.conf_manager.langfuse_m.update_output = MagicMock()
    director.conf_manager.langfuse_m.flush = MagicMock()
    director.conf_manager.headers = {'x-tenant': 'mock_tenant'}
    director.conf_manager.persist_m = MagicMock()  # Simulate persistence
    director.conf_manager.template_m = MagicMock()
    director.conf_manager.template_m.params = {'mock_param': 'value'}
    director.conf_manager.clear_quotes = "mock_clear_quotes"
    
    # Mock StreamBatch object
    director.sb = MagicMock()
    director.sb.to_list_serializable.return_value = ['mock_streambatch']

    # Mock ActionsManager
    director.actions_manager = MagicMock()
    director.actions_manager.parse_input = MagicMock()
    director.actions_manager.get_and_drop_query_actions = MagicMock()
    director.actions_manager.actions_confs = []

    # Mock PersistDict object
    director.PD = MagicMock()
    director.PD.get_conversation.return_value = "mock_conversation"
    director.PD.PD = {'mock_key': 'mock_value'}
    director.PD.get_from_redis = MagicMock()
    director.PD.save_to_redis = MagicMock()

    # Mock run_query_actions and run_actions
    director.run_query_actions = MagicMock()
    director.run_actions = MagicMock()

    # Mock output_manager methods
    director.output_manager = MagicMock()
    director.output_manager.get_answer = MagicMock()
    director.output_manager.get_scores = MagicMock()
    director.output_manager.get_lang = MagicMock()
    director.output_manager.get_n_conversation = MagicMock()
    director.output_manager.get_n_retrieval = MagicMock()


    return director

def test_init_director():
    director = Director(compose_conf={'compose_flow': 'mock_conf'}, apigw_params={'mock': 'params'})
    assert isinstance(director.sb, StreamBatch)
    assert director.apigw_params == {'mock': 'params'}
    assert director.compose_conf == {'compose_flow': 'mock_conf'}
    assert isinstance(director.PD, PersistDict)



@patch('os.getenv')
@patch('glob.glob')
@patch('builtins.open', new_callable=MagicMock)
@patch.dict(os.environ, {}, clear=True)  # Clear environment for the test
def test_load_secrets_success(mock_open, mock_glob, mock_getenv, mock_director):
    # Mock the SECRETS_PATH environment variable and files found by glob
    mock_getenv.return_value = "/mock_secrets"
    mock_glob.return_value = ['/mock_secrets/secret1.json']

    # Mock the content of the secret file
    secret_content = json.dumps({'SECRET_KEY': 'mock_value'})
    mock_open.return_value.read.return_value = secret_content
    
    # Call the load_secrets method
    mock_director.load_secrets()
        
    # Ensure getenv is called and the correct path is used
    mock_getenv.assert_called_with('SECRETS_PATH', "/secrets")
        
    # Ensure that the file was found and read correctly
    mock_glob.assert_called_with("/mock_secrets/**/*.json", recursive=True)
        
    # Check if environment variable was set from the secret
    assert os.environ['SECRET_KEY'] == 'mock_value'
        
    # Ensure the logger debug message was called
    mock_director.logger.debug.assert_called_with("Loading secret '/mock_secrets/secret1.json'")

@patch('os.getenv')
@patch('glob.glob')
def test_load_secrets_no_secrets_found(mock_glob, mock_getenv, mock_director):
    # Test when no secrets are found by glob

    mock_getenv.return_value = "/mock_secrets"
    mock_glob.return_value = []  # No secrets found

    mock_director.load_secrets()

    # Check that glob.glob was called, but no debug or warning is logged
    mock_glob.assert_called_with("/mock_secrets/**/*.json", recursive=True)
    mock_director.logger.debug.assert_not_called()
    mock_director.logger.warning.assert_not_called()

@patch('os.getenv')
@patch('glob.glob')
def test_load_secrets_invalid_json(mock_glob, mock_getenv, mock_director):
    # Test when a JSON file can't be parsed
    mock_getenv.return_value = "/mock_secrets"
    mock_glob.return_value = ['/mock_secrets/secret1.json']

    # Raise an exception when trying to load invalid JSON
    with patch('builtins.open', side_effect=Exception('Invalid JSON')):
        mock_director.load_secrets()

    # Ensure the logger warning message was called
    mock_director.logger.warning.assert_called_with("Unable to load secret '/mock_secrets/secret1.json'")

@patch('os.getenv')
@patch('glob.glob')
def test_load_secrets_partial_failure(mock_glob, mock_getenv, mock_director):
    # Test when one secret loads successfully but another fails
    mock_getenv.return_value = "/mock_secrets"
    mock_glob.return_value = ['/mock_secrets/secret1.json', '/mock_secrets/secret2.json']

    # First file loads successfully, second fails
    with patch('builtins.open', side_effect=[MagicMock(read=lambda: json.dumps({'SECRET_KEY': 'mock_value'})), Exception('Error')]):
        mock_director.load_secrets()

    # Check that the environment variable was set from the first file
    assert os.environ['SECRET_KEY'] == 'mock_value'

    # Ensure the logger recorded both the successful load and the failed one
    mock_director.logger.debug.assert_any_call("Loading secret '/mock_secrets/secret1.json'")
    mock_director.logger.warning.assert_called_with("Unable to load secret '/mock_secrets/secret2.json'")


def test_run_conf_manager_actions(mock_director):
    # This method calls multiple internal methods
    mock_director.conf_manager.template_m.template = '[{\r\n        "action": "retrieve",\r\n        "action_params": {\r\n            "params": {\r\n                "generic": {\r\n                    "indexation_conf": {\r\n                        "add_highlights": false,\r\n                        "index": "$index",\r\n                        "query": "$query",\r\n                        "task": "retrieve",\r\n\t\t\t\t\t\t"top_k": 5,\r\n\t\t\t\t\t\t"filters": $filters\r\n                    },\r\n                    "process_type": "ir_retrieve"\r\n                }\r\n            },\r\n            "type": "get_chunks"\r\n        }\r\n    }]'
    mock_director.conf_manager.template_m.query = 'mock_query'
    mock_director.conf_manager.headers = {'mock': 'headers'}
    mock_director.conf_manager.filter_m = MagicMock()
    mock_director.conf_manager.filter_m.run = MagicMock()
    mock_director.conf_manager.filter_m.run.return_value = ('filtered_answer', True)
    mock_director.get_compose_flow = MagicMock()
    mock_director.get_compose_flow.return_value = [{'action': 'llm_action', 'action_params': { 'type': 'llm_content', 'params':{'query_metadata':{'query': 'hola'}} }}, {'action': 'rescore', 'action_params': { 'type': 'genai_rescorer', 'params':{'headers_config': ''} }}]
    
    mock_director.run_conf_manager_actions()

def test_run_conf_manager_actions_exceptino(mock_director):
    # This method calls multiple internal methods
    mock_director.compose_conf = {}

    
    with pytest.raises(PrintableGenaiError) as exc_info:
        mock_director.run_conf_manager_actions()

    assert "Compose config must have" in str(exc_info.value)


def test_run_query_actions(mock_director):
    mock_director.actions_manager.query_actions_confs = [{'action': 'expansion', 'action_params': {}}]
    with patch('director.expansion', return_value='mock_expansion_output'):
        mock_director.run_query_actions()
        assert mock_director.actions_manager.query_actions_confs[0]['action'] == 'expansion'

def test_add_start_to_trace(mock_director):
    mock_director.conf_manager.langfuse_m = MagicMock()
    mock_director.add_start_to_trace('llm_action', {'headers_config': 'mock_headers'})
    assert mock_director.conf_manager.langfuse_m.add_generation.called

def test_add_end_to_trace(mock_director):
    mock_director.conf_manager.langfuse_m = MagicMock()
    mock_director.add_end_to_trace('llm_action', 'langfuse_sg')
    assert mock_director.conf_manager.langfuse_m.add_generation_output.called

def test_get_compose_flow_call_load(mock_director):
    mock_director.conf_manager.template_m.load_template = MagicMock()
    mock_director.conf_manager.template_m.template =  '{"action": "llm_action", "params": {"lang": "en"}}'
    mock_director.get_compose_flow()

def test_get_compose_flow(mock_director):
    mock_director.conf_manager.template_m.template =   '{"action": "summarize", "params": {"lang": "en"}}'
    mock_director.get_compose_flow()
    assert mock_director.conf_manager.langfuse_m.update_input.called

def test_get_output(mock_director):
    output = mock_director.get_output()

    expected_output = {
        'session_id': 'mock_session',
        'streambatch': ['mock_streambatch'],
    }

    assert output == expected_output

    # Ensure that the correct methods are being called with correct parameters
    mock_director.output_manager.get_answer.assert_called_once_with(expected_output, mock_director.sb)
    mock_director.conf_manager.langfuse_m.update_output.assert_called_once_with(expected_output.get('answer', ''))
    mock_director.output_manager.get_scores.assert_called_once_with(expected_output, mock_director.sb)
    mock_director.output_manager.get_lang.assert_called_once_with(expected_output, 'mock_lang')
    mock_director.output_manager.get_n_conversation.assert_called_once_with(expected_output, "mock_conversation")
    mock_director.output_manager.get_n_retrieval.assert_called_once_with(expected_output, mock_director.sb)

    # Ensure that the flush method is called once
    mock_director.conf_manager.langfuse_m.flush.assert_called_once()

def test_run_2(mock_director):
    # Mock the ConfManager constructor
    with patch('director.ConfManager', return_value=mock_director.conf_manager):
        # Mock the ActionsManager constructor
        with patch('director.ActionsManager', return_value=mock_director.actions_manager):
            # Mock the OutputManager constructor
            with patch('director.OutputManager', return_value=mock_director.output_manager):
                
                # Mock run_conf_manager_actions method
                mock_director.run_conf_manager_actions = MagicMock(return_value={'mock_conf': 'value'})
                
                # Call the run method
                output = mock_director.run()

                # Assertions
                assert output == {'session_id': 'mock_session', 'streambatch': ['mock_streambatch']}


@pytest.fixture
def mock_director_2():
    director = Director(compose_conf={'compose_flow': 'mock_conf'}, apigw_params={'mock': 'params'})
    
    # Mock the logger
    director.logger = MagicMock()
    
    # Mock conf_manager
    director.conf_manager = MagicMock()
    director.conf_manager.headers = {'x-tenant': 'mock_tenant'}
    
    # Mock actions_manager with an empty list of query_actions_confs initially
    director.actions_manager = MagicMock()
    director.actions_manager.query_actions_confs = []
    director.actions_manager.action_confs = [{
        "action": "retrieve",
        "action_params": {
            "params": {
                "generic": {
                    "indexation_conf": {
                        "index": "index_test",
                        "query": "hello how are you",
                        "task": "retrieve",
						"top_k": 5,
                    },
                    "process_type": "ir_retrieve"
                }
            },
            "type": "get_chunks"
        }
    }]

    return director


def test_run_query_actions_no_actions(mock_director_2):
    """Test when there are no query actions configured."""
    mock_director_2.run_query_actions()
    # Check that logger was not called
    mock_director_2.logger.info.assert_not_called()

@pytest.mark.asyncio
async def test_run_query_actions_valid_action(mock_director_2):
    """Test when a valid query action is configured."""
    # Setup a valid action configuration including both 'retrieve' and 'expansion'
    mock_director_2.actions_manager.query_actions_confs = [
        {
            'action': 'expansion',
            'action_params': {
                'type': 'lang',
                'params': {
                    'key': 'value',
                    'langs': ["es", "en"]
                }
            }
        },
    ]

    # Mocking the responses of async calls made by LangExpansion
    mock_expansion_instance = MagicMock()
    mock_expansion_instance.process = AsyncMock(return_value=[
        "mock_translated_query_1",
        "mock_translated_query_2"
    ])

    # Replace the LangExpansion instance in the context where it's used
    with patch('director.expansion', return_value=mock_expansion_instance):
        # Setup necessary context for langfuse_sg and output
        langfuse_sg = "some_langfuse_sg"
        output = "some_output"
        mock_director_2.actions_manager.actions_confs = [{'action': 'retrieve', 'action_params': {"type":"", "params":{}}}]
        mock_director_2.run_query_actions()


def test_run_query_actions_invalid_action(mock_director_2):
    """Test when an invalid query action is configured."""
    # Setup an invalid action configuration
    mock_director_2.actions_manager.query_actions_confs = [{
        'action': 'invalid_action',
        'action_params': {}
    }]

    with pytest.raises(PrintableGenaiError, match="Query action not found"):
        mock_director_2.run_query_actions()



@pytest.fixture
def director():
    """Fixture to create a Director instance with mocked dependencies."""
    compose_conf = {}
    apigw_params = {}
    director_instance = Director(compose_conf, apigw_params)

    # Mock dependencies
    director_instance.logger = MagicMock()
    director_instance.sb = MagicMock()
    director_instance.PD = MagicMock()
    director_instance.conf_manager = MagicMock()
    director_instance.actions_manager = MagicMock()
    
    return director_instance

def test_run_actions_success(director):
    """Test run_actions executes actions successfully."""
    # Mock actions configuration
    director.actions_manager.actions_confs = [
        {'action': 'retrieve', 'action_params': {'params': {}, "type": "get_chunks"}},
        {'action': 'filter', 'action_params': {'params': {}, "type": "top_k"}},
        {'action': 'llm_action', 'action_params': {'params': {}, "type": "llm_action"}}
    ]
    
    director.sb.retrieve = MagicMock()
    director.sb.filter = MagicMock()
    director.sb.llm_action = MagicMock()
    
    director.run_actions()

    director.sb.retrieve.assert_called_once()
    director.sb.filter.assert_called_once()
    director.sb.llm_action.assert_called_once()

def test_run_actions_action_not_found(director):
    """Test run_actions raises error for unknown actions."""
    director.actions_manager.actions_confs = [
        {'action': 'unknown_action', 'action_params': {'params': {}}}
    ]
    
    with pytest.raises(Exception) as excinfo:
        director.run_actions()
    
    assert "Action not found, choose one between" in str(excinfo.value)

def test_run_actions_llm_action(director):
    """Test that llm_action runs with the correct parameters."""
    director.actions_manager.actions_confs = [
        {'action': 'llm_action', 'action_params': {'params': {}, "type": "llm_content"}},
    ]

    # Mock llm_action function
    director.sb.llm_action = MagicMock()
    director.conf_manager.template_m.top_qa = "some_top_qa"
    director.conf_manager.template_m.query_type = "some_query_type"
    director.conf_manager.template_m.llm_action = "some_llm_action"

    director.run_actions()

    # Assert that the required parameters were set
    args_passed_to_llm = director.sb.llm_action.call_args[0][1]  # Getting the 'params' from call args
    assert 'PD' in args_passed_to_llm
    assert args_passed_to_llm['top_qa'] == "some_top_qa"
    assert args_passed_to_llm['query_type'] == "some_query_type"
    assert args_passed_to_llm['llm_action'] == "some_llm_action"


# Test cases for the fix_merge function
@pytest.mark.parametrize("input_str, expected_output", [
    ("{\"template\": \"value\"}\n{\"merge\": \"$var\"}", 
     "{\"template\": \"value\"}\n{\"merge\": $var}\n"),
    
    ("{\"template\": \"$var\"}\n{\"data\": \"value\"}",
     "{\"template\": \"$var\"}\n{\"data\": \"value\"}\n"),
    
    ("{\"template\": \"value\", \"merge\": \"no_var\"}\n{\"template\": \"test\"}\n{\"merge\": \"$var\"}",
     "{\"template\": \"value\", \"merge\": \"no_var\"}\n{\"template\": \"test\"}\n{\"merge\": $var}\n"),
])
def test_fix_merge(input_str, expected_output, mock_director):
    assert mock_director.fix_merge(input_str) == expected_output

