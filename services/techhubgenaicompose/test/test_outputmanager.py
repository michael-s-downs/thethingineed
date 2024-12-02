### This code is property of the GGAO ###

import os
os.environ['URL_LLM'] = "test_url"
os.environ['URL_RETRIEVE'] = "test_retrieve"
import pytest
from unittest.mock import MagicMock, patch
from outputmanager import OutputManager

@pytest.fixture
def compose_conf():
    """Fixture to create a default compose configuration."""
    return {
        'output': {
            'scores': True,
            'lang': True,
            'n_conversation': 3,
            'n_retrieval': 2
        }
    }

@pytest.fixture
def output_manager(compose_conf):
    """Fixture to create an instance of OutputManager."""
    return OutputManager(compose_conf)

def test_init(output_manager):
    """Test the initialization of the OutputManager."""
    assert output_manager.scores is True
    assert output_manager.lang is True
    assert output_manager.n_conversation == 3
    assert output_manager.n_retrieval == 2

def test_parse_with_missing_output_section():
    """Test the parse method when 'output' section is missing in compose_conf."""
    manager = OutputManager({})
    manager.logger = MagicMock()  # Mock the logger
    manager.parse({})
    manager.logger.debug.assert_called_once_with("Output not found in query")

def test_get_param(output_manager):
    """Test the get_param method."""
    params = {'scores': True, 'lang': False, 'n_conversation': 5, 'n_retrieval': 3}
    result = output_manager.get_param(params, 'scores', bool)
    assert result is True
    result = output_manager.get_param(params, 'lang', bool)
    assert result is False
    result = output_manager.get_param(params, 'n_conversation', int)
    assert result == 5

def test_get_scores_with_scores_enabled(output_manager):
    """Test get_scores when scores are enabled."""
    output = {}
    sb = [[MagicMock(scores=0.95)]]  # Mock streambatch
    output_manager.get_scores(output, sb)
    assert output['scores'] == 0.95

def test_get_scores_with_scores_disabled(output_manager):
    """Test get_scores when scores are disabled."""
    output = {}
    sb = [[MagicMock(scores=0.95)]]  # Mock streambatch
    output_manager.scores = False  # Disable scores
    output_manager.get_scores(output, sb)
    assert 'scores' not in output

def test_get_lang_with_lang_enabled(output_manager):
    """Test get_lang when lang is enabled."""
    output = {}
    output_manager.lang = True
    output_manager.get_lang(output, 'en')
    assert output['lang'] == 'en'

def test_get_lang_with_lang_disabled(output_manager):
    """Test get_lang when lang is disabled."""
    output = {}
    output_manager.lang = False
    output_manager.get_lang(output, 'en')
    assert 'lang' not in output

def test_get_n_conversation_with_data(output_manager):
    """Test get_n_conversation when conversation data is available."""
    output = {}
    conversation = ["message1", "message2", "message3", "message4"]
    output_manager.get_n_conversation(output, conversation)
    assert output['n_conversation']['n'] == 3
    assert output['n_conversation']['conversation'] == conversation[-3:]

def test_get_n_conversation_with_empty_data(output_manager):
    """Test get_n_conversation when conversation data is empty."""
    output = {}
    conversation = []
    output_manager.logger = MagicMock()  # Mock the logger
    output_manager.get_n_conversation(output, conversation)
    assert output['n_conversation']['conversation'] == "Error, conversation empty"
    output_manager.logger.error.assert_called_once_with("Persist not activated, there is no conversation saved")

def test_get_n_retrieval_with_data(output_manager):
    """Test get_n_retrieval when retrieval data is available."""
    output = {}
    sb = MagicMock()
    sb.to_list_serializable.return_value = [["ret1", "ret2", "ret3"]]
    output_manager.get_n_retrieval(output, sb)
    assert output['n_retrieval']['n'] == 2
    assert output['n_retrieval']['retrieve'] == ["ret1", "ret2"]

def test_get_n_retrieval_with_empty_data(output_manager):
    """Test get_n_retrieval when retrieval data is empty."""
    output = {}
    sb = MagicMock()
    sb.to_list_serializable.return_value = [[]]
    output_manager.get_n_retrieval(output, sb)
    assert output['n_retrieval']['retrieve'] == []

def test_get_answer(output_manager):
    """Test get_answer method with valid data."""
    output = {}
    sb = [[MagicMock(answer="This is an answer")]]
    output_manager.get_answer(output, sb)
    assert output['answer'] == "This is an answer"

def test_get_answer_with_no_data(output_manager):
    """Test get_answer method with no answer data."""
    output = {}
    sb = [[]]
    output_manager.get_answer(output, sb)
    assert 'answer' not in output

def test_get_n_conversation_reduces_n_conversation(output_manager):
    """Test get_n_conversation when n_conversation is greater than the length of the conversation."""
    output = {}
    conversation = ["message1", "message2"]  # Only 2 messages in the conversation
    output_manager.n_conversation = 5  # n_conversation is set to 5, greater than len(conversation)
    
    output_manager.get_n_conversation(output, conversation)
    
    # Ensure n_conversation is reduced to the length of the conversation
    assert output_manager.n_conversation == 2
    assert output['n_conversation']['n'] == 5
    assert output['n_conversation']['conversation'] == conversation
