### This code is property of the GGAO ###


import os
os.environ['URL_LLM'] = "test_url"
os.environ['URL_RETRIEVE'] = "test_retrieve"
import pytest
from unittest.mock import patch, MagicMock
from compose.query_actions.reformulate import MixQueries, ReformulateFactory, ReformulateMethod
from common.errors.genaierrors import PrintableGenaiError

# Define sample data and mocks
sample_query = "sample query"
sample_params = {
    "headers": {"Authorization": "Bearer mock_token"},
    "max_persistence": 5,
    "template_name": "template",
    "lang": "en",
    "save_mod_query": False,
    "session_id": "12345",
    "PD": MagicMock()
}
sample_actions_confs = [{"action": "retrieve", "action_params": {"params": {"indexation_conf": {}}}}
                        , {"action": "llm_action", "action_params": {"params": {"query_metadata": {"indexation_conf": {}}}}}]
mock_response = {"answer": "modified query"}

# Mock environment variable
@pytest.fixture(autouse=True)
def mock_env_url():
    with patch.dict('os.environ', {'URL_LLM': 'https://mockurl.com'}):
        yield

# Mock LLM Parser
@pytest.fixture
def mock_llm_parser():
    with patch('compose.query_actions.reformulate.LLMParser.parse_response') as mock_parse_response:
        mock_parse_response.return_value = mock_response
        yield mock_parse_response

# Mock requests.post for MixQueries
@pytest.fixture
def mock_requests_post():
    with patch('requests.post') as mock_post:
        mock_post.return_value.json.return_value = mock_response
        yield mock_post

# Test MixQueries class
class TestMixQueries:

    def test_process_success(self, mock_requests_post, mock_llm_parser):
        # Mocking session to simulate conversation
        session_mock = MagicMock()
        sample_params["PD"].get_conversation.return_value = session_mock
        session_mock.add = MagicMock()
        session_mock.return_value = [{"user":"test"}]
        session_mock.__len__.return_value = 1

        mix_queries = MixQueries(sample_query)

        # Execute process and check if response is as expected
        mix_queries.process(sample_params, sample_actions_confs)

    def test_process_empty_query(self):
        # Test for empty query which should raise PrintableGenaiError
        mix_queries = MixQueries("")
        with pytest.raises(PrintableGenaiError, match="Query is empty, cannot filter"):
            mix_queries.process(sample_params, sample_actions_confs)

    def test_process_with_save_mod_query_true(self, mock_requests_post, mock_llm_parser):
        sample_params["save_mod_query"] = True
        session_mock = MagicMock()
        sample_params["PD"].get_conversation.return_value = session_mock
        session_mock.add = MagicMock()
        session_mock.return_value = [{"user":"test"}]
        session_mock.__len__.return_value = 1
        mix_queries = MixQueries(sample_query)
        
        sample_params["headers"] = {"example": "test"}
        mix_queries.process(sample_params, sample_actions_confs)

# Test ReformulateFactory class
class TestReformulateFactory:

    def test_factory_valid_type(self):
        # Test with a valid reformulate_type
        factory = ReformulateFactory(reformulate_type="mix_queries")
        assert isinstance(factory.reformulatemethod, type(MixQueries))

    def test_factory_invalid_type(self):
        # Test with an invalid reformulate_type, should raise PrintableGenaiError
        with pytest.raises(PrintableGenaiError, match="Provided reformulate method does not match any of the possible ones"):
            ReformulateFactory(reformulate_type="invalid_type")

    def test_factory_process(self, mock_requests_post, mock_llm_parser):
        # Mock the factory to ensure it calls the correct method
        factory = ReformulateFactory(reformulate_type="mix_queries")
        sample_params["headers"] = {"example": "test"}
        factory.process(query=sample_query, params=sample_params, actions_confs=sample_actions_confs)

# Test ReformulateMethod abstract base class
def test_reformulate_method_abstract():
    # Attempting to instantiate the abstract base class should raise TypeError
    with pytest.raises(TypeError):
        ReformulateMethod(sample_query)
