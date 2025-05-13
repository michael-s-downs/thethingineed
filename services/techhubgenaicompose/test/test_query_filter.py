### This code is property of the GGAO ###


import os
os.environ['URL_LLM'] = "test_url"
os.environ['URL_RETRIEVE'] = "test_retrieve"
import pytest
from unittest.mock import patch, MagicMock
from compose.query_actions.filter_q import FilterExactMatch, FilterGPT, FilterFactory, FilterMethod
from common.errors.genaierrors import PrintableGenaiError
import json

# Define sample data and mocks
mock_template_content = json.dumps({
    "substitutions_template": "template",
    "substitutions": [{"from": "modified query", "to": "test query", "extra_words": ["test"]}]
})
sample_query = "modified query sample"
mock_langfuse = MagicMock()
mock_langfuse.load_template.return_value.prompt = mock_template_content
sample_params = {
    "headers": {"Authorization": "Bearer mock_token"},
    "template": "mock_template",
    "langfuse": mock_langfuse
}
sample_actions_confs = [{"action": "retrieve", "action_params": {"params": {"generic": {"indexation_conf": {}}}}}]
mock_response = {"answer": "modified query"}

# Mock environment variable
@pytest.fixture(autouse=True)
def mock_env_url():
    with patch.dict('os.environ', {'URL_LLM': 'https://mockurl.com'}):
        yield

# Mock loading template file from S3
@pytest.fixture
def mock_load_file():
    with patch('compose.query_actions.filter_q.load_file') as mock_load:
        mock_load.return_value = mock_template_content.encode()
        yield mock_load

# Mock requests.post for FilterGPT
@pytest.fixture
def mock_requests_post():
    with patch('requests.post') as mock_post:
        mock_post.return_value.json.return_value = mock_response
        mock_post.return_value.status_code = 200
        yield mock_post

# Mock LLM Parser
@pytest.fixture
def mock_llm_parser():
    with patch('compose.query_actions.filter_q.LLMParser.parse_response') as mock_parse_response:
        mock_parse_response.return_value = mock_response
        yield mock_parse_response

# Test FilterExactMatch class
class TestFilterExactMatch:

    def test_process_exact_match_success(self, mock_load_file):
        filter_exact = FilterExactMatch(sample_query)
        result = filter_exact.process(sample_params, sample_actions_confs)
        assert result is None

# Test FilterGPT class
class TestFilterGPT:

    def test_process_gpt_success(self, mock_load_file, mock_requests_post, mock_llm_parser):
        filter_gpt = FilterGPT(sample_query)
        sample_params['headers'] = {"auth": "test"}
        sample_params["langfuse"] = mock_langfuse
        filter_gpt.process(sample_params, sample_actions_confs)

    def test_process_gpt_empty_query(self):
        filter_gpt = FilterGPT("")
        with pytest.raises(PrintableGenaiError, match="Query is empty, cannot filter"):
            filter_gpt.process(sample_params, sample_actions_confs)

    def test_process_gpt_request_failure(self, mock_load_file, mock_requests_post):
        # Mock failed request
        mock_requests_post.return_value.status_code = 500
        mock_requests_post.return_value.text = "Internal Server Error"
        sample_params['headers'] = {"auth": "test"}
        sample_params["langfuse"] = mock_langfuse
        filter_gpt = FilterGPT(sample_query)
        
        with pytest.raises(PrintableGenaiError, match="Internal Server Error"):
            filter_gpt.process(sample_params, sample_actions_confs)

# Test FilterFactory class
class TestFilterFactory:

    def test_factory_valid_type(self):
        # Test with a valid filter_type
        factory = FilterFactory(filter_type="llm")
        assert isinstance(factory.filtermethod, type(FilterGPT))

    def test_factory_invalid_type(self):
        # Test with an invalid filter_type, should raise PrintableGenaiError
        with pytest.raises(PrintableGenaiError, match="Provided query filter method does not match any of the possible ones"):
            FilterFactory(filter_type="invalid_type")

    def test_factory_process(self, mock_load_file, mock_requests_post, mock_llm_parser):
        # Mock the factory to ensure it calls the correct method
        factory = FilterFactory(filter_type="llm")
        sample_params['headers'] = {"auth": "test"}
        sample_params["langfuse"] = mock_langfuse
        factory.process(query=sample_query, params=sample_params, actions_confs=sample_actions_confs)

# Test FilterMethod abstract base class
def test_filter_method_abstract():
    # Attempting to instantiate the abstract base class should raise TypeError
    with pytest.raises(TypeError):
        FilterMethod(sample_query)
