### This code is property of the GGAO ###

import pytest
from unittest.mock import patch, MagicMock
from compose.actions.filter_response import FilterLLM, FilterResponseFactory
from common.errors.genaierrors import PrintableGenaiError
import json

# Define sample data and mocks
sample_streamlist = [MagicMock(answer="sample response", content="sample content1")]
sample_params = {
    "headers": {"Authorization": "Bearer mock_token"},
    "template": "mock_template",
    "query": "test query"
}
mock_template_content = json.dumps({
    "substitutions_template": "template",
    "substitutions": [{"from": "sample response", "to": "filtered response", "extra_words": ['extra1", "extra2'], "randpick": 1}]
})
mock_response = {"answer": "sample response with modifications"}

# Mock environment variable
@pytest.fixture(autouse=True)
def mock_env_url():
    with patch.dict('os.environ', {'URL_LLM': 'https://mockurl.com'}):
        yield

# Mock loading template file from S3
@pytest.fixture
def mock_load_file():
    with patch('compose.actions.filter_response.load_file') as mock_load:
        mock_load.return_value = mock_template_content.encode()
        yield mock_load

# Mock requests.post for FilterLLM
@pytest.fixture
def mock_requests_post():
    with patch('requests.post') as mock_post:
        mock_post.return_value.json.return_value = mock_response
        mock_post.return_value.status_code = 200
        yield mock_post

# Mock LLM Parser
@pytest.fixture
def mock_llm_parser():
    with patch('compose.actions.filter_response.LLMParser.parse_response') as mock_parse_response:
        mock_parse_response.return_value = mock_response
        yield mock_parse_response

# Test FilterLLM class
class TestFilterLLM:

    def test_load_filter_raise(self, mock_load_file):
        mock_load_file.side_effect = ValueError
        filter_llm = FilterLLM(sample_streamlist)

        with pytest.raises(PrintableGenaiError, match="Error 404"):
            filter_llm.load_filtertemplate("test")
        

    def test_process_llm_with_substitution_in_answer(self, mock_load_file, mock_requests_post, mock_llm_parser):
        # Instantiate FilterLLM with sample streamlist
        filter_llm = FilterLLM(sample_streamlist)
        result = filter_llm.process(sample_params)
        
        # Check that the filter substitution took place and modified the streamlist
        assert result[-1].answer == "sample response with modifications"

    def test_process_llm_no_answer_in_streamlist(self, mock_load_file):
        # Test when the streamlist has no answer
        empty_streamlist = [MagicMock(answer=None)]
        filter_llm = FilterLLM(empty_streamlist)
        
        sample_params['headers'] = {"Authorization": "Bearer mock_token"}
        sample_params['query'] = "test query"
        with pytest.raises(PrintableGenaiError, match="No answer found to filter"):
            filter_llm.process(sample_params)

    def test_process_llm_empty_streamlist(self):
        # Test when the streamlist is empty
        filter_llm = FilterLLM([])
        
        with pytest.raises(PrintableGenaiError, match="Streamlist is empty, cannot filter response"):
            filter_llm.process(sample_params)

    def test_process_llm_request_failure(self, mock_load_file, mock_requests_post):
        # Mock a failed request
        mock_requests_post.return_value.status_code = 500
        mock_requests_post.return_value.text = "Internal Server Error"
        filter_llm = FilterLLM(sample_streamlist)
        
        sample_params['headers'] = {"Authorization": "Bearer mock_token"}
        sample_params['query'] = "test query"
        with pytest.raises(PrintableGenaiError, match="Internal Server Error"):
            filter_llm.process(sample_params)

    def test_process_llm_no_substitution_match(self, mock_load_file, mock_requests_post, mock_llm_parser):
        # Modify the mock response so it doesn’t match the `from` substitution value
        sample_params['headers'] = {"Authorization": "Bearer mock_token"}
        sample_params['query'] = "test query"
        filter_llm = FilterLLM([MagicMock(answer="non-matching response", content="test content")])
        filter_llm.process(sample_params)

# Test FilterResponseFactory
class TestFilterResponseFactory:

    def test_factory_valid_filter_type(self, mock_load_file, mock_requests_post, mock_llm_parser):
        sample_params['headers'] = {"Authorization": "Bearer mock_token"}
        sample_params['query'] = "test query"
        factory = FilterResponseFactory("llm")
        result = factory.process(sample_streamlist, sample_params)
        
        # Verify that the correct filter was applied
        assert isinstance(result, list)
        assert result[-1].answer == "sample response with modifications"

    def test_factory_invalid_filter_type(self):
        with pytest.raises(PrintableGenaiError, match="Provided filter does not match any of the possible ones"):
            FilterResponseFactory("invalid_type")

    def test_factory_empty_filtered_streamlist(self):
        sample_params['headers'] = {"Authorization": "Bearer mock_token"}
        sample_params['query'] = "test query"
        factory = FilterResponseFactory("llm")
        # Use an empty streamlist to test the factory's process method
        with pytest.raises(PrintableGenaiError, match="Error 400"):
            factory.process([], sample_params)
