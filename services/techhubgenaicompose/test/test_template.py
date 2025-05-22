### This code is property of the GGAO ###

import os
os.environ['URL_LLM'] = "test_url"
os.environ['URL_RETRIEVE'] = "test_retrieve"
import pytest
from unittest.mock import MagicMock, patch
from pcutils.template import TemplateManager
from common.genai_controllers import storage_containers
from common.errors.genaierrors import PrintableGenaiError

@pytest.fixture
def template_manager():
    """Fixture for creating a TemplateManager instance."""
    return TemplateManager()


@patch('pcutils.template.load_file')
@patch('pcutils.template.storage_containers', {'workspace': 'mock_workspace'})
def test_load_template(mock_load_file, template_manager):
    """Test loading a template from S3."""
    # Mock the S3 load file function to return a template.
    mock_load_file.return_value = b'{"mock_template": "data"}'
    template_manager.langfuse_m = MagicMock()
    template_manager.name = "test_template"
    
    # Call load_template to load the mock template
    template_manager.load_template()



def test_parse_compose_config(template_manager):
    """Test parsing the compose configuration."""
    compose_config = {
        "template": {
            "name": "sample_template",
            "params": {
                "top_k": 10,
                "query": "test query"
            }
        },
        "queryfilter_template": "sample_filter"
    }
    
    template_manager.parse(compose_config, MagicMock())

    # Assert that parameters are set correctly after parsing
    assert template_manager.name == "sample_template"
    assert template_manager.top_k == 10
    assert template_manager.query == "test query"
    assert template_manager.filter_template == "sample_filter"

def test_parse_compose_config_error(template_manager):
    """Test parsing the compose configuration."""
    compose_config = {}

    with pytest.raises(PrintableGenaiError) as exc_info:
        template_manager.parse(compose_config, MagicMock())
    assert "Template conf not found" in str(exc_info.value)

def test_parse_compose_config_error_name(template_manager):
    """Test parsing the compose configuration."""
    compose_config = {
        "template": {
            "not_name": "sample_template",
            "params": {
                "top_k": 10,
                "query": "test query"
            }
        },
        "queryfilter_template": "sample_filter"
    }

    with pytest.raises(PrintableGenaiError) as exc_info:
        template_manager.parse(compose_config, MagicMock())
    assert "Mandatory param" in str(exc_info.value)


def test_parse_compose_config_error_query(template_manager):
    """Test parsing the compose configuration."""
    compose_config = {
        "template": {
            "name": "sample_template",
            "params": {
                "top_k": 10,
                "query": ""
            }
        },
        "queryfilter_template": "sample_filter"
    }

    template_manager.parse(compose_config, MagicMock())
    assert template_manager.query is None


def test_default_template_params(template_manager):
    """Test setting default template parameters."""
    template_manager.params = {}
    template_manager.default_template_params()
    
    # Verify that default values are set correctly
    assert template_manager.params['filters'] == {}
    assert template_manager.params['top_k'] == 5


def test_set_params(template_manager):
    """Test setting query, query_type, and top_qa params."""
    template_manager.query = "test_query"
    template_manager.query_type = "test_type"
    template_manager.top_qa = 3

    template_params = {}
    result = template_manager.set_params(template_params)

    # Check if params are set correctly
    assert result['top_qa'] == 3
    assert result['query_type'] == "test_type"
    assert result['query'] == "test_query"


def test_run(template_manager):
    """Test the run method's behavior."""
    template_dict = [
        {
            'action': 'retrieve',
            'action_params': {
                'params': {
                        'indexation_conf': {
                            'query': "sample query"
                        }
                }
            }
        }
    ]
    template_params = {'search_topic': 'new topic'}
    result = template_manager.run(template_dict, template_params)

    # Check if the query was updated correctly
    assert result[0]['action_params']['params']['indexation_conf']['query'] == 'new topic'
    assert result[0]['action_params']['params']['indexation_conf']['top_k'] == 5  # default top_k

def test_run_based_on(template_manager):
    """Test the run method's behavior."""
    template_dict = [
        {
            'action': 'retrieve',
            'action_params': {
                'params': {
                        'indexation_conf': {
                            'query': "based on fernando alonso"
                        }
                }
            }
        }
    ]
    template_params = {}
    result = template_manager.run(template_dict, template_params)

    # Check if the query was updated correctly
    assert result[0]['action_params']['params']['indexation_conf']['query'] == 'fernando alonso'

def test_run_not_top_k(template_manager):
    """Test the run method's behavior."""
    temp_man = TemplateManager()
    compose_config = {
        "template": {
            "name": "sample_template",
            "params": {
                "query": "test query"
            }
        },
        "queryfilter_template": "sample_filter"
    }
    temp_man.parse(compose_config, MagicMock())

    template_dict = [
        {
            'action': 'retrieve',
            'action_params': {
                'params': {
                        'indexation_conf': {
                            'query': "sample query"
                        }
                }
            }
        }
    ]
    template_params = {}
    result = temp_man.run(template_dict, template_params)

    # Check if the query was updated correctly
    assert result[0]['action_params']['params']['indexation_conf']['query'] == 'sample query'
    assert result[0]['action_params']['params']['indexation_conf']['top_k'] == 5  # default top_k


@patch('pcutils.template.load_file', side_effect=ValueError("File not found"))
def test_load_template_file_not_found(mock_load_file, template_manager):
    S3_TEMPLATEPATH = "src/compose/templates"
    template_manager.name = "template_not_exist"
    lang_mock = MagicMock()
    t_mock = MagicMock()
    t_mock.side_effect = ValueError
    lang_mock.load_template = t_mock
    template_manager.langfuse_m = lang_mock
    template_manager.langfuse_m.side_effect = ValueError

    with pytest.raises(PrintableGenaiError) as exc_info:
        template_manager.load_template()

    assert "Template doesn't exists for name" in str(exc_info.value)

