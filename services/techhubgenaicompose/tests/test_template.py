# test_persist.py

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
    template_manager.name = "test_template"
    
    # Call load_template to load the mock template
    template_manager.load_template()

    # Validate if the template is loaded properly
    assert template_manager.template == '{"mock_template": "data"}'
    mock_load_file.assert_called_once_with('mock_workspace', 'src/compose/templates/test_template.json')


@patch('pcutils.template.load_file')
@patch('pcutils.template.storage_containers', {'workspace': 'mock_workspace'})
def test_load_template_list_with_probs(mock_load_file, template_manager):
    """Test load_template with a list of names and probabilities."""
    mock_load_file.return_value = b'{"mock_template": "data"}'
    template_manager.name = ['template_a', 'template_b']
    template_manager.probs = [1, 2]  # Probabilities for choosing templates
    
    with patch('random.choices', return_value=['template_b']):
        template_manager.load_template()

    # Validate the chosen template and that it was loaded
    assert template_manager.template == '{"mock_template": "data"}'
    mock_load_file.assert_called_once_with('mock_workspace', 'src/compose/templates/template_b.json')

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
    
    template_manager.parse(compose_config)

    # Assert that parameters are set correctly after parsing
    assert template_manager.name == "sample_template"
    assert template_manager.top_k == 10
    assert template_manager.query == "test query"
    assert template_manager.filter_template == "sample_filter"

def test_parse_compose_config_error(template_manager):
    """Test parsing the compose configuration."""
    compose_config = {}

    with pytest.raises(PrintableGenaiError) as exc_info:
        template_manager.parse(compose_config)
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
        template_manager.parse(compose_config)
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

    template_manager.parse(compose_config)
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
                    'generic': {
                        'index_conf': {
                            'query': "sample query"
                        }
                    }
                }
            }
        }
    ]
    template_params = {'search_topic': 'new topic'}
    result = template_manager.run(template_dict, template_params)

    # Check if the query was updated correctly
    assert result[0]['action_params']['params']['generic']['index_conf']['query'] == 'new topic'
    assert result[0]['action_params']['params']['generic']['index_conf']['top_k'] == 5  # default top_k

def test_run_based_on(template_manager):
    """Test the run method's behavior."""
    template_dict = [
        {
            'action': 'retrieve',
            'action_params': {
                'params': {
                    'generic': {
                        'index_conf': {
                            'query': "based on fernando alonso"
                        }
                    }
                }
            }
        }
    ]
    template_params = {}
    result = template_manager.run(template_dict, template_params)

    # Check if the query was updated correctly
    assert result[0]['action_params']['params']['generic']['index_conf']['query'] == 'fernando alonso'

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
    temp_man.parse(compose_config)

    template_dict = [
        {
            'action': 'retrieve',
            'action_params': {
                'params': {
                    'generic': {
                        'index_conf': {
                            'query': "sample query"
                        }
                    }
                }
            }
        }
    ]
    template_params = {}
    result = temp_man.run(template_dict, template_params)

    # Check if the query was updated correctly
    assert result[0]['action_params']['params']['generic']['index_conf']['query'] == 'sample query'
    assert result[0]['action_params']['params']['generic']['index_conf']['top_k'] == 5  # default top_k

@patch('random.choices', side_effect=Exception())
def test_load_template_name_is_list_but_probs_not_defined(mock_random_choices, template_manager):
    template_manager.name = ["template1", "template2"]
    template_manager.probs = None  # Probs not defined

    with pytest.raises(PrintableGenaiError) as exc_info:
        template_manager.load_template()
        
    assert "If name field is a list, probs field must be defined" in str(exc_info.value)
    mock_random_choices.assert_called_once()

@patch('pcutils.template.load_file', side_effect=ValueError("File not found"))
def test_load_template_file_not_found(mock_load_file, template_manager):
    S3_TEMPLATEPATH = "src/compose/templates"
    template_manager.name = "template_not_exist"

    with pytest.raises(PrintableGenaiError) as exc_info:
        template_manager.load_template()

    assert "S3 config file doesn't exists for name" in str(exc_info.value)
    mock_load_file.assert_called_once_with(storage_containers['workspace'], f"{S3_TEMPLATEPATH}/template_not_exist.json")

@patch('pcutils.template.load_file', return_value=b"")
def test_load_template_file_empty(mock_load_file, template_manager):
    S3_TEMPLATEPATH = "src/compose/templates"
    template_manager.name = "template_empty"

    with pytest.raises(PrintableGenaiError) as exc_info:
        template_manager.load_template()

    assert "Compose template not found" in str(exc_info.value)
    mock_load_file.assert_called_once_with(storage_containers['workspace'], f"{S3_TEMPLATEPATH}/template_empty.json")
