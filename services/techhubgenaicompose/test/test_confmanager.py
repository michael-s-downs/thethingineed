### This code is property of the GGAO ###

import os
os.environ['URL_LLM'] = "test_url"
os.environ['URL_RETRIEVE'] = "test_retrieve"
import pytest
from unittest.mock import MagicMock, patch
from confmanager import ConfManager


@pytest.fixture
def compose_config():
    return {
        "headers_config": {
            "x-department": "engineering"
        },
        "clear_quotes": False,
        "template": {
            "name": "test_template",
            "params": {
                "model": "gpt-3.5"
            }
        },
        "session_id": None
    }


@pytest.fixture
def apigw_params():
    return {
        "x-department": "main",
        "user-token": "fake-token"
    }


@pytest.fixture
def mock_load_template():
    """Fixture to mock the load_file function globally."""
    with patch("confmanager.load_file") as mock_load:
        mock_load.return_value = b'{"action": "llm_action", "model": "gpt-3.5"}'
        yield mock_load


@pytest.fixture
def conf_manager(compose_config, apigw_params, mock_load_template):
    """Fixture to create a ConfManager instance with mocked load_file"""
    conf_manager = ConfManager(compose_config, apigw_params, MagicMock())
    conf_manager.logger = MagicMock()  # Mock the logger to avoid output
    return conf_manager


def test_init(conf_manager):
    """Test the ConfManager initialization"""
    assert conf_manager.department == "main"
    assert conf_manager.session_id is not None
    assert conf_manager.clear_quotes is False


@patch("confmanager.TemplateManager")
@patch("confmanager.TemplateManager.parse", return_value = MagicMock(query="query test"))
@patch("confmanager.PersistManager")
@patch("confmanager.LangFuseManager")
def test_parse_conf_actions(mock_langfuse, mock_persist, mock_template, conf_manager, compose_config):
    """Test parsing of config actions"""
    conf_manager.parse_conf_actions(compose_config)
    


def test_parse_session_existing_session(conf_manager, compose_config):
    """Test when session_id is already present in the config"""
    compose_config["session_id"] = "test-session"
    session_id = conf_manager.parse_session(compose_config)
    assert session_id == "test-session/gpt-3.5"


@patch("confmanager.load_file")
def test_parse_session_template_name_not_found(mock_load_file, conf_manager, compose_config):
    """Test handling when the template file is not found"""
    mock_load_file.side_effect = FileNotFoundError
    compose_config["template"]["name"] = "invalid_template"
    compose_config["template"]["params"]["model"] = None
    
    with pytest.raises(Exception, match="Template file doesn't exists for name invalid_template"):
        conf_manager.parse_session(compose_config)


def test_parse_lang_detection(conf_manager, compose_config):
    """Test language detection when lang is not provided"""
    query = "Hello, how are you?"
    
    lang = conf_manager.parse_lang(compose_config, query)
    assert lang == "en"


def test_parse_lang_provided(conf_manager, compose_config):
    """Test when language is already provided in the config"""
    compose_config["lang"] = "es"
    query = "Hola, ¿cómo estás?"
    
    lang = conf_manager.parse_lang(compose_config, query)
    assert lang == "es"

def test_parse_lang_provided_list_url(conf_manager, compose_config):
    """Test when language is already provided in the config"""
    compose_config["lang"] = ["es"]
    query = "http: Hola, ¿cómo estás?"
    
    lang = conf_manager.parse_lang(compose_config, query)
    assert lang == ""


def test_parse_lang_provided_dict(conf_manager, compose_config):
    """Test when language is already provided in the config"""
    compose_config["lang"] = {}
    query = "Hola, ¿cómo estás?"
    
    lang = conf_manager.parse_lang(compose_config, query)
    assert lang == ""

def test_clean_model(conf_manager):
    """Test the cleaning of model name"""
    model = "genai-gpt-3.5-pool-europe"
    cleaned_model = conf_manager.clean_model(model)
    assert cleaned_model == "gpt-3.5"

def test_template_no_name_param(conf_manager, compose_config):
    """Test case where no name param is found in the template"""
    
    del compose_config['template']['name']  # Remove the 'name' from the config
    compose_config["template"]["params"]["model"] = None

    with patch("confmanager.load_file", side_effect=FileNotFoundError):
        with pytest.raises(Exception, match="Mandatory param <name> not found in template."):
            conf_manager.parse_session(compose_config)



    