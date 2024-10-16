### This code is property of the GGAO ###

import pytest
from unittest.mock import MagicMock, patch
from actionsmanager import ActionsManager
from common.errors.genaierrors import GenaiError, PrintableGenaiError


@pytest.fixture
def sample_compose_confs():
    return [
        {
            "action": "retrieve",
            "action_params": {
                "params": {
                    "generic": {
                        "index_conf": {
                            "add_highlights": False,
                            "index": "$index",
                            "query": "$query",
                            "task": "retrieve",
                            "top_k": 5,
                            "filters": "$filters",
                        },
                        "process_type": "ir_retrieve",
                    },
                    "headers_config": {
                        "x-reporting": "",
                        "x-department": "main",
                        "x-tenant": "techhubragemeal",
                        "x-limits": "{}",
                        "user-token": "",
                    },
                },
                "type": "get_chunks",
            },
        },
        {
            "action": "llm_action",
            "action_params": {
                "params": {
                    "llm_metadata": {"model": "$model", "max_input_tokens": 5000},
                    "platform_metadata": {"platform": "azure"},
                    "query_metadata": {
                        "query": "$query",
                        "system": "You are a helpful assistant",
                        "template_name": "system_query_and_context_plus",
                        "lang": "en",
                    },
                    "headers_config": {
                        "x-reporting": "",
                        "x-department": "main",
                        "x-tenant": "techhubragemeal",
                        "x-limits": "{}",
                        "user-token": "",
                    },
                },
                "type": "llm_content",
            },
        }
    ]


@pytest.fixture
def sample_params():
    return {
        'query': 'What does the sort action do?', 
        'strategy': 'genai_retrieval', 
        'model': 'gpt-3.5-pool-europe', 
        'index': 'testcomposellminforetrieval', 
        'filters': {'filename': []}, 
        'top_k': 5, 
        'top_qa': 3
    }


@pytest.fixture
def manager(sample_compose_confs, sample_params):
    logger_mock = MagicMock()
    manager = ActionsManager(compose_confs=sample_compose_confs, params=sample_params)
    manager.logger = logger_mock
    return manager


def test_parse_input_retrieve_action_success(manager):
    """Test the successful parsing of 'retrieve' action"""
    manager.parse_input(clear_quotes=False)

    assert len(manager.actions_confs) > 0
    assert manager.actions_confs[0]["action"] == "retrieve"
    assert manager.actions_confs[0]["action_params"]["type"] == "get_chunks"


def test_parse_input_no_retrieve_action_in_params(manager):
    """Test retrocompatibility mode when 'retrieve' is not in params"""
    manager.params.pop('retrieve', None)
    manager.parse_input(clear_quotes=False)

    assert len(manager.actions_confs) == len(manager.compose_confs)
    assert manager.actions_confs[0]["action"] == "retrieve"
    assert manager.actions_confs[1]["action"] == "llm_action"


def test_parse_input_no_retrieve_action_raises_error(manager):
    """Test if exception is raised when no 'retrieve' action is present"""
    manager.params.pop('retrieve', None)
    manager.compose_confs.pop(0)  # Remove the retrieve action from compose_confs

    with pytest.raises(GenaiError, match="It has to be at least one retrieve in actions"):
        manager.parse_input(clear_quotes=False)


def test_default_template_params_success(manager):
    """Test if default template parameters are set correctly"""
    params = {"search_topic": "test topic"}
    updated_params = manager.default_template_params(params)

    assert updated_params["filters"] == {}
    assert updated_params["top_k"] == 5
    assert updated_params["search_topic"] == "test topic"


def test_safe_substitute(manager):
    """Test if placeholders in the template are correctly substituted"""
    template = {
        "action": "retrieve",
        "action_params": {
            "params": {
                "generic": {
                    "index_conf": {
                        "query": "$query"
                    }
                }
            }
        }
    }

    template_params = {"query": "What does the sort action do?"}

    substituted_template = manager.safe_substitute(template, template_params, clear_quotes=False)

    assert substituted_template["action_params"]["params"]["generic"]["index_conf"]["query"] == "What does the sort action do?"


def test_safe_substitute_incorrect_json(manager):
    """Test if safe_substitute raises an error when the substituted template is not valid JSON"""
    template = {
        "action": "retrieve",
        "action_params": {
            "params": {
                "generic": {
                    "index_conf": {
                        "query": "$invalid_key"
                    }
                }
            }
        }
    }

    template_params = {"invalid_key": "{malformed_json"}

    with pytest.raises(PrintableGenaiError, match="After substitution template is not json serializable"):
        manager.safe_substitute(template, template_params, clear_quotes=False)


def test_preprocess_query_success(manager):
    """Test if preprocess_query correctly modifies the action params"""
    template_dict = {
        "action": "retrieve",
        "action_params": {
            "params": {
                "generic": {
                    "index_conf": {
                        "query": "test query",
                        "top_k": 5  
                    }
                }
            }
        }
    }
    template_params = {"search_topic": "updated search topic"}

    processed_template = manager.preprocess_query(template_dict, template_params)

    assert processed_template["action_params"]["params"]["generic"]["index_conf"]["query"] == "updated search topic"


def test_check_llm_action_params_with_valid_template(manager):
    """Test if check_llm_action_params works with valid llm_action params"""
    manager.actions_confs = [
        {
            "action": "llm_action",
            "action_params": {
                "params": {"query_metadata": {"template": "{'user': '$query'}"}}
            }
        }
    ]

    manager.check_llm_action_params()


def test_check_llm_action_params_raises_error_with_invalid_template(manager):
    """Test if check_llm_action_params raises error when template is missing $query"""
    manager.actions_confs = [
        {
            "action": "llm_action",
            "action_params": {
                "params": {"query_metadata": {"template": "{'user': 'invalid_template'}"}}
            }
        }
    ]

    with pytest.raises(PrintableGenaiError) as excinfo:
        manager.check_llm_action_params()
    assert "Template must contain" in str(excinfo.value)


def test_get_and_drop_query_actions(manager):
    """Test if query actions are dropped from actions_confs and moved to query_actions_confs"""
    manager.actions_confs = [
        {"action": "expansion"},
        {"action": "retrieve"}
    ]

    manager.get_and_drop_query_actions()

    assert len(manager.actions_confs) == 1
    assert len(manager.query_actions_confs) == 1
    assert manager.query_actions_confs[0]["action"] == "expansion"
    assert manager.actions_confs[0]["action"] == "retrieve"


def test_assert_json_serializable(manager):
    """Test if assert_json_serializable returns correct params"""
    params = {
        "key1": "value1",
        "key2": {"nested_key": "nested_value"}
    }

    serialized_params = manager.assert_json_serializable(params, clear_quotes=True)

    assert serialized_params["key1"] == "value1"
    assert "nested_key" in serialized_params["key2"]
