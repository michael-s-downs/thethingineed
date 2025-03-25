### This code is property of the GGAO ###

import os
os.environ['URL_LLM'] = "test_url"
os.environ['URL_RETRIEVE'] = "test_retrieve"
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
                    "indexation_conf": {
                        "add_highlights": False,
                        "index": "$index",
                        "query": "$query",
                        "task": "retrieve",
                        "top_k": 5,
                        "filters": "$filters",
                    },
                    "process_type": "ir_retrieve",
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
                "indexation_conf": {
                    "query": "$query"
                }
            }
        }
    }

    template_params = {"query": "What does the sort action do?"}

    substituted_template = manager.safe_substitute(template, template_params, clear_quotes=False)

    assert substituted_template["action_params"]["params"]["indexation_conf"]["query"] == "What does the sort action do?"


def test_safe_substitute_params_not_sub(manager):
    """Test if placeholders in the template are correctly substituted"""
    template = {
        "action": "retrieve",
        "action_params": {
            "params": {
                "indexation_conf": {
                    "query": "$query",
                    "error_param": "'$error_param'"
                }
            }
        }
    }

    template_params = {"query": "What does the sort action do?"}

    manager.safe_substitute(template, template_params, clear_quotes=False)
    



def test_safe_substitute_incorrect_json(manager):
    """Test if safe_substitute raises an error when the substituted template is not valid JSON"""
    template = {
        "action": "retrieve",
        "action_params": {
            "params": {
                    "indexation_conf": {
                        "query": "$invalid_key"
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
                    "indexation_conf": {
                        "query": "test query",
                        "top_k": 5  
                    }
            }
        }
    }
    template_params = {"search_topic": "updated search topic"}

    processed_template = manager.preprocess_query(template_dict, template_params)

    assert processed_template["action_params"]["params"]["indexation_conf"]["query"] == "updated search topic"

def test_preprocess_query_success_basedon(manager):
    """Test if preprocess_query correctly modifies the action params"""
    template_dict = {
        "action": "retrieve",
        "action_params": {
            "params": {
                    "indexation_conf": {
                        "query": "test query based on topic test",
                        "top_k": 5  
                    }
            }
        }
    }
    template_params = {}

    processed_template = manager.preprocess_query(template_dict, template_params)

    assert processed_template["action_params"]["params"]["indexation_conf"]["query"] == "topic test"


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

def test_check_llm_action_params_with_not_query(manager):
    """Test if check_llm_action_params works with valid llm_action params"""
    manager.actions_confs = [
        {
            "action": "llm_action",
            "action_params": {
                "params": {"query_metadata": {"template": "{'user': '$qery'}"}}
            }
        }
    ]

    with pytest.raises(PrintableGenaiError) as excinfo:
        manager.check_llm_action_params()
    assert "Template must contain" in str(excinfo.value)


def test_check_llm_action_params_raises_error_with_invalid_template(manager):
    """Test if check_llm_action_params raises error when template is missing $query"""
    manager.actions_confs = [
        {
            "action": "llm_action",
            "action_params": {
                "params": {"query_metadata": {"template": "{'user':}}"}}
            }
        }
    ]

    with pytest.raises(PrintableGenaiError) as excinfo:
        manager.check_llm_action_params()
    assert "Template is not" in str(excinfo.value)


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


def test_assert_json_serializable_execption(manager):
    """Test if assert_json_serializable returns correct params"""
    params = "hola"

    with pytest.raises(PrintableGenaiError) as excinfo:
        manager.assert_json_serializable(params, clear_quotes=True)
    assert "Params field must be" in str(excinfo.value)


def test_parse_input_no_retrieve_action(manager):
    """Test default retrieval action is added when no 'retrieve' is found."""
    manager.params.pop('retrieve', None)
    manager.parse_input(clear_quotes=False)

    assert len(manager.actions_confs) > 0
    assert manager.actions_confs[0]["action"] == "retrieve"



def test_parse_input_with_non_retrieve_action_set_default(manager):
    """Test if non-retrieve actions are correctly processed when 'retrieve' is in params."""
    
    manager.params['retrieve'] = [{"query": "Retrieve action test query", "index": "test_index"}]
    manager.compose_confs = []
    manager.compose_confs.append(
        {
            "action": "non_retrieve_action",
            "action_params": {
                "params": {
                        "indexation_conf": {
                            "query": "$query"
                        }
                }
            }
        }
    )
    
    manager.parse_input(clear_quotes=False)
    
    assert len(manager.actions_confs) > 1
    assert manager.actions_confs[0]["action"] == "retrieve"

def test_parse_input_with_non_retrieve_action(manager):
    """Test if non-retrieve actions are correctly processed when 'retrieve' is in params."""
    
    manager.params['retrieve'] = [{"query": "Retrieve action test query", "index": "test_index"}]
    manager.compose_confs.append(
        {
            "action": "non_retrieve_action",
            "action_params": {
                "params": {
                        "indexation_conf": {
                            "query": "$query"
                        }
                }
            }
        }
    )
    
    manager.parse_input(clear_quotes=False)
    
    assert len(manager.actions_confs) > 1
    assert manager.actions_confs[0]["action"] == "retrieve"

def test_safe_substitute_value_none(manager):
    """Test if template substitutes None values as empty strings"""
    template = {
        "action": "retrieve",
        "action_params": {
            "params": {
                    "indexation_conf": {
                        "query": "$query"
                    }
                }
        }
    }

    template_params = {"query": None}

    substituted_template = manager.safe_substitute(template, template_params, clear_quotes=False)

    assert substituted_template["action_params"]["params"]["indexation_conf"]["query"] == ''


def test_safe_substitute_value_bool(manager):
    """Test if template substitutes bool values as lowercase strings"""
    template = {
        "action": "retrieve",
        "action_params": {
            "params": {
                    "indexation_conf": {
                        "flag": "$flag"
                    }
            }
        }
    }

    template_params = {"flag": True}

    substituted_template = manager.safe_substitute(template, template_params, clear_quotes=False)

    assert substituted_template["action_params"]["params"]["indexation_conf"]["flag"] == True


def test_safe_substitute_value_dict(manager):
    """Test if template substitutes dict values correctly"""
    template = {
        "action": "retrieve",
        "action_params": {
            "params": {
                    "indexation_conf": {
                        "filters": "$filters"
                    }
            }
        }
    }

    template_params = {"filters": {"key": "value"}}

    substituted_template = manager.safe_substitute(template, template_params, clear_quotes=False)

    assert substituted_template["action_params"]["params"]["indexation_conf"]["filters"] == {'key': 'value'}


def test_safe_substitute_value_empty_dict(manager):
    """Test if template substitutes empty dict values correctly"""
    template = {
        "action": "retrieve",
        "action_params": {
            "params": {
                    "indexation_conf": {
                        "filters": "$filters"
                    }
            }
        }
    }

    template_params = {"filters": {}}

    substituted_template = manager.safe_substitute(template, template_params, clear_quotes=False)

    assert substituted_template["action_params"]["params"]["indexation_conf"]["filters"] == {}


def test_safe_substitute_value_with_braces(manager):
    """Test if template substitutes values that start and end with braces correctly"""
    template = {
        "action": "retrieve",
        "action_params": {
            "params": {
                    "indexation_conf": {
                        "data": "$data"
                    }
            }
        }
    }

    template_params = {"data": "{key: value}"}

    substituted_template = manager.safe_substitute(template, template_params, clear_quotes=False)

    assert substituted_template["action_params"]["params"]["indexation_conf"]["data"] == '{key: value}'


def test_safe_substitute_value_no_braces_or_digits(manager):
    """Test if template substitutes values that do not start/end with braces and are non-numeric"""
    template = {
        "action": "retrieve",
        "action_params": {
            "params": {
                    "indexation_conf": {
                        "query": "$query"
                    }
            }
        }
    }

    template_params = {"query": "test_query"}

    substituted_template = manager.safe_substitute(template, template_params, clear_quotes=False)

    assert substituted_template["action_params"]["params"]["indexation_conf"]["query"] == 'test_query'


def test_safe_substitute_value_with_square_brackets(manager):
    """Test if template substitutes values that start and end with square brackets correctly"""
    template = {
        "action": "retrieve",
        "action_params": {
            "params": {
                    "indexation_conf": {
                        "list_param": "$list_param"
                    }
            }
        }
    }

    template_params = {"list_param": "[1, 2, 3]"}

    substituted_template = manager.safe_substitute(template, template_params, clear_quotes=False)

    assert substituted_template["action_params"]["params"]["indexation_conf"]["list_param"] == [1, 2, 3]


def test_safe_substitute_value_numeric_string(manager):
    """Test if template substitutes numeric string values correctly"""
    template = {
        "action": "retrieve",
        "action_params": {
            "params": {
                    "indexation_conf": {
                        "numeric_param": "$numeric_param"
                    }
            }
        }
    }

    template_params = {"numeric_param": "123"}

    substituted_template = manager.safe_substitute(template, template_params, clear_quotes=False)

    assert substituted_template["action_params"]["params"]["indexation_conf"]["numeric_param"] == 123
