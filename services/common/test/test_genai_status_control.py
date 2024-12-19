### This code is property of the GGAO ###

import pytest
from unittest.mock import Mock, patch
from genai_status_control import *  # Replace 'your_module' with the name of the file where your code resides

# Mock the dbc object
mock_dbc = Mock()

# Replace dbc in the module with the mock
module_name = 'genai_status_control'  # Replace with the module name if needed
import sys
sys.modules[module_name].dbc = mock_dbc

@pytest.fixture
def origin():
    return ("service", "db")

@pytest.fixture
def key():
    return "test_key"

@pytest.fixture
def process_id():
    return "process_123"

@pytest.fixture
def doc_id():
    return "doc_456"

@pytest.fixture
def batch_id():
    return "batch_789"

@pytest.fixture
def dataset_status_key():
    return "dataset_status_001"

@pytest.fixture
def mock_value():
    return {"status": "complete", "msg": "All good"}

# Helper to mock select response
def mock_select_response(values):
    mock_dbc.select.return_value = [{"values": values.encode()}]

# Tests for create_status
def test_create_status(origin, key, mock_value):
    create_status(origin, key, mock_value["status"], mock_value["msg"])
    mock_dbc.insert.assert_called_once_with(origin, key, None, json.dumps(mock_value))

def test_create_status_no_msg(origin, key):
    create_status(origin, key, "complete", None)
    mock_dbc.insert.assert_called_with(origin, key, None, "complete")

# Tests for update_full_status
def test_update_full_status(origin, key, mock_value):
    update_full_status(origin, key, mock_value["status"], mock_value["msg"])
    mock_dbc.update.assert_called()

def test_update_full_status_no_msg(origin, key):
    update_full_status(origin, key, "complete", None)
    mock_dbc.update.assert_called()

# Tests for get_status_code
def test_get_status_code(origin, key, mock_value):
    mock_select_response(json.dumps(mock_value))
    result = get_status_code(origin, key, format_json=True)
    assert result == "complete"
    mock_dbc.select.assert_called_once_with(origin, key, None)

# Tests for get_redis_pattern
def test_get_redis_pattern(origin):
    mock_dbc.select.return_value = ["key1", "key2"]
    result = get_redis_pattern(origin, "pattern*")
    assert result == ["key1", "key2"]
    mock_dbc.select.assert_called_with(origin, None, None, match="pattern*")

# Tests for get_value
def test_get_value(origin, key, mock_value):
    mock_select_response(json.dumps(mock_value))
    result = get_value(origin, key, format_json=True)
    assert result == mock_value
    mock_dbc.select.assert_called_with(origin, key, None)

def test_get_value_except(origin, key, mock_value):
    with patch("genai_status_control.dbc.select") as mock_select_except:
        mock_select_except.side_effect = Exception
        result = get_value(origin, key, format_json=True)
    assert result == {}

# Tests for delete_status
def test_delete_status(origin, key):
    delete_status(origin, key)
    mock_dbc.delete.assert_called_once_with(origin, [key])

# Tests for incr_status_count
def test_incr_status_count(origin, key):
    incr_status_count(origin, key, 2)
    mock_dbc.update.assert_called_with(origin, key, None, None, incr=2)

# Tests for decr_status_count
def test_decr_status_count(origin, key):
    decr_status_count(origin, key, 3)
    mock_dbc.update.assert_called_with(origin, key, None, None, decr=3)

# Tests for compose functions
def test_compose_status_key():
    assert compose_status_key("process_123", "key1", False) == "process_123:key1"
    assert compose_status_key("process_123", "key1", True) == "process_123:key1:counter"

def test_compose_batch_key():
    assert compose_batch_key("proc1", "doc1", "batch1") == "proc1:doc1:batch1"

def test_compose_batch_keys():
    result = list(compose_batch_keys("proc1", "doc1", 3))
    assert result == ["proc1:doc1:0", "proc1:doc1:1", "proc1:doc1:2"]

def test_compose_document_key():
    assert compose_document_key("proc1", "doc1") == "proc1:doc1"

def test_compose_cluster_key(dataset_status_key):
    assert compose_cluster_key(dataset_status_key, "suffix1") == "dataset_status_001:suffix1"

def test_compose_status_model_key(dataset_status_key):
    assert compose_status_model_key(dataset_status_key, "symbol1", "model1") == "dataset_status_001:symbol1:model1"

# Tests for parse functions
def test_parse_status_json():
    input_value = [{"values": json.dumps({"valid": True}).encode()}]
    result = parse_status_json(input_value)
    assert result == {"valid": True}

def test_parse_status_paths():
    input_value = [
        {"values": json.dumps({"valid": True, "paths": [{"text": "path1"}]}).encode()},
        {"values": json.dumps({"valid": False}).encode()},
    ]
    result = parse_status_paths(input_value)
    assert result == ["path1"]

# Tests for get_status_paths
def test_get_status_paths(origin, process_id, doc_id):
    mock_select_response(json.dumps({"valid": True, "paths": [{"text": "path1", "number": 2}]}))
    mock_dbc.select.side_effect = lambda origin, key, *args: [{"values": json.dumps({"number": 1}).encode()}] if key.endswith("0") else []
    result = get_status_paths(origin, process_id, doc_id, 1)
    assert result ==  [{'values': b'{"number": 1}'}]

# Test for get_num_counter
def test_get_num_counter(origin, key):
    # Mocking a valid response
    mock_dbc.select.return_value = [{"values": b"5"}]
    result = get_num_counter(origin, key)
    assert result == 0
    mock_dbc.select.assert_called_with(origin, key, None)

    # Mocking an empty response
    mock_dbc.select.return_value = []
    result = get_num_counter(origin, key)
    assert result == 0
    mock_dbc.select.assert_called_with(origin, key, None)

# Tests for compose_model_key
def test_compose_model_key(dataset_status_key):
    model_id = "model123"
    result = compose_model_key(dataset_status_key, model_id)
    assert result == f"{dataset_status_key}:{model_id}"

# Tests for compose_counter_features_key
def test_compose_counter_features_key(dataset_status_key):
    result = compose_counter_features_key(dataset_status_key)
    assert result == f"{dataset_status_key}:counter_features"

# Test for update_status
def test_update_status(origin, key):
    msg = "New status message"
    update_status(origin, key, msg)
    mock_dbc.update.assert_called_with(origin, key, None, msg)

# Tests for persist_images
def test_persist_images(origin, key):
    msg = "image_data_here"
    persist_images(origin, key, msg)
    mock_dbc.insert.assert_called_with(origin, key, None, msg)

def test_get_images_raw(origin, key):
    mock_value = {"images": ["image1.jpg", "image2.jpg"]}
    mock_select_response(json.dumps(mock_value))
    result = get_images(origin, key, format_json=False)
    assert result == []
