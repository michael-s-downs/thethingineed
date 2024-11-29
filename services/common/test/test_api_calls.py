### This code is property of the GGAO ###

import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
import time
import api_calls  # Replace with your actual module name

# Test inputs
@pytest.fixture
def base_request_json():
    return {
        "apigw_params": {"param1": "value1"},
        "documents_folder": "/test/folder",
        "documents": ["doc1.pdf", "doc2.pdf"],
        "documents_metadata": {},
        "process_ids": {},
        "ts_init": datetime.now().timestamp(),
        "client_profile": {"default_ocr": True, "model": "test_model"},
        "integration_id": "test_integration",
    }


# Mock setup for dependencies
@pytest.fixture
def mock_dependencies():
    with patch("conf_utils.get_model", return_value={"multilabel": False}), \
         patch("docs_utils.parse_file_name", side_effect=lambda x, _: x.split("/")[-1]), \
         patch("core_api.sync_classification_multiclass_request", return_value={"status": "finish", "process_id": "123", "result": [{"category": "TypeA", "confidence": 0.9}]}), \
         patch("core_api.async_classification_multilabel_request", return_value={"status": "waiting", "process_id": "abc123"}), \
         patch("core_api.async_status_request", side_effect=[{"status": "waiting"}, {"status": "ready"}]), \
         patch("core_api.async_result_request", return_value={"results": [{"filename": "doc1.pdf", "categories": [{"category": "TypeB", "confidence": 0.8}]}]}), \
         patch("api_calls.logger") as mock_logger:
        yield mock_logger


# Tests
def test_sync_classification_success(base_request_json, mock_dependencies):
    # Execute sync mode test
    result = api_calls.classification_sync(base_request_json)

    # Assertions
    assert result["documents_metadata"]["doc1.pdf"]["document_type"] == "TypeA"
    assert result["documents_metadata"]["doc1.pdf"]["document_type_confidence"] == 0.9
    assert not result["documents_metadata"]["doc1.pdf"]["async"]


def test_async_classification_success(base_request_json, mock_dependencies):
    # Adjust base_request_json to test async case
    base_request_json["documents_metadata"]["doc1.pdf"] = {}
    with patch("conf_utils.get_model", return_value={"multilabel": True}), \
         patch("core_api.async_classification_multilabel_request", return_value={"status": "waiting", "process_id": "async123"}), \
         patch("core_api.async_status_request", side_effect=[{"status": "waiting"}, {"status": "ready"}]), \
         patch("core_api.async_result_request", return_value={"results": [{"filename": "doc1.pdf", "categories": [{"category": "TypeB", "confidence": 0.8}]}]}):
        
        result = api_calls.classification_sync(base_request_json)

    # Assertions
    assert result["documents_metadata"]["doc1.pdf"]["categories"][0]["category"] == "TypeB"
    assert result["documents_metadata"]["doc1.pdf"]["async"] is True
    assert "async123" in result["process_ids"]


def test_classification_timeout(base_request_json, mock_dependencies):
    # Mock async_status_request to simulate timeout
    with patch("core_api.async_status_request", return_value={"status": "waiting"}), \
         patch("time.sleep", side_effect=lambda x: None):  # Speed up sleep
        base_request_json["ts_init"] = time.time() - api_calls.request_polling_timeout - 1
        result = api_calls.classification_sync(base_request_json)

    # Assertions
    assert "error" in result["documents_metadata"]["doc1.pdf"]
    assert result["documents_metadata"]["doc1.pdf"]["error"] == "timeout"


def test_error_handling_sync_classification(base_request_json, mock_dependencies):
    # Simulate an error in sync_classification_multiclass_request
    with patch("core_api.sync_classification_multiclass_request", side_effect=Exception("Sync Error")):
        result = api_calls.classification_sync(base_request_json)

    # Assertions
    assert result["process_ids"] == {}  # Process IDs remain empty due to the error


def test_error_handling_async_results(base_request_json, mock_dependencies):
    # Simulate error when fetching async results
    with patch("core_api.async_result_request", side_effect=Exception("Async Result Error")):
        base_request_json["client_profile"]["model"] = {"multilabel": True}
        result = api_calls.classification_sync(base_request_json)

    # Assertions
    assert "error" in result["documents_metadata"]["doc1.pdf"]
    assert result["documents_metadata"]["doc1.pdf"]["error"] == "Async Result Error"
