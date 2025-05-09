### This code is property of the GGAO ###

import os
os.environ["INTEGRATION_NAME"] = "search"
os.environ["STORAGE_PERSIST_FOLDER"] = "test"
os.environ["API_ASYNC_PROCESS_URL"] = "test_url_"
import json
import pytest
from unittest.mock import patch, MagicMock, mock_open
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
        "preprocess_conf": {},
        "process_ids": {'test_pid1':{}},
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


def test_async_classification_success(base_request_json, mock_dependencies):
    # Adjust base_request_json to test async case
    base_request_json["documents_metadata"]["doc1.pdf"] = {}
    with patch("conf_utils.get_model", return_value={"multilabel": True}), \
         patch("core_api.async_classification_multilabel_request", return_value={"status": "waiting", "process_id": "async123"}), \
         patch("core_api.async_status_request", side_effect=[{"status": "waiting", "process_id": "async123"}, {"status": "ready", "process_id": "async123"}]), \
         patch("core_api.async_result_request", return_value={"status": "waiting", "process_id": "async123", "results": [{"filename": "doc1.pdf", "categories": [{"category": "TypeB", "confidence": 0.8}]}]}), \
         patch("time.sleep"):
        
        result = api_calls.classification_sync(base_request_json)

    # Assertions
    assert result["documents_metadata"]["doc1.pdf"]["categories"][0]["category"] == "TypeB"
    assert result["documents_metadata"]["doc1.pdf"]["async"] is True
    assert "async123" in result["process_ids"]


def test_classification_timeout(base_request_json, mock_dependencies):
    # Mock async_status_request to simulate timeout
    with patch("core_api.async_status_request", return_value={"status": "ready"}), \
         patch("time.sleep", side_effect=lambda x: None):  # Speed up sleep
        base_request_json["ts_init"] = time.time() - api_calls.request_polling_timeout - 1
        api_calls.classification_sync(base_request_json)


def test_async_classification_exception(base_request_json, mock_dependencies):
    # Adjust base_request_json to test async case
    base_request_json["documents_metadata"]["doc1.pdf"] = {}
    with patch("conf_utils.get_model", return_value={"multilabel": True}), \
         patch("core_api.async_classification_multilabel_request", return_value={"status": "waiting", "process_id": "async123"}), \
         patch("core_api.async_status_request", side_effect=Exception), \
         patch("core_api.async_result_request", return_value={"status": "waiting", "process_id": "async123", "results": [{"filename": "doc1.pdf", "categories": [{"category": "TypeB", "confidence": 0.8}]}]}), \
         patch("time.sleep"):
        
        api_calls.classification_sync(base_request_json)


def test_async_classification_timeout(base_request_json, mock_dependencies):
    # Adjust base_request_json to test async case
    base_request_json["documents_metadata"]["doc1.pdf"] = {}
    with patch("conf_utils.get_model", return_value={"multilabel": True}), \
         patch("core_api.async_classification_multilabel_request", return_value={"status": "waiting", "process_id": "async123"}), \
         patch("core_api.async_status_request", side_effect=[{"status": "ready", "process_id": "async123"}, {"status": "ready", "process_id": "async123"}]), \
         patch("core_api.async_result_request", return_value=Exception), \
         patch("time.sleep"):
         
        base_request_json['ts_init'] = -100.0
        api_calls.classification_sync(base_request_json)

# Additional tests for extraction_sync
def test_extraction_sync_success(base_request_json, mock_dependencies):
    # Mock necessary dependencies for successful sync extraction
    base_request_json["documents_metadata"] = {
        "doc1.pdf": {"document_type": "TypeA"},
        "doc2.pdf": {"document_type": "TypeB"}
    }
    base_request_json['client_profile']['profile_name'] = "test_name"


    with patch("conf_utils.get_type", return_value={"_sync_extraction": True}), \
         patch("core_api.sync_extraction_request", return_value={"status": "finish", "process_id": "sync123", "result": {"field1": {"value": "val1", "confidence": 0.95}}}):
        
        result = api_calls.extraction_sync(base_request_json)

    # Assertions
    assert result["documents_metadata"]["doc1.pdf"]["document_fields"]["field1"]["value"] == "val1"
    assert result["documents_metadata"]["doc1.pdf"]["async"] is False
    assert "sync123" in result["process_ids"]


def test_extraction_async_success(base_request_json, mock_dependencies):
    # Mock necessary dependencies for successful async extraction
    base_request_json["documents_metadata"] = {
        "doc1.pdf": {"document_type": "TypeA"},
        "doc2.pdf": {"document_type": "TypeB"}
    }
    base_request_json['client_profile']['profile_name'] = "test_name"

    with patch("conf_utils.get_type", return_value={"_sync_extraction": False}), \
         patch("core_api.async_extraction_request", return_value={"status": "waiting", "process_id": "async123"}), \
         patch("core_api.async_status_request", side_effect=[{"status": "waiting"}, {"status": "ready"}]), \
         patch("core_api.async_result_request", return_value={"results": [{"filename": "doc1.pdf", "entities": {"field1": "value1"}}]}), \
         patch("time.sleep"):
        
        result = api_calls.extraction_sync(base_request_json)

    # Assertions
    assert result["documents_metadata"]["doc1.pdf"]["document_fields"]["field1"]["value"] == "value1"
    assert result["documents_metadata"]["doc1.pdf"]["async"] is True
    assert "async123" in result["process_ids"]


def test_extraction_missing_document_type(base_request_json, mock_dependencies):
    # Simulate a case where document_type is missing
    base_request_json["documents_metadata"] = {
        "doc1.pdf": {}
    }
    base_request_json['client_profile']['profile_name'] = "test_name"

    result = api_calls.extraction_sync(base_request_json)

    # Assertions
    assert result["documents_metadata"]["doc1.pdf"]["status"] == "error"
    assert result["documents_metadata"]["doc1.pdf"]["error"] == "Param 'document_type' required for extraction"


def test_extraction_timeout(base_request_json, mock_dependencies):
    # Mock async_status_request to simulate timeout
    base_request_json["documents_metadata"] = {
        "doc1.pdf": {"document_type": "TypeA"}
    }
    base_request_json['client_profile']['profile_name'] = "test_name"

    with patch("conf_utils.get_type", return_value={"_sync_extraction": False}), \
         patch("core_api.async_extraction_request", return_value={"status": "waiting", "process_id": "async123"}), \
         patch("core_api.async_status_request", return_value={"status": "waiting"}), \
         patch("time.sleep", side_effect=lambda x: None):  # Speed up sleep
        base_request_json["ts_init"] = time.time() - api_calls.request_polling_timeout - 1
        result = api_calls.extraction_sync(base_request_json)

    # Assertions
    assert result["documents_metadata"]["doc1.pdf"]["status"] == "error"
    assert result["documents_metadata"]["doc1.pdf"]["error"] == "timeout"


def test_extraction_async_exception(base_request_json, mock_dependencies):
    # Simulate an exception during async_status_request
    base_request_json["documents_metadata"] = {
        "doc1.pdf": {"document_type": "TypeA"}
    }
    base_request_json['client_profile']['profile_name'] = "test_name"

    with patch("conf_utils.get_type", return_value={"_sync_extraction": False}), \
         patch("core_api.async_extraction_request", return_value={"status": "waiting", "process_id": "async123"}), \
         patch("core_api.async_status_request", side_effect=Exception), \
         patch("time.sleep"):
        
        result = api_calls.extraction_sync(base_request_json)

    # Assertions
    assert result["documents_metadata"]["doc1.pdf"]["status"] == "error"


def test_extraction_async_result_exception(base_request_json, mock_dependencies):
    # Simulate an exception during async_result_request
    base_request_json["documents_metadata"] = {
        "doc1.pdf": {"document_type": "TypeA"}
    }
    base_request_json['client_profile']['profile_name'] = "test_name"

    with patch("conf_utils.get_type", return_value={"_sync_extraction": False}), \
         patch("core_api.async_extraction_request", return_value={"status": "waiting", "process_id": "async123"}), \
         patch("core_api.async_status_request", return_value={"status": "ready"}), \
         patch("core_api.async_result_request", side_effect=Exception), \
         patch("time.sleep"):
        
        result = api_calls.extraction_sync(base_request_json)

    # Assertions
    assert result["documents_metadata"]["doc1.pdf"]["status"] == "error"
    assert "Error getting result for request" in result["documents_metadata"]["doc1.pdf"]["error"]

# Tests for preprocess_async
def test_preprocess_async_success(base_request_json, mock_dependencies):
    # Mock necessary dependencies for a successful async preprocessing
    base_request_json["documents_metadata"] = {
        "doc1.pdf": {"status": "new"},
        "doc2.pdf": {"status": "new"}
    }

    with patch("core_api.async_preprocess_request", return_value={"status": "waiting", "process_id": "pre123"}):
        result = api_calls.preprocess_async(base_request_json)

    # Assertions
    assert "pre123" in result["process_ids"]
    assert result["documents_metadata"]["doc1.pdf"]["async"] is True
    assert result["status"] == "waiting"


def test_preprocess_async_no_files_to_process(base_request_json, mock_dependencies):
    # All files are already processed or in a terminal state
    base_request_json["documents_metadata"] = {
        "doc1.pdf": {"status": "ready"},
        "doc2.pdf": {"status": "error"}
    }

    with patch("core_api.async_preprocess_request") as mock_request:
        result = api_calls.preprocess_async(base_request_json)

    # Assertions
    assert mock_request.call_count == 0  # No API calls should be made
    assert result["status"] == "processing"


def test_preprocess_async_partial_status_update(base_request_json, mock_dependencies):
    # Mock async_preprocess_request to simulate a mixed state
    base_request_json["documents_metadata"] = {
        "doc1.pdf": {"status": "new"},
        "doc2.pdf": {"status": "waiting"}
    }

    with patch("core_api.async_preprocess_request", return_value={"status": "waiting", "process_id": "pre123"}):
        result = api_calls.preprocess_async(base_request_json)

    # Assertions
    assert result["process_ids"]["pre123"] == "waiting"
    assert result["status"] == "waiting"
    assert result["documents_metadata"]["doc1.pdf"]["status"] == "waiting"


def test_preprocess_async_ready_to_finish(base_request_json, mock_dependencies):
    # Simulate files that are ready to finish
    base_request_json["documents_metadata"] = {
        "doc1.pdf": {"process_id": "pre123", "status": "ready"},
        "doc2.pdf": {"process_id": "pre123", "status": "ready"}
    }
    base_request_json["process_ids"] = {"pre123": "ready"}

    result = api_calls.preprocess_async(base_request_json)

    # Assertions
    assert result["documents_metadata"]["doc1.pdf"]["status"] == "finish"
    assert result["documents_metadata"]["doc2.pdf"]["status"] == "finish"
    assert result["process_ids"]["pre123"] == "finish"
    assert result["status"] == "processing"


def test_preprocess_async_exception(base_request_json, mock_dependencies):
    # Simulate an exception during async_preprocess_request
    base_request_json["documents_metadata"] = {
        "doc1.pdf": {"status": "new"}
    }

    with patch("core_api.async_preprocess_request", side_effect=Exception("API error")):
        with pytest.raises(Exception):
            api_calls.preprocess_async(base_request_json)


def test_preprocess_async_update_status_from_waiting(base_request_json, mock_dependencies):
    # Simulate a process transitioning from waiting to finish
    base_request_json["documents_metadata"] = {
        "doc1.pdf": {"status": "waiting", "process_id": "pre123"}
    }
    base_request_json["process_ids"] = {"pre123": "ready"}

    result = api_calls.preprocess_async(base_request_json)

    # Assertions
    assert result["documents_metadata"]["doc1.pdf"]["status"] == "finish"
    assert result["process_ids"]["pre123"] == "finish"
    assert result["status"] == "processing"

def test_classification_async_success(base_request_json, mock_dependencies):
    # Simulate successful classification
    base_request_json["documents_metadata"] = {
        "doc1.pdf": {"status": "new"},
        "doc2.pdf": {"status": "new"}
    }

    mock_model_conf = {"multilabel": False}
    with patch("conf_utils.get_model", return_value=mock_model_conf), \
         patch("core_api.async_classification_multiclass_request", return_value={"status": "waiting", "process_id": "class123"}):
        result = api_calls.classification_async(base_request_json)

    # Assertions
    assert "class123" in result["process_ids"]
    assert result["status"] == "waiting"
    assert result["documents_metadata"]["doc1.pdf"]["async"] is True
    assert result["documents_metadata"]["doc2.pdf"]["async"] is True


def test_classification_async_no_files_to_classify(base_request_json, mock_dependencies):
    # All files are already classified
    base_request_json["documents_metadata"] = {
        "doc1.pdf": {"document_type": "invoice"},
        "doc2.pdf": {"categories": [{"category": "receipt"}]}
    }

    with patch("core_api.async_classification_multiclass_request") as mock_request:
        result = api_calls.classification_async(base_request_json)

    # Assertions
    assert mock_request.call_count == 0  # No API call should be made
    assert result["status"] == "processing"


def test_classification_async_with_multilabel(base_request_json, mock_dependencies):
    # Simulate classification with multilabel enabled
    base_request_json["documents_metadata"] = {"doc1.pdf": {"status": "new"}}
    mock_model_conf = {"multilabel": True}

    with patch("conf_utils.get_model", return_value=mock_model_conf), \
         patch("core_api.async_classification_multilabel_request", return_value={"status": "waiting", "process_id": "class123"}):
        result = api_calls.classification_async(base_request_json)

    # Assertions
    assert "class123" in result["process_ids"]
    assert result["status"] == "waiting"
    assert result["documents_metadata"]["doc1.pdf"]["async"] is True


def test_classification_async_ready_to_finish(base_request_json, mock_dependencies):
    # Simulate files ready to finish
    base_request_json["documents_metadata"] = {"doc1.pdf": {"status": "waiting", "process_id": "class123"}}
    base_request_json["process_ids"] = {"class123": "ready"}
    mock_results = [{"filename": "doc1.pdf", "categories": [{"category": "invoice", "confidence": 0.95}]}]

    with patch("core_api.async_result_request", return_value={"results": mock_results}):
        result = api_calls.classification_async(base_request_json)

    # Assertions
    assert result["documents_metadata"]["doc1.pdf"]["status"] == "finish"
    assert result["documents_metadata"]["doc1.pdf"]["document_type"] == "invoice"
    assert result["documents_metadata"]["doc1.pdf"]["document_type_confidence"] == 0.95
    assert result["process_ids"]["class123"] == "finish"
    assert result["status"] == "processing"


def test_classification_async_exception_handling(base_request_json, mock_dependencies):
    # Simulate an exception during result retrieval
    base_request_json["documents_metadata"] = {"doc1.pdf": {"status": "waiting", "process_id": "class123"}}
    base_request_json["process_ids"] = {"class123": "ready"}

    with patch("core_api.async_result_request", side_effect=Exception("API error")):
        result = api_calls.classification_async(base_request_json)

    # Assertions
    assert result["process_ids"]["class123"] == "error"
    assert result["documents_metadata"]["doc1.pdf"]["status"] == "error"


def test_classification_async_missing_metadata(base_request_json, mock_dependencies):
    # Simulate missing metadata for a document
    base_request_json["documents"] = ["doc1.pdf"]
    base_request_json["documents_metadata"] = {}

    with patch("conf_utils.get_model", return_value={"multilabel": False}), \
         patch("core_api.async_classification_multiclass_request", return_value={"status": "waiting", "process_id": "class123"}):
        result = api_calls.classification_async(base_request_json)

    # Assertions
    assert "class123" in result["process_ids"]
    assert result["documents_metadata"]["doc1.pdf"]["status"] == "waiting"
    assert result["status"] == "waiting"


def test_classification_async_update_request_params(base_request_json, mock_dependencies):
    # Test proper handling of request parameters
    base_request_json["documents_metadata"] = {"doc1.pdf": {"status": "new"}}
    mock_model_conf = {"multilabel": False}
    mock_type_conf = {"_ocr": True, "_force_ocr": False, "_need_preprocess": True}

    with patch("conf_utils.get_model", return_value=mock_model_conf), \
         patch("conf_utils.get_type", return_value=mock_type_conf), \
         patch("core_api.async_classification_multiclass_request", return_value={"status": "waiting", "process_id": "class123"}):
        result = api_calls.classification_async(base_request_json)

    # Assertions
    assert "class123" in result["process_ids"]
    assert result["documents_metadata"]["doc1.pdf"]["async"] is True
    assert result["status"] == "waiting"

def test_extraction_async_success_2(base_request_json, mock_dependencies):
    # Simulate successful extraction
    base_request_json["documents_metadata"] = {
        "doc1.pdf": {"document_type": "invoice", "status": "new"},
        "doc2.pdf": {"document_type": "receipt", "status": "new"}
    }
    base_request_json["process_ids"] = {"pre123": "ready"}
    base_request_json['client_profile']['profile_name'] = "test_name"

    mock_api_response = {"status": "waiting", "process_id": "extract123"}
    with patch("core_api.async_extraction_request", return_value=mock_api_response):
        result = api_calls.extraction_async(base_request_json)

    # Assertions
    assert "extract123" in result["process_ids"]
    assert result["status"] == "waiting"
    assert result["documents_metadata"]["doc1.pdf"]["async"] is True
    assert result["documents_metadata"]["doc2.pdf"]["async"] is True


def test_extraction_async_no_type_error(base_request_json, mock_dependencies):
    # Documents without a document_type should return an error
    base_request_json["documents_metadata"] = {
        "doc1.pdf": {"status": "new"},
        "doc2.pdf": {"status": "new"}
    }

    result = api_calls.extraction_async(base_request_json)

    # Assertions
    assert result["documents_metadata"]["doc1.pdf"]["status"] == "error"
    assert "document_type" in result["documents_metadata"]["doc1.pdf"]["error"]
    assert result["documents_metadata"]["doc2.pdf"]["status"] == "error"


def test_extraction_async_ready_to_finish(base_request_json, mock_dependencies):
    # Simulate files ready to finish
    base_request_json["documents_metadata"] = {
        "doc1.pdf": {"document_type": "invoice", "status": "waiting", "process_id": "extract123"}
    }
    base_request_json["process_ids"] = {"extract123": "ready"}
    base_request_json['client_profile']['profile_name'] = "test_name"
    mock_results = [
        {
            "filename": "doc1.pdf",
            "entities": {
                "invoice_number": "12345",
                "total_amount": {"value": "100.00", "confidence": 0.95}
            }
        }
    ]

    with patch("core_api.async_result_request", return_value={"results": mock_results}):
        result = api_calls.extraction_async(base_request_json)

    # Assertions
    assert result["documents_metadata"]["doc1.pdf"]["status"] == "finish"
    assert result["documents_metadata"]["doc1.pdf"]["document_fields"]["invoice_number"]["value"] == "12345"
    assert result["documents_metadata"]["doc1.pdf"]["document_fields"]["total_amount"]["confidence"] == 0.95
    assert result["process_ids"]["extract123"] == "finish"
    assert result["status"] == "processing"


def test_extraction_async_exception_handling(base_request_json, mock_dependencies):
    # Simulate an exception during result retrieval
    base_request_json["documents_metadata"] = {
        "doc1.pdf": {"document_type": "invoice", "status": "waiting", "process_id": "extract123"}
    }

    base_request_json["process_ids"] = {"extract123": "ready"}
    base_request_json['client_profile']['profile_name'] = "test_name"

    with patch("core_api.async_result_request", side_effect=Exception("API error")):
        result = api_calls.extraction_async(base_request_json)

    # Assertions
    assert result["process_ids"]["extract123"] == "error"
    assert result["documents_metadata"]["doc1.pdf"]["status"] == "error"


def test_extraction_async_grouping_and_reuse(base_request_json, mock_dependencies):
    # Test grouping by document_type and process_id
    base_request_json["documents_metadata"] = {
        "doc1.pdf": {"document_type": "invoice", "status": "new"},
        "doc2.pdf": {"document_type": "invoice", "status": "new", "process_id": "reuse123"},
        "doc3.pdf": {"document_type": "receipt", "status": "new"}
    }
    base_request_json['client_profile']['profile_name'] = "test_name"


    mock_api_response_invoice = {"status": "waiting", "process_id": "extract_invoice"}
    mock_api_response_receipt = {"status": "waiting", "process_id": "extract_receipt"}

    with patch("core_api.async_extraction_request", side_effect=[mock_api_response_invoice, mock_api_response_receipt]):
        result = api_calls.extraction_async(base_request_json)

    # Assertions
    assert "extract_invoice" in result["process_ids"]
    assert result["documents_metadata"]["doc1.pdf"]["process_id"] == "extract_invoice"


def test_extraction_async_update_request_params(base_request_json, mock_dependencies):
    # Test correct update of request parameters
    base_request_json["documents_metadata"] = {
        "doc1.pdf": {"document_type": "invoice", "status": "new"}
    }
    base_request_json['client_profile']['profile_name'] = "test_name"

    mock_type_conf = {
        "_ocr": True,
        "_force_ocr": False,
        "_need_preprocess": True,
        "invoice_number": {},
        "total_amount": {}
    }
    mock_api_response = {"status": "waiting", "process_id": "extract123"}

    with patch("conf_utils.get_type", return_value=mock_type_conf), \
         patch("core_api.async_extraction_request", return_value=mock_api_response):
        result = api_calls.extraction_async(base_request_json)

    # Assertions
    assert "extract123" in result["process_ids"]
    assert result["documents_metadata"]["doc1.pdf"]["async"] is True
    assert result["status"] == "waiting"

def test_delete_async_process(base_request_json, mock_dependencies):
    # Simulate deleting results for async processes
    base_request_json["process_ids"] = ["async123"]
    base_request_json["documents_metadata"] = {
        "doc1.pdf": {"async": True, "process_id": "async123"}
    }
    base_request_json["tracking"] = "test_tracking"

    with patch("core_api.async_delete_request") as mock_delete:
        result = api_calls.delete(base_request_json)

    # Assertions
    mock_delete.assert_called_once()
    assert result == base_request_json

def test_delete_queue_process(base_request_json, mock_dependencies):
    # Simulate deleting results for queue processes
    base_request_json["process_ids"] = ["queue123"]
    base_request_json["documents_metadata"] = {
        "doc1.pdf": {"async": "queue", "process_id": "queue123"}
    }
    base_request_json["tracking"] = "test_tracking"

    with patch("core_api.queue_delete_request") as mock_delete:
        result = api_calls.delete(base_request_json)

    # Assertions
    mock_delete.assert_called_once()
    assert result == base_request_json

def test_delete_sync_process(base_request_json, mock_dependencies):
    # Simulate deleting results for sync processes
    base_request_json["process_ids"] = ["sync123"]
    base_request_json["documents_metadata"] = {
        "doc1.pdf": {"async": False, "process_id": "sync123"}
    }
    base_request_json["tracking"] = "test_tracking"

    with patch("core_api.sync_delete_request") as mock_delete:
        result = api_calls.delete(base_request_json)

    # Assertions
    mock_delete.assert_called_once()
    assert result == base_request_json

def test_indexing_successful(base_request_json, mock_dependencies):
    # Simulate successful indexing
    base_request_json["documents"] = ["folder/doc1.pdf", "folder/doc2.pdf"]
    base_request_json["documents_metadata"] = {
        "doc1.pdf": {"status": "new"},
        "doc2.pdf": {"status": "new"}
    }
    base_request_json['indexation_conf'] = {'models': {}, 'vector_storage_conf': {'index': {}}}
    base_request_json['preprocess_conf'] = {'layout_conf': {}}
    
    mock_models_map = {"model1": "mapped_model1", "model2": "mapped_model2"}
    mock_api_response = {"status": "waiting", "process_id": "index123"}

    with patch("builtins.open", mock_open(read_data=json.dumps(mock_models_map))), \
        patch("core_api.async_indexing_request", return_value=mock_api_response):
        result = api_calls.indexing(base_request_json)

    # Assertions
    assert "index123" in result["process_ids"]
    assert result["documents_metadata"]["doc1.pdf"]["status"] == "waiting"
    assert result["documents_metadata"]["doc2.pdf"]["status"] == "waiting"
    assert result["status"] == "waiting"

def test_indexing_no_files(base_request_json, mock_dependencies):
    # Simulate request with no files to index
    base_request_json["documents"] = []
    base_request_json['indexation_conf'] = {'models': {}, 'vector_storage_conf': {'index': {}}}
    base_request_json['preprocess_conf'] = {'layout_conf': {}}
    mock_models_map = {"model1": "mapped_model1", "model2": "mapped_model2"}

    with patch("builtins.open", mock_open(read_data=json.dumps(mock_models_map))):
        result = api_calls.indexing(base_request_json)

    # Assertions
    assert result["status"] == "error"

def test_indexing_queue_mode(base_request_json, mock_dependencies):
    # Simulate queue-based indexing
    base_request_json["documents"] = ["folder/doc1.pdf"]
    base_request_json["documents_metadata"] = {
        "doc1.pdf": {"status": "new"}
    }
    base_request_json['preprocess_conf'] = {'layout_conf': {}}
    base_request_json['indexation_conf'] = {'models': {}, 'vector_storage_conf': {'index': {}}}
    mock_api_response = {"status": "waiting", "process_id": "queue_index123"}
    mock_models_map = {"model1": "mapped_model1", "model2": "mapped_model2"}

    with patch("os.getenv", return_value="queue_url"), \
         patch("builtins.open", mock_open(read_data=json.dumps(mock_models_map))), \
         patch("core_api.queue_indexing_request", return_value=mock_api_response):
        result = api_calls.indexing(base_request_json)

    # Assertions
    assert "queue_index123" in result["process_ids"]
    assert result["documents_metadata"]["doc1.pdf"]["async"] == "queue"
    assert result["status"] == "waiting"

