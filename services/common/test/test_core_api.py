### This code is property of the GGAO ###

import os
os.environ["INTEGRATION_NAME"] = "search"
os.environ["STORAGE_PERSIST_FOLDER"] = "test"
os.environ["STORAGE_DATA"] = "test"
import pytest
import json
from unittest.mock import patch, mock_open, MagicMock


# Mock for logger to prevent actual logging
mock_logger = MagicMock()


@pytest.fixture(autouse=True)
@patch("genai_sdk_services.storage.StorageController", autospec=True)
def mock_storage_controller(mock_storage):
    """Fixture to patch StorageController for all tests."""
    mock_storage_instance = MagicMock()
    mock_storage.return_value = mock_storage_instance
    yield mock_storage_instance

@patch("core_api.logger", mock_logger)
@patch("provider_resources.storage_put_file")
@patch("genai_sdk_services.storage.StorageController")
def test_generate_dataset( mock_storage, mock_storage_put_file):
    from core_api import _generate_dataset
    files = ["file1.jpg", "file2.jpg"]
    folder = "/test_folder"
    metadata = {"Key": "Value"}
    expected_path = f"{folder}/datasets/{hash(str(files))}.csv".replace("//", "/")
    
    result_path = _generate_dataset(files, folder, metadata)
    
    # Check if the file is created at the expected path
    assert result_path == expected_path

    # Verify storage_put_file is called correctly
    mock_storage_put_file.assert_called_once()
    args, kwargs = mock_storage_put_file.call_args
    assert args[0] == expected_path  # Verify path
    assert os.getenv('STORAGE_DATA') in args[2]  # Verify storage environment variable is used

@patch("genai_sdk_services.storage.StorageController")
def test_sync_preprocessing_request_generate(mock_storage):
    from core_api import _sync_preprocessing_request_generate
    request_params = {
        "force_ocr": True,
        "ocr": "test_ocr",
        "languages": ["en"],
        "tracking": {"id": 123},
    }
    request_file = "test_file.pdf"

    mock_template = {
        "filename": "",
        "force_ocr": False,
        "ocr_origin": "",
        "language": "",
        "tracking": {}
    }

    with patch("builtins.open", mock_open(read_data=json.dumps(mock_template))):
        result = _sync_preprocessing_request_generate(request_params, request_file)

    # Verify the result
    assert result["filename"] == request_file
    assert result["force_ocr"] == True
    assert result["ocr_origin"] == "test_ocr"
    assert result["language"] == "en"
    assert result["tracking"] == {"id": 123}

@patch("genai_sdk_services.storage.StorageController")
def test_sync_classification_multiclass_request_generate(mock_storage):
    from core_api import _sync_classification_multiclass_request_generate
    request_params = {
        "model_path": "path/to/model",
        "process_id": 456,
        "force_ocr": False,
        "ocr": "test_ocr",
        "top_classes": 3,
        "languages": ["es"],
        "tracking": {"session": "abc"}
    }
    request_file = "test_file.pdf"

    mock_template = {
        "model": "",
        "id_p": "",
        "filename": "",
        "force_ocr": False,
        "ocr_origin": "",
        "top_classes": 0,
        "language": "",
        "tracking": {}
    }

    with patch("builtins.open", mock_open(read_data=json.dumps(mock_template))):
        result = _sync_classification_multiclass_request_generate(request_params, request_file)

    # Verify the result
    assert result["model"] == "path/to/model"
    assert result["id_p"] == 456
    assert result["force_ocr"] == False
    assert result["ocr_origin"] == "test_ocr"
    assert result["top_classes"] == 3
    assert result["language"] == "es"
    assert result["tracking"] == {"session": "abc"}

# Mock template files content
mock_sync_extraction_template = {
    "id_type": "",
    "id_p": "",
    "_filename": "",
    "force_ocr": False,
    "ocr_origin": "",
    "fields": [],
    "params_extraction": {},
    "tracking": {}
}

mock_async_preprocess_template = {
    "timeout_sender": 0,
    "dataset_conf": {"dataset_path": "", "dataset_csv_path": ""},
    "url_sender": "",
    "force_ocr": False,
    "origins": {"ocr": ""},
    "integration": {},
    "tracking": {}
}

mock_async_classification_template = {
    "predict_conf": {"models": {"text": ""}, "top_classes": 0},
    "dataset_conf": {"dataset_csv_path": "", "dataset_path": "", "dataset_id": ""},
    "url_sender": "",
    "languages": [],
    "csv": False,
    "integration": {},
    "tracking": {},
    "origins": {"ocr": ""},
}


@patch("genai_sdk_services.storage.StorageController")
@patch("builtins.open", new_callable=mock_open, read_data=json.dumps(mock_sync_extraction_template))
def test_sync_extraction_request_generate(mock_file, mock_storage):
    from core_api import _sync_extraction_request_generate
    request_params = {
        "doc_type": "passport",
        "process_id": 12345,
        "force_ocr": True,
        "ocr": "ocr_engine",
        "fields_to_extract": ["name", "dob"],
        "params_for_extraction": {"option": "value"},
        "tracking": {"id": "tracking_id"}
    }
    request_file = "test_file.pdf"

    result = _sync_extraction_request_generate(request_params, request_file)

    assert result["id_type"] == "passport"
    assert result["id_p"] == 12345
    assert result["_filename"] == "test_file.pdf"
    assert result["force_ocr"] == True
    assert result["ocr_origin"] == "ocr_engine"
    assert result["fields"] == ["name", "dob"]
    assert result["params_extraction"] == {"option": "value"}
    assert result["tracking"] == {"id": "tracking_id"}


@patch("builtins.open", new_callable=mock_open, read_data=json.dumps(mock_async_preprocess_template))
@patch("core_api._generate_dataset")
@patch("provider_resources.queue_url", "mocked_queue_url")
@patch("genai_sdk_services.storage.StorageController")
def test_async_preprocess_request_generate(mock_storage, mock_generate_dataset, mock_file):
    from core_api import _async_preprocess_request_generate
    mock_generate_dataset.return_value = "mocked_dataset_path"

    request_params = {
        "timeout": 30,
        "folder": "/test_folder",
        "force_ocr": True,
        "ocr": "ocr_engine",
        "integration": {"key": "value"},
        "tracking": {"id": "tracking_id"}
    }
    request_files = ["file1.jpg", "file2.jpg"]

    result = _async_preprocess_request_generate(request_params, request_files)

    assert result["timeout_sender"] == 30
    assert result["dataset_conf"]["dataset_path"] == "/test_folder"
    assert result["dataset_conf"]["dataset_csv_path"] == "mocked_dataset_path"
    assert result["force_ocr"] == True
    assert result["origins"]["ocr"] == "ocr_engine"
    assert result["integration"] == {"key": "value"}
    assert result["tracking"] == {"id": "tracking_id"}
    assert result["url_sender"] == "mocked_queue_url"


@patch("builtins.open", new_callable=mock_open, read_data=json.dumps(mock_async_classification_template))
@patch("core_api._generate_dataset")
@patch("provider_resources.queue_url", "mocked_queue_url")
@patch("genai_sdk_services.storage.StorageController")
def test_async_classification_multiclass_request_generate(mock_storage, mock_generate_dataset, mock_file):
    from core_api import _async_classification_multiclass_request_generate
    mock_generate_dataset.return_value = "mocked_dataset_path"

    request_params = {
        "model_path": "model/path",
        "folder": "/test_folder",
        "force_ocr": True,
        "ocr": "ocr_engine",
        "languages": ["en"],
        "top_classes": 5,
        "process_id": "process:12345",
        "integration": {"key": "value"},
        "tracking": {"id": "tracking_id"}
    }
    request_files = ["file1.jpg"]

    result = _async_classification_multiclass_request_generate(request_params, request_files)

    assert result["predict_conf"]["models"]["text"] == "model/path"
    assert result["dataset_conf"]["dataset_csv_path"] == "mocked_dataset_path"
    assert result["dataset_conf"]["dataset_path"] == "/test_folder"
    assert result["dataset_conf"]["dataset_id"] == "12345"
    assert result["force_ocr"] == True
    assert result["origins"]["ocr"] == "ocr_engine"
    assert result["languages"] == ["en"]
    assert result["predict_conf"]["top_classes"] == 5
    assert result["integration"] == {"key": "value"}
    assert result["tracking"] == {"id": "tracking_id"}
    assert result["url_sender"] == "mocked_queue_url"


# Mock JSON templates
mock_async_classification_multilabel_template = {
    "dataset_conf": {"multilabel_conf": {"hierarchical_category_tree": ""}, "dataset_csv_path": "", "dataset_path": ""},
    "predict_conf": {"tree_conf": {"tree_path": "", "tree_id": ""}, "top_classes": 0},
    "languages": [],
    "force_ocr": False,
    "origins": {"ocr": ""},
    "integration": {},
    "tracking": {}
}

mock_async_extraction_template = {
    "extract_type": "",
    "dataset_conf": {"dataset_csv_path": "", "dataset_path": ""},
    "fields_to_extract": [],
    "params_extraction": {},
    "integration": {},
    "origins": {"ocr": ""},
    "tracking": {}
}

mock_async_indexing_template = {
    "dataset_conf": {"dataset_csv_path": "", "dataset_path": ""},
    "index_conf": {"metadata": {}, "models": {}},
    "preprocess_conf": {"layout_conf": {}},
    "languages": [],
    "timeout_sender": 0,
    "force_ocr": False,
    "origins": {"ocr": ""},
    "integration": {},
    "tracking": {}
}


@patch("builtins.open", new_callable=mock_open, read_data=json.dumps(mock_async_classification_multilabel_template))
@patch("core_api._generate_dataset")
@patch("provider_resources.queue_url", "mocked_queue_url")
def test_async_classification_multilabel_request_generate(mock_generate_dataset, mock_file):
    from core_api import _async_classification_multilabel_request_generate
    mock_generate_dataset.return_value = "mocked_dataset_path"

    request_params = {
        "tree_csv": "path/to/tree.csv",
        "model_path": "model/path",
        "folder": "/test_folder",
        "force_ocr": True,
        "ocr": "ocr_engine",
        "languages": ["en"],
        "top_classes": 5,
        "integration": {"key": "value"},
        "tracking": {"id": "tracking_id"}
    }
    request_files = ["file1.jpg"]

    result = _async_classification_multilabel_request_generate(request_params, request_files)

    assert result["dataset_conf"]["multilabel_conf"]["hierarchical_category_tree"] == "path/to/tree.csv"
    assert result["predict_conf"]["tree_conf"]["tree_path"] == "path/to/tree.csv"
    assert result["predict_conf"]["tree_conf"]["tree_id"] == "model/path"
    assert result["dataset_conf"]["dataset_csv_path"] == "mocked_dataset_path"
    assert result["force_ocr"] == True
    assert result["origins"]["ocr"] == "ocr_engine"
    assert result["languages"] == ["en"]
    assert result["predict_conf"]["top_classes"] == 5
    assert result["url_sender"] == "mocked_queue_url"


@patch("builtins.open", new_callable=mock_open, read_data=json.dumps(mock_async_extraction_template))
@patch("core_api._generate_dataset")
@patch("provider_resources.queue_url", "mocked_queue_url")
def test_async_extraction_request_generate(mock_generate_dataset, mock_file):
    from core_api import _async_extraction_request_generate
    mock_generate_dataset.return_value = "mocked_dataset_path"

    request_params = {
        "doc_type": "invoice",
        "folder": "/test_folder",
        "force_ocr": True,
        "ocr": "ocr_engine",
        "fields_to_extract": ["field1", "field2"],
        "params_for_extraction": {"key": "value"},
        "integration": {"key": "value"},
        "tracking": {"id": "tracking_id"}
    }
    request_files = ["file1.jpg"]

    result = _async_extraction_request_generate(request_params, request_files)

    assert result["extract_type"] == "invoice"
    assert result["dataset_conf"]["dataset_csv_path"] == "mocked_dataset_path"
    assert result["dataset_conf"]["dataset_path"] == "/test_folder"
    assert result["fields_to_extract"] == ["field1", "field2"]
    assert result["params_extraction"] == {"key": "value"}
    assert result["url_sender"] == "mocked_queue_url"


@patch("builtins.open", new_callable=mock_open, read_data=json.dumps(mock_async_indexing_template))
@patch("core_api._generate_dataset")
@patch("provider_resources.queue_url", "mocked_queue_url")
def test_async_indexing_request_generate(mock_generate_dataset, mock_file):
    from core_api import _async_indexing_request_generate
    mock_generate_dataset.return_value = "mocked_dataset_path"

    request_params = {
        "folder": "/test_folder",
        "index_conf": {"metadata": {"meta1": "value1"}, "models": {"model1": "path/to/model"}},
        "layout_conf": {"layout": "value"},
        "force_ocr": True,
        "ocr": "ocr_engine",
        "languages": ["en"],
        "timeout": 120,
        "tracking": {"id": "tracking_id"},
        "integration": {"key": "value"}
    }
    request_files = ["file1.jpg"]

    result = _async_indexing_request_generate(request_params, request_files)

    assert result["dataset_conf"]["dataset_csv_path"] == "mocked_dataset_path"
    assert result["dataset_conf"]["dataset_path"] == "/test_folder"
    assert result["index_conf"]["metadata"] == {"meta1": "value1"}
    assert result["index_conf"]["models"] == {"model1": "path/to/model"}
    assert result["preprocess_conf"]["layout_conf"] == {"layout": "value"}
    assert result["languages"] == ["en"]
    assert result["timeout_sender"] == 120
    assert result["url_sender"] == "mocked_queue_url"


@pytest.fixture
def mock_env(monkeypatch):
    """Fixture to mock environment variables."""
    monkeypatch.setenv("API_SYNC_PREPROCESS_URL", "sync-preprocess.example.com")
    monkeypatch.setenv("API_SYNC_CLASSIFY_URL", "sync-classify.example.com")
    monkeypatch.setenv("API_SYNC_EXTRACT_URL", "sync-extract.example.com")
    monkeypatch.setenv("API_ASYNC_PROCESS_URL", "async-process.example.com")
    monkeypatch.setenv("API_QUEUE_PROCESS_URL", "queue-process.example.com")
    monkeypatch.setenv("API_ASYNC_STATUS_URL", "async-status.example.com")
    monkeypatch.setenv("API_ASYNC_RESULT_URL", "async-result.example.com")
    monkeypatch.setenv("API_ASYNC_DELETE_URL", "async-delete.example.com")
    monkeypatch.setenv("API_SYNC_DELETE_URL", "sync-delete.example.com")
    monkeypatch.setenv("API_QUEUE_DELETE_URL", "queue-delete.example.com")


@patch("requests.post")
def test_sync_preprocess_request(mock_post, mock_env):
    from core_api import _sync_preprocess_request

    mock_post.return_value = MagicMock(status_code=200, json=lambda: {"id_p": "mock_process_id"})

    apigw_params = {"Authorization": "Bearer token"}
    request_params = {"param1": "value1"}
    request_file = "test_file.pdf"

    result = _sync_preprocess_request(apigw_params, request_params, request_file)

    assert result["status"] == "ok"
    assert result["process_id"] == "mock_process_id"


@patch("requests.post")
def test_sync_preprocess_request_error(mock_post, mock_env):
    from core_api import _sync_preprocess_request

    mock_post.return_value = MagicMock(status_code=400, json=lambda: {"id_p": "mock_process_id"})

    apigw_params = {"Authorization": "Bearer token"}
    request_params = {"param1": "value1"}
    request_file = "test_file.pdf"

    result = _sync_preprocess_request(apigw_params, request_params, request_file)

    assert result["status"] == "error"


@patch("requests.post")
def test_sync_classification_multiclass_request(mock_post, mock_env):
    from core_api import sync_classification_multiclass_request
    mock_post.side_effect = [
        MagicMock(status_code=200, json=lambda: {"id_p": "mock_preprocess_id"}),  # Preprocess response
        MagicMock(status_code=200, json=lambda: {"id_p": "mock_classify_id", "result": "mock_result"}),  # Classification response
    ]

    apigw_params = {"Authorization": "Bearer token"}
    request_params = {"param1": "value1"}
    request_file = "test_file.pdf"

    with patch("core_api._sync_classification_multiclass_request_generate"):
        result = sync_classification_multiclass_request(apigw_params, request_params, request_file)

    assert result["status"] == "finish"
    assert result["process_id"] == "mock_classify_id"
    assert result["result"] == "mock_result"
    assert len(mock_post.call_args_list) == 2


@patch("requests.post")
def test_sync_classification_multiclass_request_error(mock_post, mock_env):
    from core_api import sync_classification_multiclass_request
    mock_post.side_effect = [
        MagicMock(status_code=400, json=lambda: {"id_p": "mock_preprocess_id"}),  # Preprocess response
        MagicMock(status_code=400, json=lambda: {"id_p": "mock_classify_id", "result": "mock_result"}),  # Classification response
    ]

    apigw_params = {"Authorization": "Bearer token"}
    request_params = {"param1": "value1"}
    request_file = "test_file.pdf"

    with patch("core_api._sync_classification_multiclass_request_generate"):
        result = sync_classification_multiclass_request(apigw_params, request_params, request_file)

    assert result["status"] == "error"


@patch("requests.post")
def test_sync_extraction_request(mock_post, mock_env):
    from core_api import sync_extraction_request
    mock_post.side_effect = [
        MagicMock(status_code=200, json=lambda: {"id_p": "mock_preprocess_id"}),  # Preprocess response
        MagicMock(status_code=200, json=lambda: {"result": "mock_extraction_result"}),  # Extraction response
    ]

    apigw_params = {"Authorization": "Bearer token"}
    request_params = {"param1": "value1"}
    request_file = "test_file.pdf"

    with patch("core_api._sync_extraction_request_generate"):
        result = sync_extraction_request(apigw_params, request_params, request_file)

    assert result["status"] == "finish"
    assert result["result"] == "mock_extraction_result"
    assert len(mock_post.call_args_list) == 2

@patch("requests.post")
def test_sync_extraction_request_error(mock_post, mock_env):
    from core_api import sync_extraction_request
    mock_post.side_effect = [
        MagicMock(status_code=400, json=lambda: {"id_p": "mock_preprocess_id"}),  # Preprocess response
        MagicMock(status_code=400, json=lambda: {"result": "mock_extraction_result"}),  # Extraction response
    ]

    apigw_params = {"Authorization": "Bearer token"}
    request_params = {"param1": "value1"}
    request_file = "test_file.pdf"

    with patch("core_api._sync_extraction_request_generate"):
        result = sync_extraction_request(apigw_params, request_params, request_file)

    assert result["status"] == "error"

@patch("requests.post")
def test_async_preprocess_request(mock_post, mock_env):
    from core_api import async_preprocess_request
    mock_post.return_value = MagicMock(status_code=200, json=lambda: {"dataset_status_key": "mock_async_id"})

    apigw_params = {"Authorization": "Bearer token"}
    request_params = {"param1": "value1"}
    request_files = ["file1.pdf", "file2.pdf"]

    with patch("core_api._async_preprocess_request_generate"):
        result = async_preprocess_request(apigw_params, request_params, request_files)

    assert result["status"] == "waiting"
    assert result["process_id"] == "mock_async_id"

@patch("requests.post")
def test_async_preprocess_request_error(mock_post, mock_env):
    from core_api import async_preprocess_request
    mock_post.return_value = MagicMock(status_code=400, json=lambda: {})

    apigw_params = {"Authorization": "Bearer token"}
    request_params = {"param1": "value1"}
    request_files = ["file1.pdf", "file2.pdf"]

    with patch("core_api._async_preprocess_request_generate"):
        result = async_preprocess_request(apigw_params, request_params, request_files)

    assert result["status"] == "error"

@patch("requests.post")
def test_async_classification_multiclass_request(mock_post, mock_env):
    from core_api import async_classification_multiclass_request
    mock_post.return_value = MagicMock(status_code=200, json=lambda: {"dataset_status_key": "mock_async_id"})

    apigw_params = {"Authorization": "Bearer token"}
    request_params = {"param1": "value1"}
    request_files = ["file1.pdf", "file2.pdf"]

    with patch("core_api._async_classification_multiclass_request_generate"):
        result = async_classification_multiclass_request(apigw_params, request_params, request_files)

    assert result["status"] == "waiting"
    assert result["process_id"] == "mock_async_id"
    mock_post.assert_called_once()

@patch("requests.post")
def test_async_classification_multiclass_request_error(mock_post, mock_env):
    from core_api import async_classification_multiclass_request
    mock_post.return_value = MagicMock(status_code=400, json=lambda: {})

    apigw_params = {"Authorization": "Bearer token"}
    request_params = {"param1": "value1"}
    request_files = ["file1.pdf", "file2.pdf"]

    with patch("core_api._async_classification_multiclass_request_generate"):
        result = async_classification_multiclass_request(apigw_params, request_params, request_files)

    assert result["status"] == "error"

@patch("requests.post")
def test_async_classification_multilabel_request(mock_post, mock_env):
    from core_api import async_classification_multilabel_request
    mock_post.return_value = MagicMock(status_code=200, json=lambda: {"dataset_status_key": "mock_async_id"})

    apigw_params = {"Authorization": "Bearer token"}
    request_params = {"param1": "value1"}
    request_files = ["file1.pdf", "file2.pdf"]

    with patch("core_api._async_classification_multilabel_request_generate"):
        result = async_classification_multilabel_request(apigw_params, request_params, request_files)

    assert result["status"] == "waiting"
    assert result["process_id"] == "mock_async_id"
    mock_post.assert_called_once()

@patch("requests.post")
def test_async_classification_multilabel_request_error(mock_post, mock_env):
    from core_api import async_classification_multilabel_request
    mock_post.return_value = MagicMock(status_code=400, json=lambda: {})

    apigw_params = {"Authorization": "Bearer token"}
    request_params = {"param1": "value1"}
    request_files = ["file1.pdf", "file2.pdf"]

    with patch("core_api._async_classification_multilabel_request_generate"):
        result = async_classification_multilabel_request(apigw_params, request_params, request_files)

    assert result["status"] == "error"

@patch("requests.post")
def test_async_extraction_request(mock_post, mock_env):
    from core_api import async_extraction_request
    mock_post.return_value = MagicMock(status_code=200, json=lambda: {"dataset_status_key": "mock_async_id"})

    apigw_params = {"Authorization": "Bearer token"}
    request_params = {"param1": "value1"}
    request_files = ["file1.pdf", "file2.pdf"]

    with patch("core_api._async_extraction_request_generate"):
        result = async_extraction_request(apigw_params, request_params, request_files)

    assert result["status"] == "waiting"
    assert result["process_id"] == "mock_async_id"
    mock_post.assert_called_once()

@patch("requests.post")
def test_async_extraction_request_error(mock_post, mock_env):
    from core_api import async_extraction_request
    mock_post.return_value = MagicMock(status_code=400, json=lambda: {})

    apigw_params = {"Authorization": "Bearer token"}
    request_params = {"param1": "value1"}
    request_files = ["file1.pdf", "file2.pdf"]

    with patch("core_api._async_extraction_request_generate"):
        result = async_extraction_request(apigw_params, request_params, request_files)

    assert result["status"] == "error"

@patch("requests.post")
def test_async_indexing_request(mock_post, mock_env):
    from core_api import async_indexing_request
    mock_post.return_value = MagicMock(status_code=200, json=lambda: {"dataset_status_key": "mock_async_id"})

    apigw_params = {"Authorization": "Bearer token"}
    request_params = {"param1": "value1"}
    request_files = ["file1.pdf", "file2.pdf"]

    with patch("core_api._async_indexing_request_generate"):
        result = async_indexing_request(apigw_params, request_params, request_files)

    assert result["status"] == "waiting"
    assert result["process_id"] == "mock_async_id"
    mock_post.assert_called_once()

@patch("requests.post")
def test_async_indexing_request_error(mock_post, mock_env):
    from core_api import async_indexing_request
    mock_post.return_value = MagicMock(status_code=400, json=lambda: {})

    apigw_params = {"Authorization": "Bearer token"}
    request_params = {"param1": "value1"}
    request_files = ["file1.pdf", "file2.pdf"]

    with patch("core_api._async_indexing_request_generate"):
        result = async_indexing_request(apigw_params, request_params, request_files)

    assert result["status"] == "error"


@patch("provider_resources.queue_write_message")
@patch("provider_resources.qc.set_credentials")
def test_queue_indexing_request(mock_set_credentials, mock_queue_write_message, mock_env):
    from core_api import queue_indexing_request
    mock_queue_write_message.return_value = True

    apigw_params = {"Authorization": "Bearer token"}
    request_params = {"param1": "value1"}
    request_files = ["file1.pdf", "file2.pdf"]

    with patch("core_api._async_indexing_request_generate"):
        result = queue_indexing_request(apigw_params, request_params, request_files)

    assert result["status"] == "waiting"
    assert "process_id" in result
    mock_set_credentials.assert_called_once()
    mock_queue_write_message.assert_called_once()


@patch("provider_resources.queue_write_message")
@patch("provider_resources.qc.set_credentials")
def test_queue_indexing_request_error(mock_set_credentials, mock_queue_write_message, mock_env):
    from core_api import queue_indexing_request
    mock_queue_write_message.return_value = False

    apigw_params = {"Authorization": "Bearer token"}
    request_params = {"param1": "value1"}
    request_files = ["file1.pdf", "file2.pdf"]

    with patch("core_api._async_indexing_request_generate"):
        result = queue_indexing_request(apigw_params, request_params, request_files)

    assert result["status"] == "error"
    mock_set_credentials.assert_called_once()
    mock_queue_write_message.assert_called_once()


@patch("requests.post")
def test_async_status_request(mock_post, mock_env):
    from core_api import async_status_request
    mock_post.return_value = MagicMock(status_code=200, json=lambda: {"process_status": "processing"})

    apigw_params = {"Authorization": "Bearer token"}
    process_id = "test_process_id"

    result = async_status_request(apigw_params, process_id)

    assert result["status"] == "waiting"
    assert result["process_id"] == process_id
    mock_post.assert_called_once()


@patch("requests.post")
def test_async_result_request(mock_post, mock_env):
    from core_api import async_result_request
    mock_post.return_value = MagicMock(status_code=200, json=lambda: {"info": {"result_key": "value"}})

    apigw_params = {"Authorization": "Bearer token"}
    process_id = "test_process_id"

    result = async_result_request(apigw_params, process_id)

    assert result["status"] == "finish"
    assert result["process_id"] == process_id
    assert "results" in result
    mock_post.assert_called_once()


@patch("requests.post")
def test_async_delete_request(mock_post, mock_env):
    from core_api import async_delete_request
    mock_post.return_value = MagicMock(status_code=200, json=lambda: {"status": "ok"})

    apigw_params = {"Authorization": "Bearer token"}
    process_id = "test_process_id"
    tracking_message = {"tracking_key": "value"}

    result = async_delete_request(apigw_params, process_id, tracking_message)

    assert result is True
    mock_post.assert_called_once()

@patch("requests.post")
def test_async_delete_request_error(mock_post, mock_env):
    from core_api import async_delete_request
    mock_post.return_value = MagicMock(status_code=400, json=lambda: {"status": "ok"})

    apigw_params = {"Authorization": "Bearer token"}
    process_id = "test_process_id"
    tracking_message = {"tracking_key": "value"}

    async_delete_request(apigw_params, process_id, tracking_message)



@patch("requests.post")
def test_sync_delete_request(mock_post, mock_env):
    from core_api import sync_delete_request
    mock_post.return_value = MagicMock(status_code=200, json=lambda: {"status": "ok"})

    apigw_params = {"Authorization": "Bearer token"}
    process_id = "test_process_id"
    tracking_message = {"tracking_key": "value"}

    result = sync_delete_request(apigw_params, process_id, tracking_message)

    assert result is True
    mock_post.assert_called_once()


@patch("provider_resources.queue_write_message")
@patch("provider_resources.qc.set_credentials")
def test_queue_delete_request(mock_set_credentials, mock_queue_write_message, mock_env):
    from core_api import queue_delete_request
    mock_queue_write_message.return_value = True

    apigw_params = {"Authorization": "Bearer token"}
    process_id = "test_process_id"
    tracking_message = {"tracking_key": "value"}

    result = queue_delete_request(apigw_params, process_id, tracking_message)

    assert result is True
    mock_set_credentials.assert_called_once()
    mock_queue_write_message.assert_called_once()


@patch("requests.post")
def test_async_status_request_error(mock_post, mock_env):
    from core_api import async_status_request
    mock_post.return_value = MagicMock(status_code=200, json=lambda: {'process_status': ''})

    apigw_params = {"Authorization": "Bearer token"}
    process_id = "test_process_id"

    result = async_status_request(apigw_params, process_id)

    assert result["status"] == "error"


@patch("requests.post")
def test_async_result_request_error(mock_post, mock_env):
    from core_api import async_result_request
    mock_post.return_value = MagicMock(status_code=200, json=lambda: {"info": "error"})

    apigw_params = {"Authorization": "Bearer token"}
    process_id = "test_process_id"

    with pytest.raises(Exception, match="Unable to get results from API"):
        async_result_request(apigw_params, process_id)


@patch("provider_resources.queue_write_message")
def test_queue_delete_request_error(mock_queue_write_message, mock_env):
    from core_api import queue_delete_request
    mock_queue_write_message.return_value = False

    apigw_params = {"Authorization": "Bearer token"}
    process_id = "test_process_id"
    tracking_message = {"tracking_key": "value"}

    result = queue_delete_request(apigw_params, process_id, tracking_message)

    assert result is False
