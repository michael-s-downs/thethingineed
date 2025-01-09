### This code is property of the GGAO ###

import os
os.environ["INTEGRATION_NAME"] = "search"
os.environ["STORAGE_PERSIST_FOLDER"] = "test"
import pytest
from unittest.mock import patch, MagicMock

from provider_resources import (
    storage_normalize_path,
    storage_validate_container,
    storage_validate_folder,
    storage_list_folder,
    storage_put_file,
    storage_upload_file,
    storage_get_file,
    storage_download_folder,
    storage_remove_files,
    queue_write_message,
    queue_read_messages,
    queue_delete_message
)

@pytest.fixture
def mock_storage_controller():
    with patch("provider_resources.sc") as mock:
        yield mock

@pytest.fixture
def mock_queue_controller():
    with patch("provider_resources.qc") as mock:
        yield mock

def test_storage_normalize_path():
    assert storage_normalize_path("/example/path") == "/example/path/"
    assert storage_normalize_path("/example/path//") == "/example/path//"
    assert storage_normalize_path("/") == ""

def test_storage_validate_container(mock_storage_controller):
    mock_storage_controller.validate_container.return_value = True
    assert storage_validate_container("container")

    mock_storage_controller.validate_container.side_effect = Exception
    assert storage_validate_container("container")

def test_storage_validate_folder(mock_storage_controller):
    mock_storage_controller.list_files.return_value = ["file1", "file2"]
    assert storage_validate_folder("/path", "container")

    mock_storage_controller.list_files.side_effect = Exception
    assert not storage_validate_folder("/path", "container")

def test_storage_list_folder(mock_storage_controller):
    mock_storage_controller.list_files.return_value = ["file1.txt", "file2.txt"]
    files = storage_list_folder("/path", "container", extensions_include=["txt"])
    assert files == ["file1.txt", "file2.txt"]

    files = storage_list_folder("/path", "container", extensions_exclude=["txt"], files_exclude=["file1.txt"])
    assert files == []

    mock_storage_controller.list_files.side_effect = Exception
    assert storage_list_folder("/path", "container") == []

def test_storage_put_file(mock_storage_controller):
    mock_storage_controller.upload_object.return_value = None
    assert storage_put_file("/path/file", b"data", "container")

    mock_storage_controller.upload_object.side_effect = Exception
    assert not storage_put_file("/path/file", b"data", "container")

def test_storage_upload_file(mock_storage_controller):
    mock_storage_controller.upload_file.return_value = True
    assert storage_upload_file("local/file", "remote/path", "container")

    mock_storage_controller.upload_file.return_value = False
    assert not storage_upload_file("local/file", "remote/path", "container")

    mock_storage_controller.upload_file.side_effect = Exception
    assert not storage_upload_file("local/file", "remote/path", "container")

def test_storage_get_file(mock_storage_controller):
    mock_storage_controller.load_file.return_value = b"data"
    assert storage_get_file("/path/file", "container") == b"data"

    mock_storage_controller.load_file.side_effect = Exception
    assert storage_get_file("/path/file", "container") is None

def test_storage_download_folder(mock_storage_controller):
    mock_storage_controller.list_files.return_value = ["/remote_path/file1", "/remote_path/file2"]
    mock_storage_controller.load_file.return_value = b"data"

    with patch("os.makedirs") as mock_makedirs, patch("builtins.open", new_callable=MagicMock):
        assert storage_download_folder("/local_path", "/remote_path", "container")

    mock_storage_controller.list_files.side_effect = Exception
    with patch("provider_resources.storage_normalize_path") as mock_norm:
        mock_norm.side_effect = Exception
        assert not storage_download_folder("/local_path", "/remote_path", "container")

def test_storage_remove_files(mock_storage_controller):
    mock_storage_controller.list_files.return_value = ["file1", "file2"]
    assert storage_remove_files("/path", "container")

    mock_storage_controller.list_files.side_effect = Exception
    assert not storage_remove_files("/path", "container")

def test_queue_write_message(mock_queue_controller):
    mock_queue_controller.write.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}
    assert queue_write_message({"key": "value"})

    mock_queue_controller.write.return_value = {"ResponseMetadata": {"HTTPStatusCode": 500}}
    assert not queue_write_message({"key": "value"})

    mock_queue_controller.write.side_effect = Exception
    assert not queue_write_message({"key": "value"})

def test_queue_read_messages(mock_queue_controller):
    mock_queue_controller.read.return_value = (["message1"], ["metadata1"])
    messages, metadata = queue_read_messages()
    assert messages == ["message1"]
    assert metadata == ["metadata1"]

    mock_queue_controller.read.side_effect = Exception
    messages, metadata = queue_read_messages()
    assert messages == []
    assert metadata == []

def test_queue_delete_message(mock_queue_controller):
    mock_queue_controller.delete_messages.return_value = True
    assert queue_delete_message(["metadata1"])

    mock_queue_controller.delete_messages.return_value = False
    assert not queue_delete_message(["metadata1"])

    mock_queue_controller.delete_messages.side_effect = Exception
    assert not queue_delete_message(["metadata1"])
