### This code is property of the GGAO ###


# Native imports
import os
import sys
import json
from unittest import mock
from functools import wraps

# Installed imports
import pytest

# Custom imports
sys.path.append(os.getenv('LOCAL_COMMON_PATH'))
from main import app


@pytest.fixture
def client():
    with app.test_client() as client:
        yield client


# Decorator to reuse mocks
def mock_resources(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        @mock.patch('main.provider_resources.queue_write_message')
        @mock.patch('main.provider_resources.storage_download_folder')
        @mock.patch('main.provider_resources.storage_upload_file')
        @mock.patch('main.provider_resources.storage_put_file')
        @mock.patch('main.provider_resources.storage_list_folder')
        def mocked(mock_storage_list_folder, mock_storage_put_file, mock_storage_upload_file, mock_storage_download_folder, mock_queue_write_message):
            # Set mocks return values
            mock_storage_list_folder.return_value = []
            mock_storage_put_file.return_value = True
            mock_storage_upload_file.return_value = True
            mock_storage_download_folder.return_value = True
            mock_queue_write_message.return_value = True

            # Call function with mocks applied
            return func(*args, **kwargs)

        return mocked()

    return wrapper


@mock_resources
def test_healthcheck(client):
    """Test the healthcheck endpoint."""
    response = client.get("/healthcheck")

    assert response.status_code == 200
    assert json.loads(response.data).get('status', "") == "ok"


@mock_resources
def test_killcheck(client):
    """Test the killcheck endpoint."""
    response = client.get("/killcheck")

    assert response.status_code == 200
    assert json.loads(response.data).get('status', "") == "ok"


@mock_resources
def test_reloadconfig(client):
    """Test the reloadconfig endpoint."""
    response = client.get("/reloadconfig")

    assert response.status_code == 200
    assert json.loads(response.data).get('status', "") == "ok"


class TestCall:
    headers = {'x-tenant': "tenant", 'x-department': "department", 'x-reporting': "report"}
    message = {'operation': "indexing", 'index': "test_index", 'documents_metadata': {'doc1': {"content_binary": "dGVzdA=="}}}

    @mock_resources
    def test_process_ok(self, client):
        """Test the process endpoint."""
        response = client.post('/process', headers=self.headers, json=self.message)

        assert response.status_code == 200
        assert json.loads(response.data).get('status', "") == "processing"

    @mock_resources
    def test_process_invalid_input(self, client):
        """Test the process endpoint."""
        message = {}  # Force bad input

        response = client.post('/process', headers=self.headers, json=message)

        assert response.status_code == 200
        assert json.loads(response.data).get('status', "") == "error"
        assert json.loads(response.data).get('error', "") == "Bad input: No JSON received or invalid format (send it as data with application/json content)"

    @mock_resources
    def test_process_error_queue(self, client):
        """Test the process endpoint."""
        with mock.patch("main.provider_resources.queue_write_message") as mock_queue_write_message:
            mock_queue_write_message.return_value = False  # Force queue error

            response = client.post('/process', headers=self.headers, json=self.message)

            assert response.status_code == 200
            assert json.loads(response.data).get('status', "") == "error"
            assert json.loads(response.data).get('error', "") == "Internal error"

    @mock_resources
    def test_process_error_unknown(self, client):
        """Test the process endpoint."""
        with mock.patch("main.provider_resources.storage_put_file") as mock_storage_put_file:
            mock_storage_put_file.side_effect = Exception  # Force unknown error

            response = client.post('/process', headers=self.headers, json=self.message)

            assert response.status_code == 200
            assert json.loads(response.data).get('status', "") == "error"
            assert json.loads(response.data).get('error', "") == "Internal error"
