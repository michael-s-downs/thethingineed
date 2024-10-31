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
        @mock.patch('main.provider_resources.storage_download_folder')
        def mocked(mock_storage_download_folder):
            # Set mocks return values
            mock_storage_download_folder.return_value = True

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


# TODO: fix and mock currents, add for validation and adaptation
class TestCall():
    def test_process_ok(self, client):
        """Test the process endpoint."""
        with patch("main.provider_resources.queue_write_message") as mock_queue_write_message:
            with patch("main.provider_resources.storage_put_file") as mock_storage_put_file:
                mock_queue_write_message.return_value = True  # MOCK NOT WORKING!
                mock_storage_put_file.return_value = True  # MOCK NOT WORKING!

                message = {}
                headers = {'x-tenant': 'tenant', 'x-department': 'department', 'x-reporting': 'report'}
                response = client.post('/process', json=message, headers=headers)

                assert response.status_code == 200
                assert json.loads(response.data).get('status', "") == "processing"

    def test_process_error_queue(self, client):
        """Test the process endpoint."""
        with patch("main.provider_resources.queue_write_message") as mock_queue_write_message:
            with patch("main.provider_resources.storage_put_file") as mock_storage_put_file:
                mock_queue_write_message.return_value = False  # MOCK NOT WORKING!
                mock_storage_put_file.return_value = True  # MOCK NOT WORKING!

                message = {}
                headers = {'x-tenant': 'tenant', 'x-department': 'department', 'x-reporting': 'report'}
                response = client.post('/process', json=message, headers=headers)

                assert response.status_code == 200
                assert json.loads(response.data).get('status', "") == "error"

    def test_process_error_storage(self, client):
        """Test the process endpoint."""
        with patch("main.provider_resources.queue_write_message") as mock_queue_write_message:
            with patch("main.provider_resources.storage_put_file") as mock_storage_put_file:
                mock_queue_write_message.return_value = True  # MOCK NOT WORKING!
                mock_storage_put_file.return_value = False  # MOCK NOT WORKING!

                message = {}
                headers = {'x-tenant': 'tenant', 'x-department': 'department', 'x-reporting': 'report'}
                response = client.post('/process', json=message, headers=headers)

                assert response.status_code == 200
                assert json.loads(response.data).get('status', "") == "error"

    def test_process_error_unknown(self, client):
        """Test the process endpoint."""
        with patch("main.provider_resources.queue_write_message") as mock_queue_write_message:
            with patch("main.provider_resources.storage_put_file") as mock_storage_put_file:
                with patch('main.receive_request', side_effect=Exception):  # MOCK NOT WORKING!
                    mock_queue_write_message.return_value = True  # MOCK NOT WORKING!
                    mock_storage_put_file.return_value = True  # MOCK NOT WORKING!

                    message = {}
                    headers = {'x-tenant': 'tenant', 'x-department': 'department', 'x-reporting': 'report'}
                    response = client.post('/process', json=message, headers=headers)

                    assert json.loads(response.data).get('status', "") == "error"
