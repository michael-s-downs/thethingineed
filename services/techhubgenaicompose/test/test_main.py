### This code is property of the GGAO ###

import os
os.environ['URL_LLM'] = "test_url"
os.environ['URL_RETRIEVE'] = "test_retrieve"
import pytest
import json
from main import app, ComposeDeployment  
from common.errors.genaierrors import PrintableGenaiError
from unittest.mock import patch, MagicMock

@pytest.fixture
def client():
    with app.test_client() as client:
        yield client

@pytest.fixture
def mock_deployment(mocker):
    """Mock the deployment to avoid actual calls."""
    mock = mocker.patch.object(ComposeDeployment, 'sync_deployment', return_value=({}, 200))
    yield mock

def test_healthcheck(client):
    """Test the healthcheck endpoint."""
    response = client.get('/healthcheck')
    assert response.status_code == 200
    assert response.json == {"status": "Service available"}

def test_must_continue():
    """Test the must_continue endpoint."""
    composed = ComposeDeployment()    
    assert composed.must_continue is False

def test_max_num_queue():
    """Test the max_num_queue endpoint."""
    composed = ComposeDeployment()    
    assert composed.max_num_queue == 1


def test_sync_deployment_key_error(client, mock_deployment):
    """Test deployment with missing key."""
    headers = {
        'x-tenant': 'tenant',
        'x-department': 'department',
        'x-limits': json.dumps({}),
        'user-token': 'user-token'
    }
    # with pytest.raises(Exception) as excinfo:
    client.post('/process', json={"generic": {}}, headers=headers)
    # assert "HTTP_X_REPORTING" in str(excinfo.value)


@patch("main.update_status")
def test_load_session_success(mock_update_status, client, mock_deployment):
    """Test loading session successfully."""
    headers = {
        'x-tenant': 'tenant',
        'x-department': 'department',
        'x-reporting': 'report'
    }
    response = client.post('/load_session', json={"session_id": "123", "conv": []}, headers=headers)
    assert response.status_code == 200

def test_load_session_key_error(client):
    """Test loading session with missing project configuration."""
    headers = {
        'x-tenant': 'tenant',
        'x-department': 'department',
        'x-reporting': 'report'
    }
    response = client.post('/load_session', json={}, headers=headers)
    assert response.status_code == 500

def test_load_session_exception(client):
    """Test loading session with missing project configuration."""
    composed = ComposeDeployment()    
    response = composed.load_session_redis(None)
    assert response[1] == 500

def test_load_session_key_error2(client):
    """Test loading session with missing project configuration."""
    composed = ComposeDeployment()    
    response = composed.load_session_redis({"project_conf": {}})
    assert response[1] == 404

def test_upload_template_success(client):
    """Test successful template upload with mocked upload_object."""
    headers = {
        'x-tenant': 'tenant',
        'x-department': 'department',
        'x-reporting': 'report',
        'user-token': 'some_token'
    }

    # Mocking the upload_object function
    with patch('main.upload_object') as mock_upload:
        mock_upload.return_value = True
        response = client.post('/upload_template', json={
            "name": "test_template",
            "content": {"key": "value"},
            "project_conf": {
                "x-reporting": "report",
                "x-department": "department",
                "x-tenant": "tenant"
            }
        }, headers=headers)

        assert response.status_code == 200

def test_upload_template_key_error(client):
    """Test uploading template with missing key."""
    headers = {
        'x-tenant': 'tenant',
        'x-department': 'department',
        'x-reporting': 'report'
    }
    with patch('main.upload_object') as mock_upload:
        mock_upload.return_value = True
        response = client.post('/upload_template', json={}, headers=headers)
        assert response.status_code == 404

def test_upload_template_exception(client, mocker):
    """Test uploading template with missing key."""
    composed = ComposeDeployment()    
    response = composed.upload_template(None)
    assert response[1] == 500

def test_upload_template_exception_uploading(client):
    """Test successful template upload with mocked upload_object."""
    headers = {
        'x-tenant': 'tenant',
        'x-department': 'department',
        'x-reporting': 'report',
        'user-token': 'some_token'
    }

    # Mocking the upload_object function
    with patch('main.upload_object') as mock_upload:
        mock_upload.side_effect = Exception
        response = client.post('/upload_template', json={
            "name": "test_template",
            "content": {"key": "value"},
            "project_conf": {
                "x-reporting": "report",
                "x-department": "department",
                "x-tenant": "tenant"
            }
        }, headers=headers)
        assert response.status_code == 500
    
def test_delete_template_success(client):
    """Test successful template deletion."""
    headers = {
        'x-tenant': 'tenant',
        'x-department': 'department',
        'x-reporting': 'report'
    }
    with patch('main.delete_file') as mock_delete:
        mock_delete.return_value = True
        response = client.post('/delete_template', json={
            "name": "test_template",
            "project_conf": {
                "x-reporting": "report",
                "x-department": "department",
                "x-tenant": "tenant"
            }
        }, headers=headers)
        
        assert response.status_code == 200

def test_delete_template_key_error(client):
    """Test deleting template with missing key."""
    headers = {
        'x-tenant': 'tenant',
        'x-department': 'department',
        'x-reporting': 'report'
    }
    response = client.post('/delete_template', json={}, headers=headers)
    assert response.status_code == 404


def test_delete_template_exception(client, mocker):
    """Test deleting template with missing key."""
    composed = ComposeDeployment()    
    response = composed.delete_template(None)
    assert response[1] == 500

def test_delete_template_exception_uploading(client):
    headers = {
        'x-tenant': 'tenant',
        'x-department': 'department',
        'x-reporting': 'report'
    }
    with patch('main.delete_file') as mock_delete:
        mock_delete.side_effect = Exception
        response = client.post('/delete_template', json={
            "name": "test_template",
            "project_conf": {
                "x-reporting": "report",
                "x-department": "department",
                "x-tenant": "tenant"
            }
        }, headers=headers)
        assert response.status_code == 500

def test_upload_filter_template(client):
    """Test upload filter template success."""
    headers = {
        'x-tenant': 'tenant',
        'x-department': 'department',
        'x-reporting': 'report'
    }
    with patch('main.upload_object') as mock_upload:
        mock_upload.return_value = True
        response = client.post('/upload_filter_template', json={
            "name": "filter_template",
            "content": {"filter_key": "filter_value"},
            "project_conf": {
                "x-reporting": "report",
                "x-department": "department",
                "x-tenant": "tenant"
            }
        }, headers=headers)
        
        assert response.status_code == 200

def test_delete_filter_template(client):
    """Test delete filter template success."""
    headers = {
        'x-tenant': 'tenant',
        'x-department': 'department',
        'x-reporting': 'report'
    }
    with patch('main.delete_file') as mock_delete:
        mock_delete.return_value = True
        response = client.post('/delete_filter_template', json={
            "name": "filter_template",
            "project_conf": {
                "x-reporting": "report",
                "x-department": "department",
                "x-tenant": "tenant"
            }
        }, headers=headers)

        assert response.status_code == 200


def test_process_exception(client):
    """Test loading session with missing project configuration."""
    composed = ComposeDeployment()    
    with patch('main.get_generic', side_effect=Exception):
        with pytest.raises(PrintableGenaiError) as excinfo:
            composed.process({})
        assert "Error" in str(excinfo.value)