### This code is property of the GGAO ###

import pytest
from unittest.mock import MagicMock, patch
import os
os.environ["INTEGRATION_NAME"] = "search"
os.environ["STORAGE_PERSIST_FOLDER"] = "test"
import json
from datetime import datetime, timedelta
from copy import deepcopy
from requests_manager import (
    generate_request,
    persist_request,
    restore_requests,
    delete_request,
    check_timeout,
    update_request,
    current_requests,
    current_processes_map,
)

@pytest.fixture
def setup_env_vars():
    """Set up environment variables for testing."""
    os.environ['STORAGE_DELETE_REQUEST'] = "True"
    os.environ['STORAGE_PERSIST_REQUEST'] = "True"
    os.environ['STORAGE_BACKEND'] = "mock_storage"
    os.environ['STORAGE_PERSIST_FOLDER'] = "mock_pointers/{integration_name}/"
    os.environ['INTEGRATION_NAME'] = "test_integration"
    os.environ['API_QUEUE_DELETE_URL'] = "mock_queue_url"

@pytest.fixture
def mock_provider_resources():
    with( patch('provider_resources.storage_put_file') as mock_put, 
        patch('provider_resources.storage_get_file') as mock_get,
        patch('provider_resources.storage_list_folder') as mock_list,
        patch('provider_resources.storage_remove_files') as mock_remove,
        patch('provider_resources.storage_validate_folder') as mock_validate):
        yield {
            'put_file': mock_put,
            'get_file': mock_get,
            'list_folder': mock_list,
            'remove_files': mock_remove,
            'validate_folder': mock_validate,
        }

@pytest.fixture
def mock_conf_utils():
    with patch('conf_utils.get_profile') as mock_get_profile:
        mock_get_profile.return_value = {'profile_name': 'mock_profile', 'default_ocr': 'mock_ocr'}
        yield mock_get_profile

@pytest.fixture
def mock_docs_utils():
    with patch('docs_utils.parse_file_name') as mock_parse_file_name:
        mock_parse_file_name.return_value = 'parsed_filename'
        yield mock_parse_file_name

# Test generate_request
def test_generate_request(setup_env_vars, mock_conf_utils):
    apigw_params = {'x-department': 'mock_department', 'x-tenant': 'mock_tenant'}
    input_json = {'documents_folder': 'custom_folder'}

    result = generate_request(apigw_params, input_json)

    assert 'integration_id' in result
    assert result['apigw_params'] == apigw_params
    assert result['input_json'] == input_json
    assert result['documents_folder'] == 'custom_folder'
    assert result['client_profile']['profile_name'] == 'mock_profile'

# Test persist_request
def test_persist_request(setup_env_vars, mock_provider_resources):
    mock_put = mock_provider_resources['put_file']
    
    request_json = {
        'integration_id': 'mock_integration_id',
        'ts_init': datetime.now().timestamp(),
        'documents': [{}],
        'process_ids': {},
        'status': 'waiting'
    }

    persist_request(request_json)

    assert current_requests['mock_integration_id'] == request_json
    assert mock_put.call_count == 2  # One for the full request, one for the summary

# Test restore_requests
def test_restore_requests(setup_env_vars, mock_provider_resources):
    mock_list = mock_provider_resources['list_folder']
    mock_get = mock_provider_resources['get_file']
    
    mock_list.return_value = ['mock_pointers/mock_integration_id.json']
    mock_get.return_value = json.dumps({
        'integration_id': 'mock_integration_id',
        'ts_init': datetime.now().timestamp(),
        'documents': [{}],
        'process_ids': {}
    })

    restore_requests()

    assert 'mock_integration_id' in current_requests
    assert mock_list.called
    assert mock_get.called

# Test delete_request
def test_delete_request(setup_env_vars, mock_provider_resources):
    mock_remove = mock_provider_resources['remove_files']

    current_requests['mock_integration_id'] = {
        'integration_id': 'mock_integration_id',
        'process_ids': {'pid1': 'mock_integration_id'}
    }
    current_processes_map['pid1'] = 'mock_integration_id'

    delete_request('mock_integration_id')

    assert 'mock_integration_id' not in current_requests
    assert 'pid1' not in current_processes_map
    assert mock_remove.call_count == 2

# Test check_timeout
def test_check_timeout(setup_env_vars, mock_provider_resources):
    current_requests['mock_integration_id'] = {
        'integration_id': 'mock_integration_id',
        'ts_init': (datetime.now() - timedelta(minutes=10)).timestamp(),
        'process_ids': {}
    }

    with patch("requests_manager.provider_resources.storage_validate_folder")as mock_validate_folder:
        mock_validate_folder.return_value = False
        check_timeout(5)

    assert 'mock_integration_id' not in current_requests

# Test update_request
def test_update_request(setup_env_vars, mock_provider_resources, mock_docs_utils):
    current_requests['mock_integration_id'] = {
        'integration_id': 'mock_integration_id',
        'process_ids': {'pid1': 'waiting'},
        'documents_metadata': {}
    }
    current_processes_map['pid1'] = 'mock_integration_id'

    response_json = {
        'pid': 'pid1',
        'integration': {
            'integration_id': "test_id_integration",
            'documents_folder': "mock_folder"
        },
        'status': 'completed',
        'tracking': {'step': 'done'},
        'filename': 'mock_file',
    }

    update_request(response_json)

    assert current_requests['mock_integration_id']['process_ids']['pid1'] == 'waiting'

