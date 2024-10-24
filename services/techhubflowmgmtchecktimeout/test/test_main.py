### This code is property of the GGAO ###

import pytest
from unittest.mock import patch, MagicMock
import datetime
import os
import json
from main import FlowMgmtCheckTimeoutDeployment  # Assuming this is the correct import path


@pytest.fixture
def deployment():
    """Fixture to create the deployment instance."""
    with patch('os.getenv', return_value='test_tenant'):
        return FlowMgmtCheckTimeoutDeployment()


def test_get_expired(deployment):
    """Test the get_expired method when there are expired keys."""
    # Set datetime mock behavior
    
    # Mock Redis pattern
    redis_data = [
        {'key': b'timeout_id_test_tenant:123', 'values': json.dumps({'timestamp': 1609459200, 'filename': 'test_file'}).encode()},
        {'key': b'timeout_id_test_tenant:456', 'values': json.dumps({'timestamp': 1609459200, 'filename': 'test_file_2'}).encode()},
    ]
    with patch('main.get_redis_pattern', return_value=redis_data):
        expired = deployment.get_expired()

    assert len(expired) == 2



@patch('main.delete_status')
def test_remove_expired(mock_delete_status, deployment):
    """Test the remove_expired method."""
    expired = [('timeout_id_test_tenant:123', 'test_file', {})]
    
    deployment.remove_expired(expired)
    
    mock_delete_status.assert_called_once()


@patch('main.delete_status')
def test_remove_expired_no_expired(mock_delete_status, deployment):
    """Test remove_expired method when no keys are expired."""
    expired = []
    
    deployment.remove_expired(expired)
    
    mock_delete_status.assert_not_called()


@patch('main.write_to_queue')
@patch('main.get_dataset_status_key', return_value='test_key')
@patch('main.update_status')
def test_return_expired(mock_update_status, mock_get_dataset_status_key, mock_write_to_queue, deployment):
    """Test the return_expired method."""
    expired = [('timeout_id_test_tenant:123', 'test_file', {'key': 'value'})]
    
    deployment.return_expired(expired)
    
    mock_write_to_queue.assert_called_once()


@patch('main.write_to_queue')
@patch('main.get_dataset_status_key')
@patch('main.update_status')
def test_return_expired_no_expired(mock_update_status, mock_get_dataset_status_key, mock_write_to_queue, deployment):
    """Test return_expired method when there are no expired processes."""
    expired = []
    
    deployment.return_expired(expired)
    
    mock_write_to_queue.assert_not_called()
    mock_update_status.assert_not_called()


@patch('main.FlowMgmtCheckTimeoutDeployment.get_expired', return_value=[('timeout_id_test_tenant:123', 'test_file', {})])
@patch('main.FlowMgmtCheckTimeoutDeployment.remove_expired')
@patch('main.FlowMgmtCheckTimeoutDeployment.return_expired')
def test_process_success(mock_return_expired, mock_remove_expired, mock_get_expired, deployment):
    """Test process method when everything works as expected."""
    deployment.process({})
    
    mock_get_expired.assert_called_once()
    mock_remove_expired.assert_called_once()
    mock_return_expired.assert_called_once()


@patch('main.FlowMgmtCheckTimeoutDeployment.get_expired', side_effect=Exception("Test error"))
@patch('main.get_exc_info', return_value='Exception info')
def test_process_failure(mock_get_exc_info, mock_get_expired, deployment):
    """Test process method when an error occurs."""
    deployment.logger = MagicMock()
    
    deployment.process({})
    
    mock_get_expired.assert_called_once()
    deployment.logger.error.assert_called_once_with('Error checking timeouts in Redis', exc_info='Exception info')


def test_service_name(deployment):
    """Test service_name property."""
    assert deployment.service_name == 'flowmgmt_checktimeout'


def test_must_continue(deployment):
    """Test must_continue property."""
    assert deployment.must_continue is True


def test_max_num_queue(deployment):
    """Test max_num_queue property."""
    assert deployment.max_num_queue == 1
