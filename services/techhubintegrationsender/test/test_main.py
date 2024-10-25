### This code is property of the GGAO ###

import sys
import os
import pytest
sys.path.append(os.getenv('LOCAL_COMMON_PATH'))
from unittest.mock import Mock, patch, call, MagicMock

from unittest import mock
from main import process, killer

# Mock global dependencies
@mock.patch('main.provider_resources.queue_read_messages')
@mock.patch('main.provider_resources.queue_delete_message')
@mock.patch('main.requests_manager.restore_requests')
@mock.patch('main.requests_manager.persist_request')
@mock.patch('main.requests_manager.update_request')
@mock.patch('main.requests_manager.check_timeout')
@mock.patch('main.requests_manager.current_requests', new_callable=dict)
@mock.patch('main.process_request')
@mock.patch('main.receive_request')
@mock.patch('logging_handler.logger')
@mock.patch('os.getenv')
@mock.patch('main.requests_manager.storage_persist_request', side_effect=False)
def test_process(
    mock_storage_persist_request,
    mock_getenv, 
    mock_logger, 
    mock_receive_request, 
    mock_process_request, 
    mock_current_requests, 
    mock_check_timeout, 
    mock_update_request, 
    mock_persist_request, 
    mock_restore_requests, 
    mock_queue_delete_message, 
    mock_queue_read_messages
):
    # Mock environment and queue settings
    mock_getenv.return_value = 'integration_test'
    mock_current_requests.clear()
    mock_current_requests.update({'test_id': {'status': 'processing'}})
    
    # Set the GracefulKiller kill_now flag to True after first loop
    killer.kill_now = False
    def set_kill_flag():
        killer.kill_now = True
    mock_queue_read_messages.side_effect = lambda: (set_kill_flag(), [])

    # Test message processing with mock queue messages
    mock_message_request = {'type': 'request', 'integration_id': 'test_id', 'status': 'processing'}
    mock_message_response = {'type': 'response', 'pid': 'test_pid', 'status': 'done'}
    mock_message_meh = {'type': 'other', 'status': 'test_status'}

    # Setup mock responses for queue reading
    mock_queue_read_messages.side_effect = [
        ([mock_message_request], []),
        ([mock_message_response], []),
        ([mock_message_meh], [])
    ]

    # Test parsing for unknown message type
    mock_receive_request.side_effect = lambda x: ({"status": "processing"}, None)

    # Execute the process function
    with pytest.raises(StopIteration):
        process()

    assert mock_queue_read_messages.call_count >= 1
    assert mock_restore_requests.call_count == 1
    assert mock_check_timeout.called
    assert mock_persist_request.called
    assert mock_update_request.called



def test_graceful_killer():
    # Check that GracefulKiller properly stops
    assert not killer.kill_now
    killer.kill_now = True
    assert killer.kill_now


@mock.patch('main.provider_resources.queue_read_messages')
@mock.patch('main.provider_resources.queue_delete_message')
@mock.patch('main.requests_manager.restore_requests')
@mock.patch('main.requests_manager.persist_request')
@mock.patch('main.requests_manager.update_request')
@mock.patch('main.requests_manager.check_timeout')
@mock.patch('main.requests_manager.current_requests', new_callable=dict)
@mock.patch('main.process_request')
@mock.patch('main.receive_request', return_value=[{"status": "processing"}, None])
@mock.patch('logging_handler.logger')
@mock.patch('os.getenv')
@mock.patch('main.requests_manager.storage_persist_request')
def test_process_exception(
    mock_storage_persist_request,
    mock_getenv, 
    mock_logger, 
    mock_receive_request, 
    mock_process_request, 
    mock_current_requests, 
    mock_check_timeout, 
    mock_update_request, 
    mock_persist_request, 
    mock_restore_requests, 
    mock_queue_delete_message, 
    mock_queue_read_messages
):
    # Mock environment and queue settings
    mock_getenv.return_value = 'integration_test'
    mock_current_requests.clear()
    mock_current_requests.update({'test_id': {'status': 'processing'}})
    mock_storage_persist_request.side_effect = False
    
    # Set the GracefulKiller kill_now flag to True after first loop
    killer.kill_now = False
    def set_kill_flag():
        killer.kill_now = True
    mock_queue_read_messages.side_effect = lambda: (set_kill_flag(), [])

    # Test message processing with mock queue messages
    mock_message_request = {'type': 'request', 'integration_id': 'test_id', 'status': 'processing'}
    mock_message_response = {'type': 'response', 'pid': 'test_pid', 'status': 'done'}
    mock_message_meh = {'type': 'other', 'status': 'test_status'}

    # Setup mock responses for queue reading
    mock_queue_read_messages.side_effect = [
        ([mock_message_request], []),
        ([mock_message_response], []),
        ([mock_message_meh], [])
    ]

    # Test parsing for unknown message type
    mock_receive_request.side_effect = lambda x: ({"status": "processing"}, None)

    # Execute the process function
    with pytest.raises(StopIteration):
        process()

    assert mock_queue_read_messages.call_count >= 1
    assert mock_restore_requests.call_count == 1
    assert mock_check_timeout.called
    assert mock_persist_request.called
    assert mock_update_request.called

