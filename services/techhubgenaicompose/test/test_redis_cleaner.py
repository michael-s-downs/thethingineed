### This code is property of the GGAO ###

import os
import sys
import redis_cleaner
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
from redis_cleaner import RedisCleaner, run_redis_cleaner

# Fixture to mock RedisCleaner instance
@pytest.fixture
def redis_cleaner():
    return RedisCleaner()

# Test case: Initialization of RedisCleaner
def test_redis_cleaner_init(redis_cleaner):
    assert redis_cleaner.tenant == os.getenv("TENANT")
    assert redis_cleaner.session_to_remove == []

# Test case: Service name property
def test_service_name(redis_cleaner):
    assert redis_cleaner.service_name == "flowmgmt_cleaner"

# Test case: max_num_queue property
def test_max_num_queue(redis_cleaner):
    assert redis_cleaner.max_num_queue == 1

# Test case: Process method with expired sessions
@patch('redis_cleaner.get_redis_pattern')
@patch('redis_cleaner.delete_status')
@patch('redis_cleaner.os.getenv', side_effect=lambda x, y=None: {"TENANT": "test_tenant", 
                                                                   "REDIS_SESSION_EXPIRATION_TIME": "48"
                                                                   }.get(x, y))
@patch('redis_cleaner.datetime')
def test_process_expired_sessions(mock_datetime, mock_getenv, mock_delete_status, mock_get_redis_pattern, redis_cleaner):
    # Mocking datetime to return a fixed 'now' time
    mock_now = datetime(2023, 10, 1, 12, 0, 0)
    mock_datetime.now.return_value = mock_now
    mock_datetime.strptime = datetime.strptime
    
    # Mock session data
    sessions = [
        {"key": "session:test_tenant:1", "values": '{"last_update": "2023-09-28 10:00:00"}'.encode()},
        {"key": "session:test_tenant:2", "values": '{"last_update": "2023-11-30 10:00:00"}'.encode()},
    ]
    mock_get_redis_pattern.return_value = sessions

    # Call the process method
    redis_cleaner.process({})

    # Expected behavior: only the session older than 48 hours should be marked for removal
    assert redis_cleaner.session_to_remove == ["session:test_tenant:1"]
    mock_delete_status.assert_called_once()

# Test case: Process method with no sessions to remove
@patch('redis_cleaner.get_redis_pattern')
@patch('redis_cleaner.delete_status')
@patch('redis_cleaner.os.getenv', side_effect=lambda x, y=None: {"TENANT": "test_tenant", "REDIS_SESSION_EXPIRATION_TIME": "48"}.get(x, y))
@patch('redis_cleaner.datetime')
def test_process_no_sessions_to_remove(mock_datetime, mock_getenv, mock_delete_status, mock_get_redis_pattern, redis_cleaner):
    # Mocking datetime to return a fixed 'now' time
    mock_now = datetime(2023, 10, 1, 12, 0, 0)
    mock_datetime.now.return_value = mock_now
    
    # Mock session data: no sessions older than 48 hours
    sessions = [
        {"key": "session:test_tenant:1", "values": '{"last_update": "2023-09-30 10:00:00"}'.encode()},
        {"key": "session:test_tenant:2", "values": '{"last_update": "2023-10-01 10:00:00"}'.encode()},
    ]
    mock_get_redis_pattern.return_value = sessions

    # Call the process method
    redis_cleaner.process({})

    # Expected behavior: no sessions should be marked for removal
    assert redis_cleaner.session_to_remove == []
    mock_delete_status.assert_not_called()


@patch('redis_cleaner.get_redis_pattern')
@patch('redis_cleaner.delete_status')
@patch('redis_cleaner.os.getenv', side_effect=lambda x, y=None: {"TENANT": "test_tenant", "REDIS_SESSION_EXPIRATION_TIME": "48"}.get(x, y))
@patch('redis_cleaner.datetime')
def test_process_exception_handling( mock_datetime, mock_getenv, mock_delete_status, mock_get_redis_pattern, redis_cleaner):
    # Mocking datetime to return a fixed 'now' time
    mock_now = datetime(2023, 10, 1, 12, 0, 0)
    mock_datetime.now.return_value = mock_now
    
    # Mock session data with invalid format to trigger the exception
    sessions = [
        {"key": "session:test_tenant:1", "values": '{"last_update": "invalid-date-format"}'.encode()}
    ]
    mock_get_redis_pattern.return_value = sessions

    # Call the process method
    redis_cleaner.process({})

    # Ensure no session was removed due to exception
    assert redis_cleaner.session_to_remove == []


@patch('redis_cleaner.get_redis_pattern', side_effect= Exception)
@patch('redis_cleaner.delete_status')
@patch('redis_cleaner.os.getenv', side_effect=lambda x, y=None: {"TENANT": "test_tenant", "REDIS_SESSION_EXPIRATION_TIME": "48"}.get(x, y))
@patch('redis_cleaner.datetime')
def test_process_exception_global(mock_datetime, mock_getenv, mock_delete_status, mock_get_redis_pattern, redis_cleaner):
    # Mocking datetime to return a fixed 'now' time
    
    mock_get_redis_pattern.return_value = Exception

    # Call the process method
    redis_cleaner.process({})
    assert redis_cleaner.session_to_remove == []



# Test case: Running the redis cleaner
@patch('redis_cleaner.RedisCleaner.cron_deployment')
@patch('redis_cleaner.os.getenv', return_value="60")
def test_run_redis_cleaner(mock_getenv, mock_cron_deployment):
    run_redis_cleaner()
    mock_cron_deployment.assert_called_once_with(60)
