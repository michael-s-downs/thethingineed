### This code is property of the GGAO ###

import pytest
from unittest.mock import patch, MagicMock
from main import FlowMgmtInfoDeleteDeployment  # Adjust based on actual file/module name


@pytest.fixture
def deployment():
    """Fixture to create the deployment instance."""
    with patch("main.set_queue"):
        return FlowMgmtInfoDeleteDeployment()


@patch('main.get_dataset_status_key', return_value='1234:5678')
@patch('main.get_headers', return_value={'x-department': 'test-department'})
@patch('main.delete_folder')
@patch('main.delete_status')
def test_process_success(mock_delete_status, mock_delete_folder, mock_get_headers, mock_get_dataset_status_key, deployment):
    """Test successful processing of message."""
    json_input = {'some': 'data'}
    
    result = deployment.process(json_input)

    # Asserting the correct calls were made
    mock_get_dataset_status_key.assert_called_once_with(json_input)
    mock_get_headers.assert_called_once_with(json_input)
    mock_delete_folder.assert_called()
    mock_delete_status.assert_called_once()

    # Asserting the return values
    assert result == (False, json_input, deployment.service_name)


@patch('main.get_dataset_status_key', side_effect=Exception("Test error"))
def test_process_dataset_status_key_error(mock_get_dataset_status_key, deployment):
    """Test process when get_dataset_status_key fails."""
    deployment.logger = MagicMock()
    json_input = {'some': 'data'}

    result = deployment.process(json_input)

    # Asserting the correct calls were made
    mock_get_dataset_status_key.assert_called_once()
    deployment.logger.error.assert_called()

    # Asserting the return values
    assert result == (False, json_input, deployment.service_name)


@patch('main.get_dataset_status_key', return_value='1234:5678')
@patch('main.get_headers', side_effect=Exception("Test error"))
def test_process_headers_error(mock_get_headers, mock_get_dataset_status_key, deployment):
    """Test process when headers retrieval fails."""
    deployment.logger = MagicMock()
    json_input = {'some': 'data'}

    result = deployment.process(json_input)

    # Asserting the correct calls were made
    mock_get_dataset_status_key.assert_called_once_with(json_input)
    mock_get_headers.assert_called_once_with(json_input)

    # Asserting the return values
    assert result == (False, json_input, deployment.service_name)


@patch('main.get_dataset_status_key', return_value='1234:5678')
@patch('main.get_headers', return_value={'x-department': 'test-department'})
@patch('main.delete_folder', side_effect=Exception("Test error"))
def test_process_delete_folder_error(mock_delete_folder, mock_get_headers, mock_get_dataset_status_key, deployment):
    """Test process when delete_folder fails."""
    deployment.logger = MagicMock()
    json_input = {'some': 'data'}

    result = deployment.process(json_input)

    # Asserting the correct calls were made
    mock_get_dataset_status_key.assert_called_once_with(json_input)
    mock_get_headers.assert_called_once_with(json_input)
    mock_delete_folder.assert_called()

    # Asserting the return values
    assert result == (False, json_input, deployment.service_name)


@patch('main.get_dataset_status_key', return_value='1234:5678')
@patch('main.get_headers', return_value={'x-department': 'test-department'})
@patch('main.delete_folder')
@patch('main.delete_status', side_effect=Exception("Test error"))
def test_process_delete_status_error(mock_delete_status, mock_delete_folder, mock_get_headers, mock_get_dataset_status_key, deployment):
    """Test process when delete_status fails."""
    deployment.logger = MagicMock()
    json_input = {'some': 'data'}

    result = deployment.process(json_input)

    # Asserting the correct calls were made
    mock_get_dataset_status_key.assert_called_once_with(json_input)
    mock_get_headers.assert_called_once_with(json_input)
    mock_delete_folder.assert_called()
    mock_delete_status.assert_called()

    # Asserting the return values
    assert result == (False, json_input, deployment.service_name)


def test_service_name(deployment):
    """Test service_name property."""
    assert deployment.service_name == "flowmgmt_infodelete"


def test_must_continue(deployment):
    """Test must_continue property."""
    assert deployment.must_continue is False


def test_max_num_queue(deployment):
    """Test max_num_queue property."""
    assert deployment.max_num_queue == 1
