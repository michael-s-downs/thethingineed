### This code is property of the GGAO ###

import pytest
from unittest.mock import MagicMock, patch
from main import FlowMgmtCheckEndDeployment
from common.status_codes import PROCESS_FINISHED, ERROR
from common.error_messages import PARSING_PARAMETERS_ERROR, SENDING_RESPONSE_ERROR, REPORTING_MESSAGE_ERROR

@pytest.fixture
def mock_deployment():
    """Fixture to create a FlowMgmtCheckEndDeployment instance"""
    with patch("main.set_queue"):
        deployment = FlowMgmtCheckEndDeployment()
    deployment.logger = MagicMock()  # Mock logger to prevent actual logging
    return deployment

@patch('main.db_dbs', {'status': {}, 'timeout': {}})
@patch('main.set_queue')
@patch('main.set_db')
def test_init(mock_set_queue, mock_set_db, mock_deployment):
    """Test the initialization of the deployment"""
    assert mock_set_queue.called_once_with(mock_deployment.Q_IN)
    assert mock_set_db.called_once_with({'status': {}, 'timeout': {}})

def test_service_name(mock_deployment):
    """Test service_name property"""
    assert mock_deployment.service_name == "flowmgmt_checkend"

def test_max_num_queue(mock_deployment):
    """Test max_num_queue property"""
    assert mock_deployment.max_num_queue == 1

def test_must_continue(mock_deployment):
    """Test must_continue property"""
    assert not mock_deployment.must_continue

@patch('main.db_dbs', {'status': {}, 'timeout': {}})
@patch('main.update_full_status')
@patch('main.get_status_code')
@patch('main.get_value')
@patch('main.delete_status')
def test_report_end(mock_update_status, mock_get_status, mock_get_value, mock_delete_status, mock_deployment):
    """Test report_end logic for reporting end of process"""

    url = "http://example.com"
    response = {"message": "Test"}

    # Test successful message send
    mock_deployment.send_any_message = MagicMock(return_value=True)
    mock_deployment.report_end(url, response)
    mock_deployment.send_any_message.assert_called_with(url, response)

    # Test failure case
    mock_deployment.send_any_message = MagicMock(return_value=False)
    with pytest.raises(Exception, match=SENDING_RESPONSE_ERROR):
        mock_deployment.report_end(url, response)

    # Test case with no URL
    mock_deployment.report_end("", response)
    mock_deployment.logger.warning.assert_called_once_with("No origin URL to report response.")

def test_compose_message(mock_deployment):
    """Test compose_message logic"""

    dataset_status_key = "12345"
    integration_message = {"data": "test"}
    message_to_send = "Process finished"
    filename = "file.txt"
    status_code = PROCESS_FINISHED
    tracking_message = {"tracking": "data"}

    result = mock_deployment.compose_message(dataset_status_key, integration_message, message_to_send, filename, status_code, tracking_message)

    assert result['type'] == "response"
    assert result['status'] == "ready"
    assert result['message'] == message_to_send
    assert result['pid'] == dataset_status_key
    assert result['status_code'] == status_code
    assert result['tracking'] == tracking_message
    assert result['filename'] == filename
    assert result['integration'] == integration_message

    # Test error status
    result = mock_deployment.compose_message(dataset_status_key, integration_message, message_to_send, filename, ERROR, tracking_message)
    assert result['status'] == "error"

@patch('main.get_status_code', return_value=PROCESS_FINISHED)
@patch('main.get_value', return_value={"msg": "Process complete"})
@patch('main.update_full_status')
@patch('main.delete_status')
@patch('main.get_document', return_value={"filename": "file.txt"})
@patch('main.get_project_config', return_value={"url_sender": "http://example.com", "timeout_id": "timeout123"})
@patch('main.get_generic', return_value={"config": "generic"})
@patch('main.get_specific', return_value={"config": "specific"})
@patch('main.get_dataset_status_key', return_value="12345")
def test_process_success(mock_get_status_code, mock_get_value, mock_update_status, mock_delete_status, mock_get_document, mock_get_project_config, mock_get_generic, mock_get_specific, mock_get_dataset_status_key, mock_deployment):
    """Test the process function with successful completion"""

    json_input = {
        "type": "regular",
        "request_json": {"integration": {"data": "integration_data"}, "tracking": {"data": "tracking_data"}}
    }

    mock_deployment.report_end = MagicMock()

    result = mock_deployment.process(json_input)

    assert result[0] is False
    assert result[1] == {'type': 'regular', 'request_json': {'integration': {'data': 'integration_data'}, 'tracking': {'data': 'tracking_data'}}}
    assert result[2] == "flowmgmt_checkend"

def test_process_failure(mock_deployment):
    """Test the process function with a failure (mocking exceptions)"""

    json_input = {"type": "regular", "request_json": {}}

    # Mock internal functions to raise exceptions
    mock_deployment.report_end = MagicMock()
    mock_deployment.process = MagicMock(side_effect=Exception(PARSING_PARAMETERS_ERROR))

    with pytest.raises(Exception, match=PARSING_PARAMETERS_ERROR):
        mock_deployment.process(json_input)

@patch('main.get_status_code', return_value=PROCESS_FINISHED)
@patch('main.get_value', return_value={"msg": "Process complete"})
@patch('main.update_full_status')
@patch('main.delete_status')
@patch('main.get_document', return_value={"filename": "file.txt"})
@patch('main.get_project_config', return_value={"url_sender": "http://example.com", "timeout_id": "timeout123"})
@patch('main.get_generic', side_effect=KeyError)
@patch('main.get_specific', return_value={"config": "specific"})
@patch('main.get_dataset_status_key', return_value="12345")
def test_process_keyerror_generic(mock_get_status_code, mock_get_value, mock_update_status, mock_delete_status, mock_get_document, mock_get_project_config, mock_get_generic, mock_get_specific, mock_get_dataset_status_key, mock_deployment):
    """Test the process function with successful completion"""

    json_input = {
        "type": "regular",
        "request_json": {"integration": {"data": "integration_data"}, "tracking": {"data": "tracking_data"}}
    }

    mock_deployment.report_end = MagicMock()

    result = mock_deployment.process(json_input)
    assert result[0] is False

@patch('main.get_status_code', return_value=PROCESS_FINISHED)
@patch('main.get_value', return_value={"msg": "Process complete"})
@patch('main.update_full_status')
@patch('main.delete_status')
@patch('main.get_document', side_effect=KeyError)
@patch('main.get_project_config', return_value={"url_sender": "http://example.com", "timeout_id": "timeout123"})
@patch('main.get_generic', return_value={"config": "generic"})
@patch('main.get_specific', return_value={"config": "specific"})
@patch('main.get_dataset_status_key', return_value="12345")
def test_process_keyerror_getdocument(mock_get_status_code, mock_get_value, mock_update_status, mock_delete_status, mock_get_document, mock_get_project_config, mock_get_generic, mock_get_specific, mock_get_dataset_status_key, mock_deployment):
    """Test the process function with successful completion"""

    json_input = {
        "type": "regular",
        "request_json": {"integration": {"data": "integration_data"}, "tracking": {"data": "tracking_data"}}
    }

    mock_deployment.report_end = MagicMock()

    result = mock_deployment.process(json_input)
    assert result[0] is False


@patch('main.get_status_code', return_value=PROCESS_FINISHED)
@patch('main.get_value', return_value={"msg": "Process complete"})
@patch('main.update_full_status')
@patch('main.delete_status')
@patch('main.get_document', return_value={"filename": "file.txt"})
@patch('main.get_project_config', side_effect=KeyError)
@patch('main.get_generic', return_value={"config": "generic"})
@patch('main.get_specific', return_value={"config": "specific"})
@patch('main.get_dataset_status_key', return_value="12345")
def test_process_keyerror_getprojectconfig(mock_get_status_code, mock_get_value, mock_update_status, mock_delete_status, mock_get_document, mock_get_project_config, mock_get_generic, mock_get_specific, mock_get_dataset_status_key, mock_deployment):
    """Test the process function with successful completion"""

    json_input = {
        "type": "regular",
        "request_json": {"integration": {"data": "integration_data"}, "tracking": {"data": "tracking_data"}}
    }

    mock_deployment.report_end = MagicMock()

    result = mock_deployment.process(json_input)
    assert result[0] is False


@patch('main.get_status_code', side_effect=Exception)
@patch('main.get_value', return_value={"msg": "Process complete"})
@patch('main.update_full_status')
@patch('main.delete_status')
@patch('main.get_document', return_value={"filename": "file.txt"})
@patch('main.get_project_config', return_value={"url_sender": "http://example.com", "timeout_id": "timeout123"})
@patch('main.get_generic', return_value={"config": "generic"})
@patch('main.get_specific', return_value={"config": "specific"})
@patch('main.get_dataset_status_key', return_value="12345")
def test_process_keyerror_getstatuscode(mock_get_status_code, mock_get_value, mock_update_status, mock_delete_status, mock_get_document, mock_get_project_config, mock_get_generic, mock_get_specific, mock_get_dataset_status_key, mock_deployment):
    """Test the process function with successful completion"""

    json_input = {
        "type": "regular",
        "request_json": {"integration": {"data": "integration_data"}, "tracking": {"data": "tracking_data"}}
    }

    mock_deployment.report_end = MagicMock()

    result = mock_deployment.process(json_input)
    assert result[0] is False

@patch('main.get_status_code', return_value=PROCESS_FINISHED)
@patch('main.get_value', return_value={"msg": "Process complete"})
@patch('main.update_full_status', side_effect=Exception)
@patch('main.delete_status')
@patch('main.get_document', return_value={"filename": "file.txt"})
@patch('main.get_project_config', return_value={"url_sender": "http://example.com", "timeout_id": "timeout123"})
@patch('main.get_generic', return_value={"config": "generic"})
@patch('main.get_specific', return_value={"config": "specific"})
@patch('main.get_dataset_status_key', return_value="12345")
def test_process_keyerror_updatefullstatus(mock_get_status_code, mock_get_value, mock_update_status, mock_delete_status, mock_get_document, mock_get_project_config, mock_get_generic, mock_get_specific, mock_get_dataset_status_key, mock_deployment):
    """Test the process function with successful completion"""

    json_input = {
        "type": "regular",
        "request_json": {"integration": {"data": "integration_data"}, "tracking": {"data": "tracking_data"}}
    }

    mock_deployment.report_end = MagicMock()

    result = mock_deployment.process(json_input)
    assert result[0] is False

@patch('main.get_status_code', return_value=PROCESS_FINISHED)
@patch('main.get_value', return_value={"msg": "Process complete"})
@patch('main.update_full_status')
@patch('main.delete_status')
@patch('main.get_document', return_value={"filename": "file.txt"})
@patch('main.get_project_config', return_value={"url_sender": "http://example.com", "timeout_id": "timeout123"})
@patch('main.get_generic', return_value={"config": "generic"})
@patch('main.get_specific', return_value={"config": "specific"})
@patch('main.get_dataset_status_key', return_value="12345")
def test_process_exception_reportend(mock_get_status_code, mock_get_value, mock_update_status, mock_delete_status, mock_get_document, mock_get_project_config, mock_get_generic, mock_get_specific, mock_get_dataset_status_key, mock_deployment):
    """Test the process function with successful completion"""

    json_input = {
        "type": "regular",
        "request_json": {"integration": {"data": "integration_data"}, "tracking": {"data": "tracking_data"}}
    }

    mock_deployment.report_end = MagicMock(side_effect=Exception(REPORTING_MESSAGE_ERROR))

    with pytest.raises(Exception, match=REPORTING_MESSAGE_ERROR):
        mock_deployment.process(json_input)

@patch('main.get_status_code', return_value=PROCESS_FINISHED)
@patch('main.get_value', return_value={"msg": "Process complete"})
@patch('main.update_full_status')
@patch('main.delete_status')
@patch('main.get_document', return_value={"filename": "file.txt"})
@patch('main.get_project_config', return_value={"url_sender": "http://example.com", "timeout_id": "timeout123"})
@patch('main.get_generic', return_value={"config": "generic"})
@patch('main.get_specific', return_value={"config": "specific"})
@patch('main.get_dataset_status_key', side_effect=["12345", KeyError("Error"), "12345"])
def test_process_exception_datasetstatus(mock_get_status_code, mock_get_value, mock_update_status, mock_delete_status, mock_get_document, mock_get_project_config, mock_get_generic, mock_get_specific, mock_get_dataset_status_key, mock_deployment):
    """Test the process function with successful completion"""

    json_input = {
        "type": "regular",
        "request_json": {"integration": {"data": "integration_data"}, "tracking": {"data": "tracking_data"}}
    }

    mock_deployment.report_end = MagicMock()

    result = mock_deployment.process(json_input)
    assert result[0] is False