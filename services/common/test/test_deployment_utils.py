### This code is property of the GGAO ###

import pytest
import os
from unittest.mock import MagicMock, patch, call
from deployment_utils import BaseDeployment
from common.errors.genaierrors import PrintableGenaiError, GenaiError

# Dummy subclass to test BaseDeployment (since it's abstract)
class TestDeployment(BaseDeployment):
    @property
    def service_name(self):
        return "test_service"

    @property
    def max_num_queue(self):
        return 5

    def process(self, json_input):
        # Dummy process implementation
        return True, {"status": "success"}, "next_service"


@pytest.fixture
def deployment():
    """Fixture to initialize TestDeployment."""
    return TestDeployment()


def test_service_name(deployment):
    assert deployment.service_name == "test_service"


def test_max_num_queue(deployment):
    assert deployment.max_num_queue == 5

def test_must_continue(deployment):
    assert deployment.must_continue is False

def test_service_name2(deployment):
    assert deployment.service_name == "test_service"


def test_send_any_message_queue(deployment, mocker):
    mock_set_queue = mocker.patch("deployment_utils.set_queue")
    mock_write_to_queue = mocker.patch("deployment_utils.write_to_queue")

    url = "queue_name"
    message = {"key": "value"}
    assert deployment.send_any_message(url, message)

    mock_set_queue.assert_called_once()
    mock_write_to_queue.assert_called_once_with((deployment.Q_IN[0], url), message)

def test_send_any_message_queue_error(deployment, mocker):
    mock_set_queue = mocker.patch("deployment_utils.set_queue")
    mock_write_to_queue = mocker.patch("deployment_utils.write_to_queue")
    mock_write_to_queue.side_effect = Exception

    url = "queue_name"
    message = {"key": "value"}
    assert not deployment.send_any_message(url, message)



def test_send_any_message_api(deployment, mocker):
    mock_post = mocker.patch("requests.post")

    url = "https://api.example.com"
    message = {"key": "value"}
    assert deployment.send_any_message(url, message)

    mock_post.assert_called_once_with(url, json=message)

def test_send_any_message_api_error(deployment, mocker):
    mock_post = mocker.patch("requests.post")
    mock_post.side_effect = Exception

    url = "https://api.example.com"
    message = {"key": "value"}
    assert not deployment.send_any_message(url, message)


def test_generate_tracking_message(deployment):
    request_json = {}
    service_name = "test_service"
    tracking_type = "INPUT"

    updated_request = deployment.generate_tracking_message(request_json, service_name, tracking_type)

    assert "tracking" in updated_request
    assert "pipeline" in updated_request["tracking"]
    assert updated_request["tracking"]["pipeline"][0]["step"] == service_name.upper()


def test_propagate_queue_message_input_with_key(deployment):
    raw_input = {"input_key": {"data": "value"}}
    with patch("os.getenv", return_value="input_key"):
        raw, json_input = deployment.propagate_queue_message_input(raw_input)
        assert json_input == {"data": "value"}
        assert raw == raw_input


def test_propagate_queue_message_output_with_key(deployment):
    raw_input = {"data": "value"}
    json_output = {"result": "value"}
    with patch("os.getenv", return_value="output_key"):
        output = deployment.propagate_queue_message_output(raw_input, json_output)
        assert output["output_key"] == {"result": "value"}


@patch("deployment_utils.read_from_queue", return_value=([{"data": "value"}], ["entry"]))
@patch("deployment_utils.delete_from_queue")
@patch.object(TestDeployment, "process", return_value=(True, {"status": "success"}, "next_service"))
def test_async_deployment(mock_process, mock_delete, mock_read, deployment):
    def set_kill_flag_and_return_data(*args, **kwargs):
        deployment.killer.kill_now = True
        return [None], [None]
    
    mock_read.side_effect = set_kill_flag_and_return_data
    deployment.send_tracking_message = MagicMock()
    with patch("deployment_utils.set_queue"), \
        patch("deployment_utils.write_to_queue"):
        deployment.async_deployment()
    mock_process.assert_called_once()
    mock_read.assert_called_once()
    mock_delete.assert_called_once()

@patch("deployment_utils.read_from_queue", return_value=([{"data": "value"}], ["entry"]))
@patch("deployment_utils.delete_from_queue")
@patch.object(TestDeployment, "process", return_value=(True, {"status": "success"}, "next_service"))
def test_async_deployment_except(mock_process, mock_delete, mock_read, deployment):
    def set_kill_flag_and_return_data(*args, **kwargs):
        deployment.killer.kill_now = True
        return [None], [None]
    
    mock_read.side_effect = set_kill_flag_and_return_data
    deployment.send_tracking_message = MagicMock()
    with patch("deployment_utils.set_queue"), \
        patch("deployment_utils.write_to_queue"),\
        patch("deployment_utils.get_document", side_effect = Exception):
        deployment.async_deployment()


@patch("deployment_utils.get_dataset_status_key", return_value="test_key")
@patch.object(TestDeployment, "process", return_value=(True, {"status": "success"}, "next_service"))
def test_sync_deployment_success(mock_process, mock_dataset_key, deployment):
    dat = {"data": "value"}
    response, status_code = deployment.sync_deployment(dat)
    assert status_code == 200
    assert '"status": "finished"' in response


@patch("deployment_utils.get_dataset_status_key", return_value="test_key")
@patch.object(TestDeployment, "process", return_value=(True, {"status_code": 500}, "next_service"))
def test_sync_deployment_success_500(mock_process, mock_dataset_key, deployment):
    dat = {"data": "value"}
    response, status_code = deployment.sync_deployment(dat)
    assert status_code == 500


@patch("deployment_utils.time.sleep", return_value=None)
@patch.object(TestDeployment, "process", side_effect=[None, Exception])
def test_cron_deployment(mock_process, mock_sleep, deployment):
    deployment.killer.kill_now = False
    def set_killer(*args, **kwargs):
        deployment.killer.kill_now = True
    deployment.process.side_effect = set_killer
    deployment.cron_deployment(time_sleep=0)
    mock_process.assert_called_once()

@patch("deployment_utils.time.sleep", side_effect=Exception)
@patch.object(TestDeployment, "process", side_effect=[None, Exception])
def test_cron_deployment_except(mock_process, mock_sleep, deployment):
    deployment.killer.kill_now = False
    def set_killer(*args, **kwargs):
        deployment.killer.kill_now = True
    deployment.process.side_effect = set_killer
    deployment.cron_deployment(time_sleep=0)


@patch.object(TestDeployment, "process", return_value={"status": "success"})
def test_call_back_deployment_success(mock_process, deployment):
    response = {"data": "value"}
    result, status_code = deployment.call_back_deployment(response)
    assert status_code == 200
    assert '"status": "finished"' in result
    mock_process.assert_called_once_with(response)

    
@patch.object(TestDeployment, "process", return_value={"status": "success"})
def test_call_back_deployment_except(mock_process, deployment):
    mock_process.side_effect = Exception
    response = {"data": "value"}
    result, status_code = deployment.call_back_deployment(response)
    assert status_code == 500


@patch("deployment_utils.get_dataset_status_key", return_value="test_key")
@patch.object(TestDeployment, "process", side_effect=Exception)
def test_sync_deployment_error_500(mock_process, mock_dataset_key, deployment):
    dat = {"data": "value"}
    response, status_code = deployment.sync_deployment(dat)
    assert status_code == 500
    assert '"status": "error"' in response

@patch("deployment_utils.get_dataset_status_key", return_value="test_key")
@patch.object(TestDeployment, "process", side_effect=PrintableGenaiError)
def test_sync_deployment_error_Printablegenaierror(mock_process, mock_dataset_key, deployment):
    dat = {"data": "value"}
    response, status_code = deployment.sync_deployment(dat)
    assert status_code == 500
    assert '"status": "error"' in response

@patch("deployment_utils.get_dataset_status_key", return_value="test_key")
@patch.object(TestDeployment, "process", side_effect=GenaiError)
def test_sync_deployment_error_Genaierror(mock_process, mock_dataset_key, deployment):
    dat = {"data": "value"}
    response, status_code = deployment.sync_deployment(dat)
    assert status_code == 500
    assert '"status": "error"' in response

@patch("deployment_utils.get_dataset_status_key", return_value="test_key")
@patch.object(TestDeployment, "process", side_effect=KeyError)
def test_sync_deployment_error_KeyError(mock_process, mock_dataset_key, deployment):
    dat = {"data": "value"}
    response, status_code = deployment.sync_deployment(dat)
    assert status_code == 400
    assert '"status": "error"' in response

    
def test_send_tracking(deployment):
    deployment.generate_tracking_message = MagicMock()
    deployment.send_any_message = MagicMock()
    tracking_type = "test_tracking"
    os.environ[f"TRACKING_{tracking_type}_URL"] = "test_url"
    deployment.send_tracking_message(None, None, tracking_type)
    
def test_report_api(deployment, mocker):
    mock_post = mocker.patch("requests.post")
    deployment.report_api(1, "test_id", "test_url", "resource", "process_id", "PAGS")

def test_report_api_error(deployment, mocker):
    mock_post = mocker.patch("requests.post")
    mock_post.side_effect = Exception
    deployment.report_api(1, "test_id", "test_url", "resource", "process_id", "PAGS")