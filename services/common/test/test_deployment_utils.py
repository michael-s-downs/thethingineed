### This code is property of the GGAO ###

import pytest
from unittest.mock import MagicMock, patch, call
from deployment_utils import BaseDeployment

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


def test_send_any_message_api(deployment, mocker):
    mock_post = mocker.patch("requests.post")

    url = "https://api.example.com"
    message = {"key": "value"}
    assert deployment.send_any_message(url, message)

    mock_post.assert_called_once_with(url, json=message)


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
    deployment.killer.kill_now = False
    def set_kill_flag():
        deployment.killer.kill_now = True
    mock_read.side_effect = lambda: (set_kill_flag(), [])
    deployment.async_deployment()
    mock_process.assert_called_once()
    mock_read.assert_called_once()
    mock_delete.assert_called_once()


@patch("deployment_utils.get_dataset_status_key", return_value="test_key")
@patch.object(TestDeployment, "process", return_value=(True, {"status": "success"}, "next_service"))
def test_sync_deployment_success(mock_process, mock_dataset_key, deployment):
    dat = {"data": "value"}
    response, status_code = deployment.sync_deployment(dat)
    assert status_code == 200
    assert '"status": "finished"' in response


@patch("deployment_utils.time.sleep", return_value=None)
@patch.object(TestDeployment, "process", side_effect=[None, Exception])
def test_cron_deployment(mock_process, mock_sleep, deployment):
    with patch.object(deployment.killer, "kill_now", side_effect=[False, True]):
        deployment.cron_deployment(time_sleep=0)
    mock_process.assert_called_once()


@patch.object(TestDeployment, "process", return_value={"status": "success"})
def test_call_back_deployment_success(mock_process, deployment):
    response = {"data": "value"}
    result, status_code = deployment.call_back_deployment(response)
    assert status_code == 200
    assert '"status": "finished"' in result
    mock_process.assert_called_once_with(response)
