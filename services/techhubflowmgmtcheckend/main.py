### This code is property of the GGAO ###

from typing import Tuple

# Custom import
from common.deployment_utils import BaseDeployment
from common.genai_controllers import db_dbs, set_queue, set_db
from common.genai_status_control import update_full_status, get_status_code, get_value, delete_status
from common.genai_json_parser import (
    get_generic,
    get_specific,
    get_document,
    get_exc_info,
    get_dataset_status_key,
    get_project_config,
)
from common.services import FLOWMGMT_CHECKEND_SERVICE
from common.status_codes import (
    ERROR,
    PROCESS_FINISHED,
)
from common.error_messages import (
    PARSING_PARAMETERS_ERROR,
    GETTING_DATASET_STATUS_KEY_ERROR,
    SENDING_RESPONSE_ERROR,
    REPORTING_MESSAGE_ERROR,
)



class FlowMgmtCheckEndDeployment(BaseDeployment):
    def __init__(self):
        """ Creates the deployment"""
        super().__init__()
        set_queue(self.Q_IN)
        set_db(db_dbs)

    @property
    def service_name(self):
        return FLOWMGMT_CHECKEND_SERVICE

    @property
    def max_num_queue(self):
        return 1

    @property
    def must_continue(self) -> bool:
        return False

    def report_end(self, url: str, response: dict):
        """ Write in next queue when end process

        :param url: URL to next queue
        :param response: Information to write in queue
        """
        self.logger.info("Reporting end of process.")

        if url:
            if not self.send_any_message(url, response):
                raise Exception(SENDING_RESPONSE_ERROR)
        else:
            self.logger.warning("No origin URL to report response.")


    def compose_message(self, dataset_status_key: str, integration_message: dict, message_to_send: str, filename: str, status_code: int, tracking_message: dict) -> dict:
        """ Create message to next queue

        :param dataset_status_key: Id process
        :param integration_message: Message of integration
        :param message_to_send: Message of process
        :param filename: Filename of process
        :param status_code: Status of process
        :param tracking_message: Message of tracking
        :return: Information with status of process
        """
        message = {
            'type': "response",
            'status': "ready",
            'message': message_to_send,
            'pid': dataset_status_key,
            'status_code': status_code,
            'tracking': tracking_message
        }
        if filename:
            message['filename'] = filename
            message['integration'] = integration_message
        if status_code == ERROR:
            message['status'] = "error"

        message = self.generate_tracking_message(message, self.service_name, "OUTPUT")

        return message

    def process(self, json_input: dict) -> Tuple[bool, dict, str]:
        """ Main function. Return if the output must be written to next step, the output to write and the next step
        :return: Tuple[bool, dict, str]
        """
        self.logger.debug(f"Data entry: {json_input}")
        redis_status = db_dbs['status']
        redis_timeout = db_dbs['timeout']
        url = ""
        filename = ""
        timeout = json_input.get('type', "") == "timeout"
        message = json_input.get('request_json') if timeout else json_input
        dataset_status_key = get_dataset_status_key(json_input=message)
        integration_message = message.get('integration')
        tracking_message = message.get('tracking', {})

        try:
            try:
                generic = get_generic(message)
                specific = get_specific(message)
            except KeyError:
                self.logger.error(f"[Process {dataset_status_key}] Error parsing JSON. No generic and specific configuration", exc_info=get_exc_info())
                raise Exception(PARSING_PARAMETERS_ERROR)

            try:
                dataset_status_key = get_dataset_status_key(specific=specific)
            except KeyError:
                self.logger.error(f"[Process {dataset_status_key}] Error parsing JSON. No dataset_status_key defined", exc_info=get_exc_info())
                raise Exception(GETTING_DATASET_STATUS_KEY_ERROR)

            try:
                document = get_document(specific=specific)
                filename = message.get('filename') if timeout else document.get('filename', "")
            except KeyError:
                self.logger.error(f"[Process {dataset_status_key}] Error parsing JSON. No filename defined", exc_info=get_exc_info())
                raise Exception(PARSING_PARAMETERS_ERROR)

            try:
                project_conf = get_project_config(generic=generic)
                url = project_conf.get('url_sender', "")
                timeout_id = project_conf['timeout_id']
            except KeyError:
                self.logger.error(f"[Process {dataset_status_key}] Error parsing JSON. No project config defined", exc_info=get_exc_info())
                raise Exception(PARSING_PARAMETERS_ERROR)

            try:
                self.logger.debug(f"Checking status of process of {dataset_status_key}")
                error = get_status_code(redis_status, dataset_status_key, format_json=True) == ERROR
                self.logger.info(f"Status of process of {dataset_status_key} is an error -> '{error}'")
            except Exception:
                self.logger.error(f"Process {dataset_status_key} not exists in Redis.", exc_info=get_exc_info())
                error = True

            try:
                try:
                    message_to_send = get_value(redis_status, dataset_status_key, format_json=True).get('msg', "No defined message error in Redis")
                    status_code = ERROR if error else PROCESS_FINISHED

                    update_full_status(redis_status, dataset_status_key, status_code, message_to_send)

                    delete_status(redis_timeout, timeout_id)
                except Exception:
                    message_to_send = "No defined message error in Redis"
                    status_code = ERROR if error else PROCESS_FINISHED
                    self.logger.error(f"[Process {dataset_status_key}] Error to read of Redis '{timeout_id}'", exc_info=get_exc_info())

                self.report_end(url, self.compose_message(dataset_status_key, integration_message, message_to_send, filename, status_code, tracking_message))
            except Exception:
                self.logger.error(f"[Process {dataset_status_key}] Error to reporting message to url '{url}'", exc_info=get_exc_info())
                raise Exception(REPORTING_MESSAGE_ERROR)
        except Exception as ex:
            status_code = ERROR
            self.logger.error(ex, exc_info=get_exc_info())
            self.report_end(url, self.compose_message(dataset_status_key, integration_message, str(ex), filename, status_code, tracking_message))
        return self.must_continue, message, self.service_name


if __name__ == "__main__":
    deployment = FlowMgmtCheckEndDeployment()
    deployment.async_deployment()

