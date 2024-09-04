### This code is property of the GGAO ###


# Native imports
import os
import json
import datetime
from typing import Tuple

# Custom imports
from common.deployment_utils import BaseDeployment
from common.genai_controllers import db_dbs, set_queue, set_db
from common.genai_controllers import provider, write_to_queue
from common.genai_status_control import update_status, get_redis_pattern, delete_status
from common.genai_json_parser import get_dataset_status_key, get_exc_info
from common.services import FLOWMGMT_CHECKTIMEOUT_SERVICE, FLOWMGMT_CHECKEND_SERVICE
from common.status_codes import ERROR
from common.utils import convert_service_to_queue


class FlowMgmtCheckTimeoutDeployment(BaseDeployment):
    def __init__(self):
        """ Creates the deployment"""
        super().__init__()
        set_db(db_dbs)

        self.q_flowmgmt_checkend = (provider, convert_service_to_queue(FLOWMGMT_CHECKEND_SERVICE, provider))
        set_queue(self.q_flowmgmt_checkend)
        self.tenant = os.getenv("TENANT")
        self.senders = []

    @property
    def must_continue(self) -> bool:
        """ True if the output should be sent to next step """
        return True

    @property
    def service_name(self) -> str:
        """ Service name.
        The name must be the same as the AWS SQS queue name without the Q_ identifier.
        Example: Q_TRAIN_GPU - train_gpu
        It can be in lowercase but must have the same chars.
        The endpoint for sync deployments will be the same as the service name. """
        return FLOWMGMT_CHECKTIMEOUT_SERVICE

    @property
    def max_num_queue(self):
        """ Max number of messages to read from queue at once """
        return 1

    def get_expired(self) -> list:
        """ Check timeout by tenant of process

        :return: Processes that timeout expired
        """
        self.logger.debug("Checking timeouts.")
        expired = []
        match = f"timeout_id_{self.tenant}:*"
        items = get_redis_pattern(db_dbs['timeout'], match)
        self.logger.debug(f"{len(items)} keys retrieved.")
        for item in items:
            key = item['key'].decode()
            value = json.loads(item['values'].decode())
            if datetime.datetime.fromtimestamp(value['timestamp']) < datetime.datetime.now():
                self.logger.info(f"{key} expired.")
                expired.append((key, value['filename'], value.get('request_json', {})))

        return expired

    def remove_expired(self, expired: list):
        """ Remove of Redis the processes that are expired

        :param: Keys of processes
        """
        self.logger.info(f"Removing {len(expired)} keys")
        if expired:
            for e in expired:
                delete_status(db_dbs['timeout'], e[0])

    def return_expired(self, expired: list):
        """ Create message and write in queue process to exceeded timeout

        :param expired: Processed expired
        """
        for key, filename, request_json in expired:
            message = {
                'type': "timeout",
                'filename': filename,
                'request_json': request_json
            }

            dataset_status_key = get_dataset_status_key(json_input=request_json)
            msg = json.dumps({'status': ERROR, 'msg': "Timeout expired"})

            update_status(db_dbs['status'], dataset_status_key, msg)

            write_to_queue(self.q_flowmgmt_checkend, message)
            self.logger.info(f"Write to queue '{FLOWMGMT_CHECKEND_SERVICE}' message of process {dataset_status_key}")

    def process(self, json_input: dict):
        """ Main function. Return if the output must be written to next step, the output to write and the next step.
        :return: Tuple[bool, dict, str]
        """
        try:
            expired = self.get_expired()
            self.remove_expired(expired)
            self.return_expired(expired)
        except Exception:
            self.logger.error("Error checking timeouts in Redis", exc_info=get_exc_info())


if __name__ == "__main__":
    deploy = FlowMgmtCheckTimeoutDeployment()
    deploy.cron_deployment(int(os.getenv("CRON_TIME")))
