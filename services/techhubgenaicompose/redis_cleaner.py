### This code is property of the GGAO ###


# Native imports
import os
import json
from typing import Tuple
from datetime import datetime

# Custom imports
from common.deployment_utils import BaseDeployment
from common.genai_sdk_controllers import db_dbs
from common.dolffia_status_control import get_redis_pattern, delete_status
from common.dolffia_json_parser import get_exc_info
from common.services import FLOWMGMT_CLEANER_SERVICE


class RedisCleaner(BaseDeployment):

    def __init__(self):
        """ Creates the deployment"""
        super().__init__()
        self.tenant = os.getenv("TENANT")
        self.session_to_remove = []

    @property
    def service_name(self) -> str:
        """ Service name.
        The name must be the same as the AWS SQS queue name without the Q_ identifier.
        Example: Q_TRAIN_GPU - train_gpu
        It can be in lowercase but must have the same chars.
        The endpoint for sync deployments will be the same as the service name. """
        return FLOWMGMT_CLEANER_SERVICE

    @property
    def max_num_queue(self):
        """ Max number of messages to read from queue at once """
        return 1

    def process(self, json_input: dict):
        try:
            self.logger.info("Service redis cleaner started...")
            sessions = get_redis_pattern(origin=db_dbs['session'], pattern=f"session:{self.tenant}:*")

            today = datetime.now()
            time_diff = int(os.getenv('REDIS_SESSION_EXPIRATION_TIME', 48))
            for session in sessions:
                try:
                    last_update = json.loads(session['values'].decode()).get('last_update')
                    if last_update:
                        last_update = datetime.strptime(last_update, "%Y-%m-%d %H:%M:%S")
                        if (today - last_update).total_seconds() / 3600 > time_diff:
                            self.session_to_remove.append(session['key'])
                except Exception as ex:
                    self.logger.info(str(ex), exc_info=get_exc_info())
            if len(self.session_to_remove) > 0:
                for session in self.session_to_remove:
                    delete_status(db_dbs['session'], session)
            self.logger.info("Service redis cleaner finished")
        except Exception:
            self.logger.error("Error cleaning session in Redis", exc_info=get_exc_info())


def run_redis_cleaner():
    cleaner = RedisCleaner()
    cleaner.cron_deployment(int(os.getenv("CRON_TIME")))
