### This code is property of the GGAO ###


# Native imports
from typing import Tuple

# Custom imports
from common.deployment_utils import BaseDeployment
from common.genai_sdk_controllers import storage_containers, db_dbs, set_queue, set_storage, set_db
from common.genai_sdk_controllers import delete_folder
from common.dolffia_status_control import delete_status
from common.dolffia_json_parser import get_exc_info, get_headers, get_dataset_status_key
from common.services import FLOWMGMT_INFODELETE_SERVICE
from common.error_messages import *


class FlowMgmtInfoDeleteDeployment(BaseDeployment):
    def __init__(self):
        """ Creates the deployment"""
        super().__init__()
        set_queue(self.Q_IN)
        set_storage(storage_containers)
        set_db(db_dbs)

    @property
    def must_continue(self) -> bool:
        """ True if the output should be sent to next step """
        return False

    @property
    def service_name(self) -> str:
        """ Service name.
        The name must be the same as the AWS SQS queue name without the Q_ identifier.
        Example: Q_TRAIN_GPU - train_gpu
        It can be in lowercase but must have the same chars.
        The endpoint for sync deployments will be the same as the service name. """
        return FLOWMGMT_INFODELETE_SERVICE

    @property
    def max_num_queue(self) -> int:
        """ Max number of messages to read from queue at once """
        return 1

    def process(self, json_input: dict) -> Tuple[bool, dict, str]:
        """ Main function. Return if the output must be written to next step, the output to write and the next step.
        :return: Tuple[bool, dict, str]
        """
        self.logger.debug(f"Data entry: {json_input}")
        message = json_input
        try:
            try:
                dataset_status_key = get_dataset_status_key(message)
            except Exception:
                self.logger.error("[Process] Error getting dataset_status_key", exc_info=get_exc_info())
                raise GETTING_DATASET_STATUS_KEY_ERROR

            try:
                headers = get_headers(message)
                department = headers.get('x-department', "")
            except Exception:
                self.logger.error(f"[Process {dataset_status_key}] Error getting department", exc_info=get_exc_info())
                raise HEADERS_ERROR

            try:
                self.logger.info(f"Deleting folder of process {dataset_status_key}")
                process_id, dataset_id = dataset_status_key.split(":")

                delete_folder(storage_containers['workspace'], f"{department}/{process_id}")

                if dataset_id != process_id:
                    delete_folder(storage_containers['workspace'], f"{department}/{dataset_id}")
            except Exception:
                self.logger.error(f"[Process {dataset_status_key}] Error while deleting folder", exc_info=get_exc_info())
                raise DELETING_FOLDER_ERROR

            try:
                self.logger.info(f"Deleting redis status for {dataset_status_key}")
                delete_status(db_dbs['status'], dataset_status_key)
            except Exception:
                self.logger.error(f"[Process {dataset_status_key}] Error while deleting status", exc_info=get_exc_info())
                raise DELETING_STATUS_REDIS_ERROR

        except:
            dataset_status_key = message.get('dataset_status_key', "")
            self.logger.error(f"[Process {dataset_status_key}] Error while deleting data")
        finally:
            return self.must_continue, message, self.service_name


if __name__ == "__main__":
    deploy = FlowMgmtInfoDeleteDeployment()
    deploy.async_deployment()
