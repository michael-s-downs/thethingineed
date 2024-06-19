### This code is property of the GGAO ###


# Native imports
import json
from typing import Tuple

# Custom imports
from common.deployment_utils import BaseDeployment
from common.genai_sdk_controllers import storage_containers, db_dbs, set_queue, set_storage, set_db
from common.dolffia_status_control import get_status_code, update_status, get_value
from common.dolffia_json_parser import get_generic, get_specific, get_exc_info, get_dataset_status_key, get_project_config
from common.services import PREPROCESS_END_SERVICE, GENAI_INFO_INDEXING_SERVICE, FLOWMGMT_CHECKEND_SERVICE
from common.status_codes import ERROR, START_PROCESS
from common.error_messages import *


class PreprocessEndDeployment(BaseDeployment):
    def __init__(self):
        """ Creates the deployment"""
        super().__init__()
        set_queue(self.Q_IN)
        set_storage(storage_containers)
        set_db(db_dbs)

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
        return PREPROCESS_END_SERVICE

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
        next_service = GENAI_INFO_INDEXING_SERVICE
        msg = json.dumps({'status': ERROR, 'msg': "Error while check status of process"})
        redis_status = db_dbs['status']
        dataset_status_key = get_dataset_status_key(json_input=json_input)
        try:
            try:
                generic = get_generic(json_input)
                specific = get_specific(json_input)
            except Exception:
                self.logger.error(f"[Process] Error getting generic and specific configurations", exc_info=get_exc_info())
                raise Exception(PARSING_PARAMETERS_ERROR)

            try:
                dataset_status_key = get_dataset_status_key(specific=specific)
            except Exception:
                self.logger.error("[Process] Error getting dataset keys", exc_info=get_exc_info())
                raise Exception(GETTING_DATASET_STATUS_KEY_ERROR)

            try:
                redis_status = db_dbs['status']
            except KeyError:
                self.logger.error(f"[Process {dataset_status_key}] Error getting redis configuration", exc_info=get_exc_info())
                raise Exception(GETTING_REDIS_CONFIGURATION_ERROR)

            try:
                project_conf = get_project_config(generic=generic)
                process_type = project_conf['process_type']
            except Exception:
                self.logger.error(f"[Process {dataset_status_key}] Error getting process type from project configuration", exc_info=get_exc_info())
                raise Exception(PARSING_PARAMETERS_ERROR)

            try:
                status = int(get_status_code(redis_status, dataset_status_key, format_json=True))
                self.logger.info(f"[Process {dataset_status_key}] Status: {status}")
            except Exception:
                self.logger.error(f"[Process {dataset_status_key}] Error getting status for {dataset_status_key}.", exc_info=get_exc_info())
                raise Exception(GETTING_REDIS_STATUS_ERROR)

            if status != ERROR:
                self.logger.info(f"Checking status of process type '{process_type}'")
                if process_type == "preprocess":
                    next_service = FLOWMGMT_CHECKEND_SERVICE
                elif process_type == "ir_index":
                    next_service = GENAI_INFO_INDEXING_SERVICE

                msg = json.dumps({'status': START_PROCESS, 'msg': "Preprocess finished. Start indexing"})
            else:
                error_msg = get_value(redis_status, dataset_status_key, format_json=True).get('msg', PREPROCESS_ERROR)
                raise Exception(error_msg)
        except Exception as ex:
            dataset_status_key = get_dataset_status_key(json_input=json_input)
            next_service = FLOWMGMT_CHECKEND_SERVICE
            self.logger.error(f"[Process {dataset_status_key}] Error in preprocess end.", exc_info=get_exc_info())
            msg = json.dumps({'status': ERROR, 'msg': str(ex)})
        finally:
            update_status(redis_status, dataset_status_key, msg)
            return self.must_continue, message, next_service


if __name__ == "__main__":
    deploy = PreprocessEndDeployment()
    deploy.async_deployment()
