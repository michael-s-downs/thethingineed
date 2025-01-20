### This code is property of the GGAO ###


# Native imports
import json
import string
import os
from random import choice
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Tuple, Union

# Installed imports
import pandas as pd
from mergedeep import merge

# Custom imports
from common.deployment_utils import BaseDeployment
from common.genai_controllers import storage_containers, db_dbs, set_queue, set_storage, set_db
from common.genai_controllers import list_files, provider, load_file, write_to_queue, get_dataset
from common.genai_status_control import create_status, update_status
from common.genai_json_parser import get_exc_info, get_dataset_status_key, get_project_config, get_dataset_config, generate_dataset_status_key
from common.services import PREPROCESS_START_SERVICE, PREPROCESS_EXTRACT_SERVICE, PREPROCESS_END_SERVICE
from common.status_codes import ERROR, BEGIN_LIST, END_LIST, BEGIN_DOCUMENT
from common.error_messages import (
    CREATING_STATUS_REDIS_ERROR,
    PARSING_PARAMETERS_ERROR,
    GETTING_DATASET_STATUS_KEY_ERROR,
    CHECKING_FILES_STORAGE_ERROR,
)
from common.utils import convert_service_to_queue


class PreprocessStartDeployment(BaseDeployment):
    def __init__(self):
        """ Creates the deployment"""
        super().__init__()
        set_queue(self.Q_IN)
        set_storage(storage_containers)
        set_db(db_dbs)

        self.json_base = json.loads(load_file(storage_containers['workspace'], "src/layout.json").decode())
        self.sep = ":"
        self.q_preprocess_extract = (provider, convert_service_to_queue(PREPROCESS_EXTRACT_SERVICE, provider))
        self.q_preprocess_end = (provider, convert_service_to_queue(PREPROCESS_END_SERVICE, provider))
        set_queue(self.q_preprocess_extract)

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
        return PREPROCESS_START_SERVICE

    @property
    def max_num_queue(self) -> int:
        """ Max number of messages to read from queue at once """
        return 1

    def get_json_generic(self, json_input: dict, tenant: str, department: str, report_url: str) -> dict:
        """ Get generic params from json_input and create generic key for json output

        :param json_input: Input JSON
        :param tenant: Name of tenant
        :param department: Name of department for process
        :param report_url: Url to report pags, tokens, etc.

        :return: Generic params dict
        """
        generic = deepcopy(self.json_base).get('generic', {})

        process_type = json_input.get('process_type', {})

        project_type = json_input.get('project_type', {})

        dataset_conf = json_input.get('dataset_conf', {})

        if dataset_conf.get('dataset_id', ""):
            process_id = dataset_conf.get('dataset_id', "")
        else:
            process_id = process_type + "_" + datetime.now().strftime("%Y%m%d_%H%M%S_%f_") + "".join([choice(string.ascii_lowercase + string.digits) for _ in range(6)])

        dataset_conf.setdefault('dataset_id', process_id)

        generic['project_conf']['process_id'] = process_id
        generic['project_conf']['timeout_id'] = f"timeout_id_{tenant}:{process_id}"
        generic['project_conf']['process_type'] = process_type
        generic['project_conf']['department'] = department
        generic['project_conf']['report_url'] = report_url
        generic['project_conf']['tenant'] = tenant
        generic['project_conf']['project_type'] = project_type
        generic['project_conf']['url_sender'] = json_input.get('url_sender', "")
        generic['project_conf']['timeout_sender'] = json_input.get('timeout_sender', 5)
        
        generic['dataset_conf'] = dataset_conf

        
        if "csv" in json_input:
            generic['project_conf']['csv'] = json_input.get('csv', False)

        if process_type == "ir_index":
            merge(generic.setdefault('indexation_conf', {}), json_input.get('indexation_conf', {}))
            
        merge(generic.setdefault('preprocess_conf', {}), json_input.get('preprocess_conf', {}))

        return generic

    def get_json_specific(self, generic: dict) -> dict:
        """ Get specific params from json_input and create specific key for json output

        :param generic: Generic dict from json_output

        :return: Specific params dict
        """
        dataset_conf = get_dataset_config(generic=generic)
        project_conf = get_project_config(generic=generic)
        process_id = project_conf.get('process_id')
        dataset_id = dataset_conf.get('dataset_id')
        department = project_conf.get('department')
        base_path = os.path.join(department, dataset_id)

        specific = {
            'path_txt': os.path.join(base_path, "txt"),
            'path_text': os.path.join(base_path, "text"),
            'path_img': os.path.join(base_path, "imgs"),
            'path_cells': os.path.join(base_path, "cells"),
            'path_tables': os.path.join(base_path, "tables"),
            'dataset': {
                'dataset_key': self.sep.join([process_id, dataset_id])
            }

        }

        return specific

    def adapt_input(self, json_input: dict) -> dict:
        """ Transform input JSON in output JSON with generic, specific and integration keys

        :param json_input: Input JSON

        :return: Output JSON to send to next service
        """
        integration = json_input.get('integration', {})
        apigw_params = json_input.get('headers', {})
        tenant = apigw_params.get('x-tenant', "")
        department = apigw_params.get('x-department', "")
        report_url = apigw_params.get('x-reporting', "")

        self.logger.debug("Generating generic configuration or process")
        generic = self.get_json_generic(json_input, tenant, department, report_url)

        self.logger.debug("Generating specific configuration or process")
        specific = self.get_json_specific(generic)

        tracking_request = json_input.get('tracking', {})

        json_output = {
            'generic': generic,
            'specific': specific,
            'integration': integration,
            'tracking': tracking_request
        }

        return json_output

    def get_dataset_files(self, dataset_conf: dict, dataset_status_key: str) -> pd.DataFrame:
        """ Get dataset from dataset_conf or from storage origin

        :param dataset_conf: Dataset configuration dict
        :param dataset_status_key: Id of process

        :return: Dataframe with process dataset
        """
        if "files" in dataset_conf:
            try:
                columns = [
                    dataset_conf.get('path_col', "Url"),
                    dataset_conf.get('label_col', "CategoryId")
                ]
                files = [(file, 1) for file in dataset_conf.get('files', '')]
                df = pd.DataFrame(files, columns=columns)
            except Exception:
                self.logger.debug(f"[Process {dataset_status_key}] Error while getting dataset from files key in json", exc_info=get_exc_info())
                raise Exception()
        else:
            try:
                # Instantiate de pandas DataFrame from remote csv file
                df = get_dataset(origin=storage_containers['origin'], dataset_type="csv", path_name=dataset_conf['dataset_csv_path'])
            except Exception:
                self.logger.debug(f"[Process {dataset_status_key}] Error while getting dataset from cloud storage origin", exc_info=get_exc_info())
                raise Exception()

        return df

    def add_status(self, db_provider: dict, dataset_status_key: str, project_conf: dict, dataset_conf: dict, df: pd.DataFrame, message: dict) -> None:
        """ Add status to json output

        :param db_provider: Credentials of Redis
        :param dataset_status_key: Id of process
        :param project_conf: Project configuration parameters
        :param dataset_conf: Dataset configuration parameters
        :param df: Dataframe of process dataset
        :param message: Json output without status
        """

        try:
            msg = json.dumps({'status': BEGIN_LIST, 'msg': f"Start process {dataset_status_key}"})
            update_status(db_provider['status'], dataset_status_key, msg)
        except Exception:
            self.logger.debug(f"[Process {dataset_status_key}] Error creating dataset status", exc_info=get_exc_info())
            raise Exception()

        # Timeout REDIS key
        timeout_id = project_conf['timeout_id']
        timeout_value = int(project_conf.get('timeout_sender', "30"))
        timeout = datetime.now() + timedelta(minutes=timeout_value)
        timestamp_timeout = timeout.timestamp()
        self.logger.debug(f"Timeout apply {timeout_value}.")
        self.logger.info(f"Process '{dataset_status_key}' expired to {datetime.fromtimestamp(timestamp_timeout)}")

        path_col = dataset_conf.get('path_col', "Url")
        filename = df.iloc[0][path_col]

        json_timeout = json.dumps({
            'timestamp': timestamp_timeout,
            'filename': filename,
            'request_json': message
        })

        try:
            self.logger.info("Creating timeout and counter status for timeout")
            create_status(db_provider['timeout'], timeout_id, json_timeout, None)
        except Exception:
            self.logger.debug(f"[Process {dataset_status_key}] Error creating timeout status", exc_info=get_exc_info())
            raise Exception()

    def process_row(self, row: pd.Series, dataset_status_key: str, redis_status: Union[str, str], dataset_conf: dict, message: dict):
        """ Process row to send and write in next queue

        :param row: Dataset row
        :param dataset_status_key: Id of process
        :param redis_status: Redis to register status
        :param dataset_conf: Dataset configuration parameters
        :param message: JSON to write in next queue
        """
        try:
            prefix = dataset_conf['dataset_path']
            list_IRStorage = [file for file in list_files(storage_containers['origin'], prefix=prefix) if not file.endswith('.csv')]
            self.logger.debug(f"Numbers of documents to process: {len(list_IRStorage)}")

            path_col = dataset_conf.get('path_col', "Url")
            label_col = dataset_conf.get('label_col', "CategoryId")

            document = row[path_col]
            metadata = {meta_col: row[meta_col] for meta_col in row.index if meta_col not in [path_col, label_col]}
            self.logger.info(f"Processing document '{document}' with metadata '{', '.join(list(metadata.keys()))}'")
            # Check if file exists in the origin
            if document in list_IRStorage:
                try:
                    # Create an entry to store status of the document
                    msg = json.dumps({'status': BEGIN_DOCUMENT, 'msg': "Document sent to extract text and images"})
                    update_status(redis_status, dataset_status_key, msg)
                    message['specific']['document'] = {
                        'filename': document,
                        'label': 0,
                        'metadata': metadata
                    }
                    message = self.generate_tracking_message(message, self.service_name, "OUTPUT")
                    write_to_queue(self.q_preprocess_extract, message)
                except Exception:
                    self.logger.debug(f"[Process {dataset_status_key}] Error sending document {document} to text and images extraction", exc_info=get_exc_info())
                    raise Exception()
        except Exception:
            raise Exception()

    def list_documents(self, db_provider: dict, df: pd.DataFrame, dataset_status_key: str, dataset_conf: dict, message: dict, csv_method: bool = False):
        """ Check if documents of dataset are in cloud storage and process dataset rows

        :param db_provider: Credentials of redis
        :param df: Dataframe of process dataset
        :param dataset_status_key: Id of process
        :param dataset_conf: Dataset configuration parameters
        :param message: JSON to write in next queue
        :param csv_method: Flag with type of method to extract text
        """
        try:
            n_docs = len(df)
            if n_docs == 0:
                self.logger.debug(f"[Process {dataset_status_key}] No documents found for the dataset", exc_info=get_exc_info())
                raise Exception()
            else:
                if csv_method:
                    message = self.generate_tracking_message(message, self.service_name, "OUTPUT")
                    write_to_queue(self.q_preprocess_end, message)
                else:
                    try:
                        df.apply(self.process_row, axis=1, dataset_status_key=dataset_status_key, redis_status=db_provider['status'], dataset_conf=dataset_conf, message=message)
                    except Exception:
                        self.logger.debug(f"[Process {dataset_status_key}] Error processing documents", exc_info=get_exc_info())
                        raise Exception()
        except Exception:
            raise Exception()

    def process(self, json_input: dict) -> Tuple[bool, dict, str]:
        """ Main function. Return if the output must be written to next step, the output to write and the next step.
        :return: Tuple[bool, dict, str]
        """
        self.logger.debug(f"Data entry: {json_input}")
        message = self.adapt_input(json_input)
        next_service = PREPROCESS_END_SERVICE
        msg = json.dumps({'status': ERROR, 'msg': "Error in preprocess start"})
        redis_status = db_dbs['status']
        dataset_status_key = generate_dataset_status_key(message)
        must_continue = self.must_continue

        try:
            try:
                project_conf = get_project_config(message)
                dataset_conf = get_dataset_config(message)
            except KeyError:
                self.logger.error(f"[Process {dataset_status_key}] Error getting project and dataset configuration", exc_info=get_exc_info())
                raise Exception(PARSING_PARAMETERS_ERROR)

            try:
                dataset_status_key = get_dataset_status_key(json_input=message)
            except KeyError:
                self.logger.error(f"[Process {dataset_status_key}] Error getting dataset keys", exc_info=get_exc_info())
                raise Exception(GETTING_DATASET_STATUS_KEY_ERROR)

            try:
                self.logger.info(f"Getting dataset files for dataset: '{dataset_conf['dataset_csv_path']}'")
                df = self.get_dataset_files(dataset_conf=dataset_conf, dataset_status_key=dataset_status_key)
            except Exception:
                self.logger.error(f"[Process {dataset_status_key}] Error check files in storage", exc_info=get_exc_info())
                raise Exception(CHECKING_FILES_STORAGE_ERROR)

            try:
                self.logger.info(f"Adding status to dataset: {dataset_status_key}")
                self.add_status(db_provider=db_dbs, dataset_status_key=dataset_status_key, project_conf=project_conf, dataset_conf=dataset_conf, df=df, message=message)
            except Exception:
                self.logger.error(f"[Process {dataset_status_key}] Error creating status of process in Redis", exc_info=get_exc_info())
                raise Exception(CREATING_STATUS_REDIS_ERROR)

            try:
                self.logger.info("Prepare and sending messages to preprocess extract")
                self.list_documents(db_provider=db_dbs, df=df, dataset_status_key=dataset_status_key, dataset_conf=dataset_conf, message=message, csv_method=project_conf.get('csv_method', False))
            except Exception:
                self.logger.error(f"[Process {dataset_status_key}] Error checking files in storage or sending next steep", exc_info=get_exc_info())
                raise Exception(CHECKING_FILES_STORAGE_ERROR)

            msg = json.dumps({'status': END_LIST, 'msg': "All documents were sent to be preprocessed"})
        except Exception as ex:
            dataset_status_key = generate_dataset_status_key(message)
            next_service = PREPROCESS_END_SERVICE
            self.logger.error(f"[Process {dataset_status_key}] Error in preprocess start.", exc_info=get_exc_info())
            must_continue = True
            msg = json.dumps({'status': ERROR, 'msg': str(ex)})

        update_status(redis_status, dataset_status_key, msg)
        return must_continue, message, next_service


if __name__ == "__main__":
    deploy = PreprocessStartDeployment()
    deploy.async_deployment()
