### This code is property of the GGAO ###


from __future__ import annotations

# Native imports
import os

# Installed imports

# Custom imports
from common.deployment_utils import BaseDeployment
from common.genai_controllers import storage_containers, db_dbs, set_queue, set_storage, set_db
from common.services import GENAI_INFO_INDEXING_SERVICE, FLOWMGMT_CHECKEND_SERVICE
from common.utils import load_secrets
from common.genai_json_parser import get_exc_info, get_specific, get_dataset_status_key
from common.genai_status_control import update_full_status
from common.status_codes import PROCESS_FINISHED, ERROR
from common.ir.connectors import ManagerConnector
from common.storage_manager import ManagerStorage
from common.ir.parsers import ManagerParser

from vector_storages import ManagerVectorDB


class InfoIndexationDeployment(BaseDeployment):
    def __init__(self):
        """ Creates the deployment"""
        super().__init__()
        set_queue(self.Q_IN)
        set_storage(storage_containers)
        set_db(db_dbs)

        try:
            self.origin = storage_containers.get('origin')
            self.workspace = storage_containers.get('workspace')
            self.redis_status = db_dbs.get('status')
            file_loader = ManagerStorage().get_file_storage(
                {'type': "IRStorage", 'workspace': self.workspace, 'origin': self.origin})
            self.available_pools = file_loader.get_pools_per_embedding_model()
            self.available_models = file_loader.get_available_embedding_models()
            self.all_models = file_loader.get_unique_embedding_models()
            self.models_credentials, self.vector_storages, self.aws_credentials = load_secrets()
            self.logger.info("---- Infoindexing initialized")
        except Exception as ex:
            self.logger.error(f"Error loading {str(ex)}", exc_info=get_exc_info())

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
        return GENAI_INFO_INDEXING_SERVICE

    @property
    def max_num_queue(self) -> int:
        """ Max number of messages to read from queue at once """
        return 1

    def process(self, json_input: dict):
        self.logger.debug(f"Data entry: {json_input}")
        try:
            input_object = ManagerParser().get_parsed_object({'type': "infoindexing", 'json_input': json_input,
                                                              'available_pools': self.available_pools,
                                                              'available_models': self.available_models,
                                                              'vector_storages': self.vector_storages,
                                                              'models_credentials': self.models_credentials})

            self.logger.info(f"Input parsed for index {input_object.index}")

            if eval(os.getenv('TESTING', "False")):
                file_loader = ManagerStorage().get_file_storage(
                    {'type': "IRStorage", 'workspace': self.origin, 'origin': self.origin})
            else:
                file_loader = ManagerStorage().get_file_storage(
                    {'type': "IRStorage", 'workspace': self.workspace, 'origin': self.origin})

            self.connector = ManagerConnector().get_connector(input_object.vector_storage)
            self.connector.connect()

            # check if the models used are the same
            self.connector.assert_correct_index_conf(input_object.index, input_object.chunking_method['method'], self.all_models, input_object.models)
            vector_db = ManagerVectorDB.get_vector_database({'type': "LlamaIndex", 'connector': self.connector,
                                                             'workspace': self.workspace, 'origin': self.origin,
                                                             'aws_credentials': self.aws_credentials})

            dataframe_file, markdowns_file = file_loader.get_specific_files(input_object)
            # Here the first connection with the connector is made
            docs = vector_db.get_processed_data(input_object, dataframe_file, markdowns_file)
            self.logger.info(
                f"Connection in {self.connector.MODEL_FORMAT} was successful and the documents were processed for indexation")

            tokens_used = vector_db.index_documents(docs=docs, io=input_object)

            self.logger.info("Documents have been written correctly")

            if not eval(os.getenv('TESTING', "False")):
                for model_tokens in tokens_used:
                    for r, p in model_tokens.items():
                        self.report_api(p.get('num'), input_object.dataset_status_key, input_object.url, r,
                                        input_object.process_id, p.get('type'))

            status_code = PROCESS_FINISHED
            message = "Indexing finished"
        except Exception as ex:
            self.logger.error(f"{str(ex)} for process: {json_input.get('dataset_status_key')}", exc_info=get_exc_info())

            status_code = ERROR
            message = str(ex)

        specific = get_specific(json_input)
        dataset_status_key = get_dataset_status_key(specific=specific)
        if hasattr(self, "connector") and self.connector:
            self.connector.close()

        update_full_status(self.redis_status, dataset_status_key, status_code, message)
        return self.must_continue, json_input, FLOWMGMT_CHECKEND_SERVICE


if __name__ == "__main__":
    deploy = InfoIndexationDeployment()
    deploy.async_deployment()
