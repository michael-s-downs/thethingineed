### This code is property of the GGAO ###


from __future__ import annotations

# Native imports
import json
import os

# Installed imports


# Custom imports
from common.deployment_utils import BaseDeployment
from common.genai_sdk_controllers import upload_object
from common.genai_sdk_controllers import storage_containers, db_dbs, set_queue, set_storage, set_db
from common.services import GENAI_INFO_INDEXING_SERVICE, FLOWMGMT_CHECKEND_SERVICE
from common.ir import INDEX_S3
from common.dolffia_json_parser import get_exc_info, get_specific, get_dataset_status_key
from common.dolffia_status_control import update_full_status
from common.status_codes import PROCESS_FINISHED, ERROR
from common.indexing.connectors import ManagerConnector
from common.indexing.loaders import ManagerLoader
from common.indexing.vector_storages import ManagerVectorDB
from common.indexing.parsers import ManagerParser, Parser


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
            file_loader = ManagerLoader().get_file_storage({'type': "s3", 'workspace': self.workspace, 'origin': self.origin})
            self.available_pools = file_loader.get_available_embeddings_pools()
            self.available_models = file_loader.get_available_models()
            self.load_secrets()
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

    def load_secrets(self):
        models_keys_path = os.path.join(os.getenv('SECRETS_PATH', '/secrets'), "models", "models.json")
        vector_storages_path = os.path.join(os.getenv('SECRETS_PATH', '/secrets'), "vector-storage", "vector_storage_config.json")

        # Load models credentials
        if os.path.exists(models_keys_path):
            with open(models_keys_path, "r") as file:
                self.models_credentials = json.load(file).get("embeddings")
        else:
            raise FileNotFoundError(f"Credentials file not found {models_keys_path}.")

        # Load vector storages credentials
        if os.path.exists(vector_storages_path):
            with open(vector_storages_path, "r") as file:
                self.vector_storages = json.load(file).get("vector_storage_supported")
        else:
            raise FileNotFoundError(f"Vector storages file not found {vector_storages_path}.")

    @staticmethod
    def assert_correct_index_conf(state_dict: dict, input_object: Parser):
        """Raises an error if you try to index with another configuration for an already indexed model

        :param state_dict: state dict
        :param input_object: input object
        """
        input_models = input_object.models
        vector_db = input_object.vector_storage
        if not vector_db:
            raise ValueError(f"Error wrong vector_db credentials")
        if len(state_dict) > 0:
            if vector_db.get('vector_storage_name') != state_dict.get('vector_storage'):
                raise ValueError(f"Error wrong vector_db credentials for {vector_db.get('vector_storage_name')} and {state_dict.get('vector_storage')}")
            # In retrocompatibility mode, vector_db is not None so it will pass the assert
            for model in input_models:
                found = False
                for model_state_dict in state_dict.get('models'):
                    if model_state_dict.get('embedding_model') == model.get('embedding_model') and \
                       model_state_dict.get('column_name') == model.get('column_name') and \
                       model_state_dict.get('retriever') == model.get('retriever'):
                        # If sentence transformers, retriever_model is stored too
                        if model_state_dict.get('retriver') == "sentence_trasnformers" and \
                           model_state_dict.get('retriever_model') == model.get('retriever_model'):
                            found = True
                        else:
                            found = True
                if not found:
                    raise ValueError(f"Error the model {model.get('embedding_model')} is not in the state_dict")

    def write_state_dict(self, state_dict: dict, index: str):
        """ Save configuration to indexes

        :param state_dict: state dict to write
        :param index: Index to write the state dict
        """
        upload_object(self.workspace, json.dumps(state_dict).encode('utf-8'), INDEX_S3(index))

    def process(self, json_input: dict):
        self.logger.info("Starting process")
        self.logger.debug(f"Data entry: {json_input}")
        try:
            input_object = ManagerParser().get_parsed_object({'type': "infoindexing", 'json_input': json_input,
                                                             'available_pools': self.available_pools,
                                                             'available_models': self.available_models,
                                                             'vector_storages'  : self.vector_storages,
                                                             'models_credentials': self.models_credentials})

            self.logger.info(f"Input parsed for index {input_object.index}")

            file_loader = ManagerLoader().get_file_storage({'type': "s3", 'workspace': self.workspace, 'origin': self.origin})
            state_dict = file_loader.get_state_dict(input_object)

            self.assert_correct_index_conf(state_dict, input_object)

            connector = ManagerConnector().get_connector(input_object.vector_storage)
            vector_db = ManagerVectorDB.get_vector_database({'type': "UhiStack", 'connector': connector,
                                                            'workspace': self.workspace, 'origin': self.origin})

            dataframe_file, markdowns_file = file_loader.get_specific_files(input_object)
            #Here the first connection with elasticsearch is made
            docs = vector_db.get_processed_data(input_object, dataframe_file, markdowns_file)
            self.logger.info(f"Connection in {connector.MODEL_FORMAT} was successful and the documents were processed for indexation")

            #TODO get page as metadata and pass it in doc_by_pages
            aux_state_dict, tokens_used = vector_db.index_documents(docs=docs, io=input_object, doc_by_pages=[])

            self.logger.info("Documents have been written correctly")

            for model_tokens in tokens_used:
                for r, p in model_tokens.items():
                    self.report_api(p.get('num'), input_object.dataset_status_key, input_object.url, r, input_object.process_id, p.get('type'))

            if state_dict == {}:
                self.write_state_dict(aux_state_dict, input_object.index)

            status_code = PROCESS_FINISHED
            message = "Indexing finished"
        except Exception as ex:
            self.logger.error(f"{str(ex)} for process: {json_input.get('dataset_status_key')}", exc_info=get_exc_info())

            status_code = ERROR
            message = str(ex)
        finally:
            specific = get_specific(json_input)
            dataset_status_key = get_dataset_status_key(specific=specific)

            update_full_status(self.redis_status, dataset_status_key, status_code, message)
            return self.must_continue, json_input, FLOWMGMT_CHECKEND_SERVICE


if __name__ == "__main__":
    deploy = InfoIndexationDeployment()
    deploy.async_deployment()
