### This code is property of the GGAO ###


# Native imports
import json

# Custom imports
from common.logging_handler import LoggerHandler
from common.genai_sdk_controllers import *
from common.ir import IR_INDICES
from common.services import GENAI_INFO_DELETION_SERVICE
from common.indexing.loaders import ManagerLoader
from common.indexing.connectors import ManagerConnector


class DeletionAPI:

    def __init__(self):
        logger_handler = LoggerHandler(GENAI_INFO_DELETION_SERVICE, level=os.environ.get('LOG_LEVEL', "INFO"))
        self.logger = logger_handler.logger
        self.origin = storage_containers.get('origin')
        self.workspace = storage_containers.get('workspace')
        self.file_loader = ManagerLoader().get_file_storage({"type": "s3", "workspace": self.workspace, "origin": self.origin})
        self.load_secrets()
        self.ir_models = {}

    def load_secrets(self):
        vector_storages_path = os.path.join(os.getenv('SECRETS_PATH', '/secrets'), "vector-storage", "vector_storage_config.json")

        # Load vector storages credentials
        if os.path.exists(vector_storages_path):
            with open(vector_storages_path, "r") as file:
                self.vector_storages = json.load(file).get("vector_storage_supported")
        else:
            raise FileNotFoundError(f"Vector storages file not found {vector_storages_path}.")

    def update_ir_models(self):
        files = list_files(self.workspace, IR_INDICES)
        for file in files:
            self.logger.debug("Loading index: " + str(file))
            if not file.endswith(".json"):
                continue
            index = os.path.splitext(os.path.basename(file))[0]
            state_dict = self.file_loader.load_file(self.workspace, file)
            if state_dict:
                state_dict = json.loads(state_dict)
            else:
                self.logger.error(f"Index {index} is empty.")
                continue

            vector_storage = self.get_vector_storage(state_dict.get('vector_storage'))
            if not vector_storage:
                self.logger.error(f" Vector storage: {state_dict.get('vector_storage')} has no vector storage.")

            self.ir_models[index] = {
                'vector_storage': vector_storage,
                'retrievers': {}
            }

    def get_vector_storage(self, vector_storage_name: str) -> dict:
        vector_storage = None
        for vs in self.vector_storages:
            if vs.get("vector_storage_name") == vector_storage_name:
                vector_storage = vs
        return vector_storage

    def check_input(self, json_input):
        """
        Check if the input is correct
        """
        if not isinstance(json_input.get('delete'), dict):
            raise ValueError(f"'delete' key must be a dictionary with the metadata used to delete")
        if not isinstance(json_input.get('index'), str):
            raise ValueError(f"'index' key must be a string with the index name")

    def delete(self, json_input):
        """ Delete based on sent conditions.
        """
        self.logger.info(f"Request recieved with data: {json_input}")
        index = json_input['index']

        if index not in self.ir_models:
            self.update_ir_models()
            if index not in self.ir_models:
                raise ValueError(f"Index conf is not in s3. Please add it in the {IR_INDICES} s3 path")

        vector_storage = self.ir_models[index]['vector_storage']
        connector = ManagerConnector().get_connector(vector_storage)
        connector.connect()
        result = connector.delete_document(index, json_input['delete'])
        if len(result.get('failures', 0)) > 0:
            self.logger.error(f"Error deleting documents: {result}")
            return json.dumps({'status': "error",
                               'result': "Error deleting documents",
                               'status_code': 400}), 400
        elif result.get('deleted', 0) == 0:
            self.logger.error(f"Documents not found for filters: {json_input['delete']}")
            return json.dumps({'status': "error",
                               'result': f"Documents not found for filters: {json_input['delete']}",
                               'status_code': 400}), 400
        else:
            connector.close()
        return json.dumps({'status': "finished", 'result': f"{result['deleted']} chunks deleted", 'status_code': 200}), 200
