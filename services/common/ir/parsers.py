### This code is property of the GGAO ###


# Native imports
from abc import ABC
from typing import List
import random
import os
from string import Template

# Installed imports
import langdetect
from llama_index.core.retrievers.fusion_retriever import FUSION_MODES

# Custom imports
from common.genai_json_parser import (get_project_config, get_dataset_status_key,
                                      get_do_titles, get_do_tables, get_specific, get_generic, get_exc_info)
from common.logging_handler import LoggerHandler
from common.services import PARSERS_SERVICE
from common.ir.validations import is_available_metadata
from common.errors.genaierrors import PrintableGenaiError


class Parser(ABC):
    MODEL_FORMAT = "Parser"

    def __init__(self):
        logger_handler = LoggerHandler(PARSERS_SERVICE, level=os.environ.get('LOG_LEVEL', "INFO"))
        self.logger = logger_handler.logger

    @classmethod
    def is_parser_type(cls, model_type):
        """Checks if a given model type is equel to the model format and thus it must be the one to use.
        """
        return model_type == cls.MODEL_FORMAT

    @staticmethod
    def get_embedding_model_data(platform, alias, models_credentials, model_selected):
        """Returns the model data in a dictionary format.
        """
        if platform == "azure":
            template = Template(models_credentials.get('URLs').get('AZURE_EMBEDDINGS_URL'))
            return {
                "alias": alias,
                "platform": platform,   
                "embedding_model": model_selected.get('embedding_model'),
                "api_key": models_credentials['api-keys'][platform][model_selected.get('zone')],
                "azure_api_version": model_selected.get("azure_api_version"),
                "azure_base_url": template.safe_substitute(ZONE=model_selected.get('zone').lower()),
                "azure_deployment_name": model_selected.get("azure_deployment_name")
            }
        elif platform == "bedrock":
            return {
                "alias": alias,
                "platform": platform,
                "embedding_model": model_selected.get('embedding_model'),
                "region": model_selected.get("zone")
            }
        elif platform == "huggingface" or platform == "similarity":
            return {
                "alias": alias,
                "platform": "huggingface",  # harcoded for retrocompatibility
                "embedding_model": model_selected.get('embedding_model'),
                "retriever_model": model_selected.get('retriever_model')
            }
        else:
            raise PrintableGenaiError(400, f"Platform {platform} not supported")


class ParserInfoindexing(Parser):
    OPTIONAL_PARAMS = {
        'method': "simple",
        'window_overlap': 10,
        'window_length': 300,
        'windows': 1,
        'sub_window_overlap': 5,
        'sub_window_length': 100
    }

    INDEXING_MODES = ["simple", "recursive", "surrounding_context_window"]

    MODEL_FORMAT = "infoindexing"

    def __init__(self, json_input: dict, available_pools: dict, available_models: dict, vector_storages: dict,
                 models_credentials: dict):
        super().__init__()
        try:
            self.get_generic(json_input)
            self.get_specific(json_input)
            self.get_dataset_status_key()
            self.get_txt_path()
        except KeyError as ex:
            self.logger.error("Error parsing JSON. No generic and specific configuration.",
                              exc_info=get_exc_info())
            raise ex
        try:
            self.get_dataset_csv_path()
        except KeyError as ex:
            self.logger.debug("Error parsing JSON.", exc_info=get_exc_info())
            self.logger.error(f"[Process {self.dataset_status_key}] Error parsing JSON.", exc_info=get_exc_info())

            raise ex

        try:
            self.get_project_conf()
        except KeyError as ex:
            self.logger.debug("Error parsing JSON. No configuration of project defined.",
                              exc_info=get_exc_info())
            self.logger.error(
                f"[Process {self.dataset_status_key}] Error parsing JSON. No configuration of project defined.",
                exc_info=get_exc_info())
            raise ex

        try:
            self.get_do_titles()
            self.get_do_tables()
        except KeyError as ex:
            self.logger.debug("Error parsing JSON.", exc_info=get_exc_info())
            self.logger.error(f"[Process {self.dataset_status_key}] Error parsing JSON.",
                              exc_info=get_exc_info())
            raise ex

        try:
            self.get_index_conf()
            self.get_chunking_method()
            self.get_metadata()
            self.get_index_metadata()
            self.get_vector_storage_conf(vector_storages)
            self.get_models(available_pools, available_models, models_credentials)
        except KeyError as ex:
            self.logger.debug(f"Error getting indexing params for {self.index_conf}",
                              exc_info=get_exc_info())
            self.logger.error(f"[Process {self.dataset_status_key}] Error getting indexation params.",
                              exc_info=get_exc_info())
            raise ex

    def get_generic(self, input:dict):
        self.generic = get_generic(input)

    def get_specific(self, input:dict):
        self.specific = get_specific(input)

    def get_dataset_status_key(self):
        self.dataset_status_key = get_dataset_status_key(specific=self.specific)

    def get_txt_path(self):
        self.txt_path = self.specific.get('paths', {}).get('text', "")

    def get_dataset_csv_path(self):
        self.dataset_csv_path = self.generic.get('dataset_conf', {}).get('dataset_csv_path')

    def get_project_conf(self):
        project_conf = get_project_config(generic=self.generic)
        self.url = project_conf['report_url']
        self.process_type = project_conf['process_type']
        self.process_id = project_conf['process_id']
        self.department = project_conf['department']
        self.csv = project_conf.get("csv", False)

    def get_do_titles(self):
        self.do_titles = get_do_titles(generic=self.generic)

    def get_do_tables(self):
        self.do_tables = get_do_tables(generic=self.generic)

    def get_index_conf(self):
        self.index_conf = self.generic['indexation_conf']

    def get_vector_storage_conf(self, vector_storages):
        vector_storage_conf = self.index_conf['vector_storage_conf']
        self.index = vector_storage_conf['index']
        self.metadata_primary_keys = self.get_metadata_primary_keys(vector_storage_conf)
        self.vector_storage = self.get_vector_storage(vector_storages, vector_storage_conf)

    def get_metadata_primary_keys(self, vector_storage_conf):
        metadata_primary_keys = vector_storage_conf.get('metadata_primary_keys')
        if not isinstance(metadata_primary_keys, list) and metadata_primary_keys:
            raise PrintableGenaiError(400, "'metadata_primary_keys' must be a list")
        if metadata_primary_keys:
            for key in metadata_primary_keys:
                if not is_available_metadata(self.metadata, key, self.chunking_method.get('method')):
                    raise PrintableGenaiError(f"The 'metadata_primary_keys' key ({key}) does not appear in the passed metadata or in the mandatory metadata for the chunking method '{self.chunking_method.get('method')}'", 400)
            metadata_primary_keys = sorted(metadata_primary_keys)
        return metadata_primary_keys

    @staticmethod
    def get_vector_storage(vector_storages, vector_storage_conf):
        vector_storage = vector_storage_conf.get('vector_storage')
        for vs in vector_storages:
            if vs.get("vector_storage_name") == vector_storage:
                return vs
        raise PrintableGenaiError(400, f"Vector storage {vector_storage} not available")

    def get_chunking_method(self):
        self.chunking_method = self.index_conf.get('chunking_method', {})
        if self.chunking_method.setdefault('method', self.OPTIONAL_PARAMS['method']) in self.INDEXING_MODES:
            # If exists get the value if not, set the default value
            self.chunking_method.setdefault('window_overlap', self.OPTIONAL_PARAMS['window_overlap'])
            self.chunking_method.setdefault('window_length', self.OPTIONAL_PARAMS['window_length'])
            if self.chunking_method['method'] == "recursive":
                self.chunking_method.setdefault('sub_window_overlap', self.OPTIONAL_PARAMS['sub_window_overlap'])
                self.chunking_method.setdefault('sub_window_length', self.OPTIONAL_PARAMS['sub_window_length'])
            elif self.chunking_method['method'] == "surrounding_context_window":
                self.chunking_method.setdefault('windows', self.OPTIONAL_PARAMS['windows'])
        else:
            raise PrintableGenaiError(400, f"Chunking mode {self.chunking_method.get('method')} not available")

    def get_metadata(self):
        self.metadata = self.index_conf.get('metadata', {})

    def get_models(self, available_pools: dict, available_models: dict, models_credentials: dict):
        self.models = self.index_conf['models']
        for i, model in enumerate(self.models):
            model_selected = {}
            platform = model.get('platform')
            alias = model.get('alias')
            embedding_model = model.get('embedding_model')
            # Test if the alias is a pool
            if alias in available_pools.get(platform, {}).get(embedding_model, []):
                alias = random.choice(available_pools.get(platform).get(embedding_model).get(alias).copy())
                self.logger.debug(f"Model selected from pool: {alias}")
            # Get the model parameters based on the alias
            for m in available_models.get(platform, []):
                if alias == m.get('embedding_model_name'):
                    model_selected = m
                    break
            if not model_selected:
                raise PrintableGenaiError(400, f"Model {model.get('alias')} not found in available models")
            self.models[i] = self.get_embedding_model_data(platform, alias, models_credentials, model_selected)

        unique_embedding_models = []
        for model in self.models:
            if model.get('embedding_model') in unique_embedding_models:
                raise PrintableGenaiError(400, f"Model '{model.get('embedding_model')}' duplicated")
            unique_embedding_models.append(model.get('embedding_model'))

    def get_index_metadata(self):
        index_metadata = self.index_conf.get('index_metadata')
        if not (isinstance(index_metadata, list) or isinstance(index_metadata, bool)) and index_metadata:
            raise PrintableGenaiError(400, "'index_metadata' must be a list or a boolean")
        if isinstance(index_metadata, list):
            for key in index_metadata:
                if not is_available_metadata(self.metadata, key, self.chunking_method.get('method')):
                    raise PrintableGenaiError(f"The 'index_metadata' key ({key}) does not appear in the passed metadata or in the mandatory metadata for the chunking method '{self.chunking_method.get('method')}'", 400)
        self.index_metadata = index_metadata




class ParserInforetrieval(Parser):
    GENAI_STRATEGIES = ["genai_retrieval", "recursive_genai_retrieval", "surrounding_genai_retrieval"]
    LLAMAINDEX_STRATEGIES = ["llamaindex_fusion"]
    AVAILABLE_STRATEGIES = GENAI_STRATEGIES + LLAMAINDEX_STRATEGIES
    AVAILABLE_RESCORING_FUNCTIONS = ["mean", "length", "loglength", "pos", "posnorm", "norm", "nll", "rrf"]

    MODEL_FORMAT = "inforetrieval"
    @staticmethod
    def _strip_accents(s):
        """Function to delete accents
        """
        chars_origin = "áéíóúÁÉÍÓÚàèìòùÀÈÌÒÙäëïöüÄËÏÖÜâêîôûÂÊÎÔÛ"
        chars_parsed = "aeiouAEIOUaeiouAEIOUaeiouAEIOUaeiouAEIOU"

        return s.translate(str.maketrans(chars_origin, chars_parsed))

    def __init__(self, json_input: dict, available_pools: dict, available_models: dict, models_credentials: dict):
        super().__init__()

        try:
            if json_input.get("generic"):
                # Retrocompatibility with older inforetrieval calls
                self.index_conf = json_input["generic"].get("index_conf")
            else:
                self.index_conf = json_input["indexation_conf"]

            self.get_index(self.index_conf)
            self.get_filters(self.index_conf)
            self.get_strategy(self.index_conf)
            self.get_strategy_mode(self.index_conf)
            self.get_top_k(self.index_conf)
            self.get_rescoring_function(self.index_conf)
            self.get_models(self.index_conf, available_pools, available_models, models_credentials)
            self.get_query(self.index_conf)

            self.get_x_reporting(json_input['project_conf'])
        except KeyError as ex:
            raise PrintableGenaiError(400, f"Key '{ex.args[0]}' not found parsing input JSON")

        except Exception as ex:
            raise PrintableGenaiError(400, f"Exception '{ex}' while parsing input JSON")


    def get_index(self, index_conf):
        self.index = index_conf['index']


    def get_models(self, index_conf, available_pools, available_models, models_credentials):
        sent_models = index_conf.get("models", [])
        if len(sent_models) > 0:
            self.models = self.get_sent_models(sent_models, available_pools, available_models, models_credentials)
            unique_embedding_models = []
            for model in self.models:
                if model.get('embedding_model') in unique_embedding_models:
                    raise PrintableGenaiError(400, f"Model '{model.get('embedding_model')}' duplicated")
                unique_embedding_models.append(model.get('embedding_model'))
        else:
            self.models = []


    def get_filters(self, index_conf):
        self.filters = index_conf.get("filters", {})

    def get_strategy(self, index_conf):
        self.strategy = index_conf.get("strategy", "genai_retrieval")
        if self.strategy not in self.AVAILABLE_STRATEGIES:
            raise PrintableGenaiError(400, f"Strategy '{self.strategy}' not supported, the available ones are {self.AVAILABLE_STRATEGIES}")

    def get_strategy_mode(self, index_conf):
        if self.strategy in self.GENAI_STRATEGIES and "strategy_mode" in index_conf:
            raise PrintableGenaiError(400, f"Strategy '{self.strategy}' does not use 'strategy_mode' parameter, use one in '{self.LLAMAINDEX_STRATEGIES}' instead")
        strategy_mode = index_conf.get("strategy_mode", "reciprocal_rerank")
        if strategy_mode not in FUSION_MODES._value2member_map_:
            raise PrintableGenaiError(400, f"Strategy mode '{strategy_mode}' not implemented, try one of {[i.value for i in FUSION_MODES]}")
        self.strategy_mode = strategy_mode


    def get_top_k(self, index_conf):
        self.top_k = index_conf.get("top_k", 10)


    def get_rescoring_function(self, index_conf):
        if self.strategy in self.LLAMAINDEX_STRATEGIES and "rescoring_function" in index_conf:
            raise PrintableGenaiError(400, f"Strategy '{self.strategy}' does not use 'rescoring_function' parameter, use one in '{self.GENAI_STRATEGIES}' instead")
        self.rescoring_function = index_conf.get("rescoring_function", "mean")
        if self.rescoring_function not in self.AVAILABLE_RESCORING_FUNCTIONS:
            raise PrintableGenaiError(400, f"Rescoring function '{self.rescoring_function}' not supported, the available ones are {self.AVAILABLE_RESCORING_FUNCTIONS}")


    def get_x_reporting(self, project_conf):
        self.x_reporting = project_conf['x-reporting']

    def get_query(self, index_conf):
        self.query = index_conf['query']
        if langdetect.detect(self.query) != 'ja':
            self.query = self._strip_accents(self.query)

    def get_sent_models(self, sent_models, available_pools, available_models, models_credentials) -> list:
        """ Method to get the models to use in the retrieval process

        :param sent_models: List of models
        :param available_pools: Available pools
        :param available_models: Available models
        :param models_credentials: Models credentials

        :return: List of models
        """
        models = []
        for model in sent_models:
            if model == "bm25":
                models.append({
                    "alias": "bm25",
                    "embedding_model": "bm25"
                })
                continue
            model_selected = {}
            if model in available_pools:
                alias = random.choice(available_pools[model])
                self.logger.debug(f"Model selected from pool: {alias}")
            else:
                alias = model
            for m in available_models:
                if alias == m.get('embedding_model_name'):
                    model_selected = m
                    break
            if not model_selected:
                raise PrintableGenaiError(400, f"Model {alias} not found in available embedding models")
            platform = model_selected.get('platform')
            models.append(self.get_embedding_model_data(platform, alias, models_credentials, model_selected))
        return models

class ManagerParser(object):
    MODEL_TYPES = [ParserInfoindexing, ParserInforetrieval]

    @staticmethod
    def get_parsed_object(conf:dict) -> Parser:
        """ Method to instantiate the parsers class: [ParserInfoindexing, ParserInforetrieval]

        :param conf: Model configuration. Example:  {"type":"infoindexing", "json_input":{...}}
        """
        for parser in ManagerParser.MODEL_TYPES:
            parser_type = conf.get('type')
            if parser.is_parser_type(parser_type):
                conf.pop('type')
                return parser(**conf)
        raise PrintableGenaiError(400, f"Platform type doesnt exist {conf}. "
                         f"Possible values: {ManagerParser.get_possible_parsers()}")

    @staticmethod
    def get_possible_parsers() -> List:
        """ Method to list the parser types: [infoindexing, inforetrieval]

        :param conf: Model configuration. Example:  {"type":"infoindexing","platform":{...}}
        """
        return [store.MODEL_FORMAT for store in ManagerParser.MODEL_TYPES]
