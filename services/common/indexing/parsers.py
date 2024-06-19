### This code is property of the GGAO ###


# Native imports
from abc import ABC
from typing import List
import unicodedata
import random
import os

# Installed imports
import langdetect

# Custom imports
from common.dolffia_json_parser import (get_project_config, get_dataset_status_key,
                                        get_do_titles, get_do_tables, get_specific, get_generic,
                                        get_index_conf, get_exc_info)
from common.logging_handler import LoggerHandler
from common.services import PARSERS_SERVICE


class Parser(ABC):
    MODEL_FORMAT = "Connector"

    def __init__(self):
        logger_handler = LoggerHandler(PARSERS_SERVICE, level=os.environ.get('LOG_LEVEL', "INFO"))
        self.logger = logger_handler.logger

    @classmethod
    def is_platform_type(cls, model_type):
        """Checks if a given model type is equel to the model format and thus it must be the one to use.
        """
        return model_type == cls.MODEL_FORMAT


class ParserInfoindexing(Parser):
    OPTIONAL_PARAMS = {
        'windows_overlap': 100,
        'windows_length': 300,
        'modify_index_docs': {}
    }

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
            self.get_url()
            self.get_process_type()
            self.get_process_id()
            self.get_department()
            self.get_csv()
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
            self.get_index()
            self.get_models(available_pools, available_models, models_credentials)

            self.get_vector_storage(vector_storages)
            self.get_windows_overlap()
            self.get_windows_length()
            self.get_modify_index_docs()
        except KeyError as ex:
            self.logger.debug(f"Error getting indexing params for {self.index_conf}",
                              exc_info=get_exc_info())
            self.logger.error(f"[Process {self.dataset_status_key}] Error getting model params.",
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
        self.dataset_csv_path = self.generic['dataset_conf']['dataset_csv_path']

    def get_project_conf(self):
        self.project_conf = get_project_config(generic=self.generic)

    def get_url(self):
        self.url = self.project_conf['report_url']

    def get_process_type(self):
        self.process_type = self.project_conf['process_type']

    def get_process_id(self):
        self.process_id = self.project_conf['process_id']

    def get_department(self):
        self.department = self.project_conf['department']

    def get_csv(self):
        self.csv = self.project_conf.get("csv", False)

    def get_do_titles(self):
        self.do_titles = get_do_titles(generic=self.generic)

    def get_do_tables(self):
        self.do_tables = get_do_tables(generic=self.generic)

    def get_index_conf(self):
        self.index_conf = self.generic['index_conf']

    def get_index(self):
        self.index = self.index_conf['index']

    def get_models(self, available_pools: dict, available_models: dict, models_credentials:dict):
        self.models = self.index_conf['models']
        if not self._models_retrocompatibility():
            model_selected = {}
            for i, model in enumerate(self.models):
                retriever = model.get('retriever')
                embedding_model = model.get('embedding_model')
                alias = model.get('alias')
                # Test if the alias is a pool
                if alias in available_pools.get(retriever, {}).get(embedding_model, []):
                    alias = random.choice(available_pools.get(retriever).get(embedding_model).get(alias).copy())
                # Get the model parameters based on the alias

                for m in available_models.get(retriever, []):
                    if alias == m.get('embedding_model_name'):
                        model_selected = m
                        break
                if not model_selected:
                    raise ValueError(f"Model {model.get('alias')} not found in available models")
                if not model.get('retriever_model'):
                    self.models[i] = {
                        "alias": model.get('alias'),
                        "embedding_model": model.get('embedding_model'),
                        "column_name": model.get('column_name', "_" + str(hash(model['embedding_model']))),
                        "retriever": model.get('retriever'),
                        "api_key": models_credentials[retriever][alias]['api_key'],
                        "azure_api_version": model_selected.get("azure_api_version"),
                        "azure_base_url": models_credentials[retriever][alias]['azure_base_url'],
                        "azure_deployment_name": model_selected.get("azure_deployment_name")
                    }
                else:
                    self.models[i] = {
                        "alias": model.get('alias'),
                        "embedding_model": model.get('embedding_model'),
                        "column_name": model.get('column_name', "_" + str(hash(model['embedding_model']))),
                        "retriever": model.get('retriever'),
                        "retriever_model": model.get('retriever_model'),
                        "similarity": model.get('similarity')
                    }

    def get_vector_storage(self, vector_storages):
        self.vector_storage = self.index_conf.get('vector_storage')
        # Retrocompatibility with older infoindexing calls
        if self.vector_storage is None:
            document_store = self.index_conf.get('document_store')
            if not self._get_storage_name(document_store.get('host'), document_store.get('password'), vector_storages):
                # The vector storage is not in the vector_storages loaded
                self.vector_storage = None
            else:
                self.vector_storage = {
                    "vector_storage_name": self._get_storage_name(document_store.get('host'),
                                                                  document_store.get('password'), vector_storages),
                    "vector_storage_type": document_store.get('docstore_format'),
                    "vector_storage_host": document_store.get('host'),
                    "vector_storage_port": 9200,
                    "vector_storage_username": document_store.get('username'),
                    "vector_storage_password": document_store.get('password')
                }
        else:
            found = False
            for vs in vector_storages:
                if vs.get("vector_storage_name") == self.vector_storage:
                    self.vector_storage = vs
                    found = True
            if not found:
                self.vector_storage = None

    def get_windows_overlap(self):
        self.windows_overlap = self.index_conf.get('windows_overlap', self.OPTIONAL_PARAMS['windows_overlap'])

    def get_windows_length(self):
        self.windows_length = self.index_conf.get('windows_length', self.OPTIONAL_PARAMS['windows_length'])

    def get_modify_index_docs(self):
        self.modify_index_docs = self.index_conf.get('modify_index_docs', self.OPTIONAL_PARAMS['modify_index_docs'])

    def _models_retrocompatibility(self):
        retrocompatibility = False
        for i, model in enumerate(self.models):
            if model.get('index_model'):
                if model.get('api_key'):
                    self.models[i] = {
                        "alias": "ada-002-pool-europe",
                        "embedding_model": model.get('index_model'),
                        "column_name": model.get('embedd'),
                        "retriever": model.get('model_format'),
                        "api_key": model.get('api_key'),
                        "azure_api_version": model.get('azure_api_version'),
                        "azure_base_url": model.get('azure_base_url'),
                        "azure_deployment_name": model.get('azure_deployment_name')
                    }
                else:
                    self.models[i] = {
                        "alias": "dpr-encoder",
                        "embedding_model": model.get('index_model'),
                        "column_name": model.get('embedd'),
                        "retriever": model.get('model_format'),
                        "retriever_model": model.get('retriever_model'),
                        "similarity": model.get('similarity')
                    }
                retrocompatibility = True
        return retrocompatibility

    @staticmethod
    def _get_storage_name(host, password, vector_storages):
        for vs in vector_storages:
            if vs.get('vector_storage_password') == password and host in vs.get('vector_storage_host'):
                return vs.get('vector_storage_name')
        return None


class ParserInforetrieval(Parser):
    OPTIONAL_PARAMS = {
        'windows_overlap': 100,
        'windows_length': 300,
        'modify_index_docs': {}
    }

    MODEL_FORMAT = "inforetrieval"
    @staticmethod
    def _strip_accents(s):
        """Function to delete accents
        """
        chars_origin = "áéíóúÁÉÍÓÚàèìòùÀÈÌÒÙäëïöüÄËÏÖÜâêîôûÂÊÎÔÛ"
        chars_parsed = "aeiouAEIOUaeiouAEIOUaeiouAEIOUaeiouAEIOU"

        return s.translate(str.maketrans(chars_origin, chars_parsed))

    def __init__(self, json_input: dict):
        super().__init__()
        try:
            self.get_generic(json_input)
        except KeyError as ex:
            self.logger.error(f'[Process ] Error parsing JSON. No generic and specific configuration',
                              exc_info=get_exc_info())
            raise ex

        try:
            self.get_index_conf(generic=self.generic)
            self.get_index(self.index_conf)
            self.get_task(self.index_conf)
            self.get_models(self.index_conf)
            self.get_filters(self.index_conf)
            self.get_content_based(self.index_conf)
            self.get_top_k(self.index_conf)
            self.get_top_qa(self.index_conf)
            self.get_clear_cache(self.index_conf)
            self.get_add_highlights(self.index_conf)
            self.get_platform(self.index_conf)
            self.get_rescoring_function(self.index_conf)
            self.get_system(self.index_conf)
            self.get_temperature(self.index_conf)

            assert self.platform in {"azure", "openai"}
            assert self.rescoring_function in {"length", "loglength", "mean", "pos", "posnorm", "norm", "nll"}
        except KeyError as ex:
            self.logger.error(f'Error parsing JSON',
                              exc_info=get_exc_info())
            raise ex

        try:
            self.get_project_config(generic=self.generic)
            self.get_x_reporting(self.project_config)
        except Exception as ex:
            self.logger.error(
                f"[Process] Error parsing JSON. No configuration of project defined. Generic: {self.generic}",
                get_exc_info())
            raise ex

        try:
            self.get_query(self.index_conf)
            self.get_template_name(self.index_conf)
        except KeyError as ex:
            self.logger.error(f' Error getting query',
                              exc_info=get_exc_info())
            raise ex

    def get_generic(self, json_input):
        self.generic = get_generic(json_input)

    def get_index_conf(self, generic):
        self.index_conf = get_index_conf(generic=generic)

    def get_index(self, index_conf):
        self.index = index_conf['index']

    def get_task(self, index_conf):
        self.task = index_conf.get("task")
        if self.task != "retrieve":
            raise ValueError(f"Task {self.task} not supported it must be: \"retrieve\".")

    def get_models(self, index_conf):
        self.models = index_conf.get("models", [])

    def get_filters(self, index_conf):
        self.filters = index_conf.get("filters", {})

    def get_content_based(self, index_conf):
        self.content_based = index_conf.get('content_based', False)

    def get_top_k(self, index_conf):
        top_k = index_conf.get("top_k", 10)
        self.top_k = top_k * 2 if self.content_based else top_k
        pass

    def get_top_qa(self, index_conf):
        self.top_qa = index_conf.get("top_qa", 1)

    def get_clear_cache(self, index_conf):
        self.clear_cache = index_conf.get("clear_cache", False)

    def get_add_highlights(self, index_conf):
        self.add_highlights_bool = index_conf.get("add_highlights", False)

    def get_platform(self, index_conf):
        self.platform = index_conf.get("platform", index_conf.get("call_type", "azure"))

    def get_rescoring_function(self, index_conf):
        self.rescoring_function = index_conf.get("rescoring_function", "mean")

    def get_system(self, index_conf):
        self.system = index_conf.get("system")

    def get_temperature(self, index_conf):
        self.temperature = index_conf.get("temperature")

    def get_project_config(self, generic):
        self.project_config = get_project_config(generic=generic)

    def get_x_reporting(self, project_conf):
        self.x_reporting = project_conf['x-reporting']

    def get_query(self, index_conf):
        self.query = index_conf['query']
        if langdetect.detect(self.query) != 'ja':
            self.query = self._strip_accents(self.query)

    def get_template_name(self, index_conf):
        self.template_name = index_conf.get("template_name", index_conf.get("query_type"))


class ManagerParser(object):
    MODEL_TYPES = [ParserInfoindexing, ParserInforetrieval]

    @staticmethod
    def get_parsed_object(conf:dict) -> Parser:
        """ Method to instantiate the parsers class: [ParserInfoindexing, ParserInforetrieval]

        :param conf: Model configuration. Example:  {"type":"infoindexing", "json_input":{...}}
        """
        for parser in ManagerParser.MODEL_TYPES:
            parser_type = conf.get('type')
            if parser.is_platform_type(parser_type):
                conf.pop('type')
                return parser(**conf)
        raise ValueError(f"Platform type doesnt exist {conf}. "
                         f"Possible values: {ManagerParser.get_possible_platforms()}")

    @staticmethod
    def get_possible_platforms() -> List:
        """ Method to list the parser types: [infoindexing, inforetrieval]

        :param conf: Model configuration. Example:  {"type":"infoindexing","platform":{...}}
        """
        return [store.MODEL_FORMAT for store in ManagerParser.MODEL_TYPES]
