### This code is property of the GGAO ###


# Native imports
from abc import ABC
from typing import List
import json
import os
import time

# Installed imports
import pandas as pd

# Custom imports
from common.genai_controllers import load_file, get_dataset, list_files, upload_object, delete_file
from common.logging_handler import LoggerHandler
from common.errors.genaierrors import PrintableGenaiError


class BaseStorageManager(ABC):
    MODEL_FORMAT = "BaseStorageManager"

    def __init__(self, workspace, origin):
        logger_handler = LoggerHandler(self.MODEL_FORMAT, level=os.environ.get('LOG_LEVEL', "INFO"))
        self.logger = logger_handler.logger
        self.workspace = workspace
        self.origin = origin

    def load_file(self, origin, file):
        """ Loads a file from S3

        :param origin: Bucket to load file from
        :param file: Filename to load
        """
        try:
            loaded_file = load_file(origin, file)
        except Exception as ex:
            return None
        return loaded_file if len(loaded_file) > 0 else None

    def get_specific_files(self, input_object):
        """ Gets the files associated to an index

        :param input_object: Inital configuration
        :param workspace, origin: configuration from S3
        """
        pass

    def get_pools_per_embedding_model(self):
        """ Get the embedding pools that can be used  """
        pass

    def get_available_embedding_models(self, inforetrieval_mode: bool = False):
        """ Get the models that can be used """
        pass

    def get_unique_embedding_models(self):
        """ Get all the embeddings models that can be used"""
        pass

    def get_embedding_equivalences(self):
        """ Get the equivalences for the default embeddings models """
        pass

    def get_available_models(self):
        """ Get the models that can be used """
        pass

    def get_available_pools(self):
        """ Get the pools that can be used """
        pass

    def get_templates(self):
        """ Load templates from LLMStorage """
        pass

    @classmethod
    def is_file_storage_type(cls, model_type):
        """Checks if a given model type is equel to the model format and thus it must be the one to use.
        """
        return model_type == cls.MODEL_FORMAT

    def upload_template(self, dat: dict):
        """ Uploads a template to the storage

        :param dat: dict with the template name and content
        """
        pass

    def delete_template(self, dat: dict):
        """ Delete a template from the storage

        :param dat: dict with the template name
        """
        pass


class LLMStorageManager(BaseStorageManager):
    MODEL_FORMAT = "LLMStorage"

    def __init__(self, workspace, origin):
        super().__init__(workspace, origin)
        self.prompts_path = "src/LLM/prompts/"
        self.models_file_path = "src/LLM/conf/models_config.json"
        if not self.load_file(self.workspace, self.models_file_path):
            self.models_file_path = "src/compose/conf/models_config.json"

    def get_available_models(self):
        s3_models_file = self.load_file(self.workspace, self.models_file_path)
        if s3_models_file is None or len(s3_models_file) <= 0:
            raise PrintableGenaiError(400, f"Models can't be downloaded because {self.models_file_path} not found in {self.workspace}")
        else:
            if json.loads(s3_models_file).get("LLMs"):
                return json.loads(s3_models_file).get("LLMs")
            else:
                raise PrintableGenaiError(400, f"Models can't be loaded, maybe the models_config.json is wrong")

    def get_available_pools(self):
        s3_models_file = self.load_file(self.workspace, self.models_file_path)
        if s3_models_file is None or len(s3_models_file) <= 0:
            raise PrintableGenaiError(400, f"Pools can't be downloaded because {self.models_file_path} not found in {self.workspace}")
        else:
            available_pools = {}
            for key, value in json.loads(s3_models_file).get("LLMs").items():
                for model in value:
                    pools = model.get("model_pool", [])
                    for pool in pools:
                        if pool not in available_pools:
                            available_pools[pool] = []
                        if model not in available_pools[pool]:
                            available_pools[pool].append(model)
            if len(available_pools) == 0:
                raise PrintableGenaiError(400, f"Pools were not loaded, maybe the models_config.json is wrong")
            # Convert the set in lists
            for key, value in available_pools.items():
                available_pools[key] = list(value)
            return available_pools

    def get_templates(self):
        """ Load templates from LLMStorage """
        templates = {}
        for file in list_files(self.workspace, self.prompts_path):
            if file.endswith(".json"):
                try:
                    aux_dict = json.loads(self.load_file(self.workspace, file))
                    for key in templates:
                        if key in ".json":
                            raise KeyError(f"Two create query jsons cannot have the same key {key}.")
                    templates.update(aux_dict)
                except:
                    self.logger.warning(f"Malformed json file not loaded: {file}")

        return templates, list(templates.keys())

    def upload_template(self, dat: dict):
        try:
            template_name = dat['name']
            content = dat['content']

            upload_object(self.workspace, content, self.prompts_path + template_name + ".json")
            time.sleep(0.5)
            response = {"status": "finished", "result": "Request finished", "status_code": 200}

        except KeyError as ex:
            response = {"status": "error", "error_message": f"Error parsing Input, Key: 'name' or 'content' not found",
                        "status_code": 404}
            self.logger.error(response)
        except Exception as ex:
            response = {"status": "error", "error_message": f"Error uploading prompt file. {ex}","status_code": 500}
            self.logger.error(f"Error uploading prompt file. {ex}")
        return response

    def delete_template(self, dat: dict):
        try:
            template_name = dat['name']
            delete_file(self.workspace, self.prompts_path + template_name + ".json")
            time.sleep(0.5)
            response = {"status": "finished", "result": "Request finished", "status_code": 200}

        except KeyError as ex:
            response = {"status": "error", "error_message": f"Error parsing Input, Key: 'name' or 'content' not found",
                        "status_code": 404}
            self.logger.error(response)
        except Exception as ex:
            response = {"status": "error", "error_message": f"Error uploading prompt file. {ex}","status_code": 500}
            self.logger.error(f"Error uploading prompt file. {ex}")
        return response


class IRStorageManager(BaseStorageManager):
    MODEL_FORMAT = "IRStorage"

    def __init__(self, workspace, origin):
        super().__init__(workspace, origin)
        self.models_file_path = "src/ir/conf/models_config.json"
        if not self.load_file(self.workspace, self.models_file_path):
            self.models_file_path = "src/compose/conf/models_config.json"

    def get_specific_files(self, input_object):
        dataframe_file = self._get_dataframe_file(input_object.csv, input_object.dataset_csv_path,
                                                  input_object.txt_path, input_object.do_titles, input_object.do_tables)
        markdown_files = self._get_markdown_files(input_object.txt_path, dataframe_file, input_object.department,
                                                  input_object.process_id)
        return dataframe_file, markdown_files
        #self._load_doc_per_pages(txt_path)

    def get_pools_per_embedding_model(self):
        s3_models_file = self.load_file(self.workspace, self.models_file_path)
        if s3_models_file is None or len(s3_models_file) <= 0:
            raise PrintableGenaiError(400, f"Pools can't be downloaded because {self.models_file_path} not found in {self.workspace}")
        else:
            available_pools = {}
            for key, value in json.loads(s3_models_file).get("embeddings").items():
                for model in value:
                    pools = model.get("model_pool")
                    embedding_model = model.get("embedding_model")
                    embedding_model_name = model.get("embedding_model_name")
                    if pools:
                        ## Check if the zone is already loaded: Example: "openai"
                        if not available_pools.get(key):
                            available_pools[key] = {}
                        ## Check if the embedding model is already loaded: Example: "text-embedding-ada-002"
                        if not available_pools[key].get(embedding_model):
                            available_pools[key][embedding_model] = {}
                        for pool in pools:
                            ## Check if the pool is already loaded: Example: "ada-002-pool-europe"
                            if not available_pools[key][embedding_model].get(pool):
                                available_pools[key][embedding_model][pool] = []
                            ## Check if the embedding model_name is already loaded: "'ada-002-genai-westeurope'
                            if not embedding_model_name in available_pools[key][embedding_model][pool]:
                                (available_pools[key][embedding_model][pool].append(embedding_model_name))
            if len(available_pools) == 0:
                raise PrintableGenaiError(400, "Pools were not loaded, maybe the models_config.json is wrong")
            return available_pools

    def get_available_pools(self):
        s3_models_file = self.load_file(self.workspace, self.models_file_path)
        if s3_models_file is None or len(s3_models_file) <= 0:
            raise PrintableGenaiError(400, f"Pools can't be downloaded because {self.models_file_path} not found in {self.workspace}")
        else:
            available_pools = {}
            for key, value in json.loads(s3_models_file).get("embeddings").items():
                for model in value:
                    pools = model.get("model_pool", [])
                    model_name = model.get("embedding_model_name")
                    for pool in pools:
                        if pool not in available_pools:
                            available_pools[pool] = set()
                        available_pools[pool].add(model_name)
            if len(available_pools) == 0:
                raise PrintableGenaiError(400, "Pools were not loaded, maybe the models_config.json is wrong")
            # Convert the set in lists
            for key, value in available_pools.items():
                available_pools[key] = list(value)
            return available_pools

    def get_unique_embedding_models(self):
        s3_models_file = self.load_file(self.workspace, self.models_file_path)
        if s3_models_file is None or len(s3_models_file) <= 0:
            raise PrintableGenaiError(400, f"Pools can't be downloaded because {self.models_file_path} not found in {self.workspace}")
        else:
            available_embedding_models = set()
            for key, value in json.loads(s3_models_file).get("embeddings").items():
                for model in value:
                    available_embedding_models.add(model.get("embedding_model"))
            return available_embedding_models

    def get_available_embedding_models(self, inforetrieval_mode: bool = False):
        s3_models_file = self.load_file(self.workspace, self.models_file_path)
        if s3_models_file is None or len(s3_models_file) <= 0:
            raise PrintableGenaiError(400, f"Models can't be downloaded because {self.models_file_path} not found in {self.workspace}")
        else:
            if json.loads(s3_models_file).get("embeddings"):
                available_models = json.loads(s3_models_file).get("embeddings")
                if inforetrieval_mode:
                    models_response = []
                    for platform, models in available_models.items():
                        for m in models:
                            m["platform"] = platform
                        models_response.extend(models)
                    return models_response
                return available_models
            else:
                raise PrintableGenaiError(400, f"Models can't be loaded, maybe the models_config.json is wrong")

    def get_embedding_equivalences(self):
        default_embeddings = self.load_file(self.workspace, "src/ir/conf/default_embedding_models.json")
        return json.loads(default_embeddings)



    ############################################################################################################
    #                                                                                                          #
    #                                           PRIVATE METHODS                                                #
    #                                                                                                          #
    ############################################################################################################

    def _get_dataframe_file(self, csv: bool, dataset_csv_path: str, txt_path: str, do_titles: bool,
                            do_tables: bool):
        """ Gets the file in dataframe format

        :param csv: if there are multiple files (csv True in call)
        :param origin, workspace: bucket configuration
        :param dataset_csv_path: csv path
        :param txt_path: file path
        :param do_titles: If the file has titles in it
        :param do_tables: If the file has tables in it
        """
        if csv:
            # Preprocess is not necessary
            df = get_dataset(self.origin, "csv", path_name=dataset_csv_path)
            df = df.dropna(subset=['text'])
        else:
            # Preprocess is necessary
            txt_data = self.load_file(self.workspace, txt_path)
            if txt_data is None: raise PrintableGenaiError(400, f"File {txt_path} not found in IRStorage")
            txt_data = txt_data.decode().split("\t")
            url, text, lang, n_pags = txt_data[:4]
            text = text.replace("\\\\n", "\n").replace("\\\\t", "\t").replace("\\\\r", "\r")
            metadata = txt_data[4:]
            if do_titles or do_tables:
                mtext_path = os.path.splitext(txt_path)[0] + "_markdowns.txt"
                m_text = self.load_file(self.workspace, mtext_path)
                if m_text is None: raise PrintableGenaiError(400, f"File {mtext_path} not found in IRStorage")
                m_text = m_text.decode().strip()
                text = m_text if m_text is not None else text
            df = pd.DataFrame({'Url': [url], 'CategoryId': [""], 'text': [text]})
            for m in metadata:
                df[m.split(":")[0].strip()] = self._parse_metadata(":".join(m.split(":")[1:]).strip())
        return df

    def _get_markdown_files(self, txt_path: str, df: pd.DataFrame, department: str, process_id: str):
        """ Gets the markdown files associated to the file

        :param df: previous dataframe
        :param workspace: bucket configuration
        :param txt_path: file path
        :param department: department
        :param process_id: process id
        """
        markdown_txts = []
        for i in range(len(df)):
            markdown_path = os.path.splitext(txt_path)[0] + "_markdowns.txt" if txt_path \
                else f"{department}/{process_id}/txt/{os.path.splitext(df['Url'].loc[i])[0]}_markdowns.txt"
            markdown_file = self.load_file(self.workspace, markdown_path)
            if markdown_file is not None:
                markdown_txts.append(markdown_file)
        return markdown_txts

    def _load_doc_per_pages(self, txt_path: str):
        """ Gets the pages from a file to add it as a metadata

        :param workspace: bucket configuration
        :param txt_path: file path
        """
        pags_path_pdfminer = txt_path.replace("/txt/", "/text/txt/").replace(".txt", "/pags/")
        pags_path_ocr = txt_path.replace("/txt/", "/text/ocr/").replace(".txt", "/pags/")
        try:
            files = list_files(self.workspace, pags_path_pdfminer)
        except:
            files = []
        if files == []:
            try:
                files = list_files(self.workspace, pags_path_ocr)
            except:
                files = []
        for file in files:
            page = self.load_file(self.workspace, file)
            if page is not None:
                self.doc_by_pages.append(page.decode())

    @staticmethod
    def _parse_metadata(metadata):
        """ Parses the metadata passed by input

         param: metadata: metadata
         """
        try:
            metadata = json.loads(metadata)
        except:
            pass
        return metadata


class ManagerStorage(object):
    MODEL_TYPES = [IRStorageManager, LLMStorageManager]

    @staticmethod
    def get_file_storage(conf: dict) -> BaseStorageManager:
        """ Method to instantiate the document loader class: [IRStorage, LLMStorage]

        :param conf: Loader configuration. Example:  {"type":"IRStorage"}
        """
        for loader in ManagerStorage.MODEL_TYPES:
            loader_type = conf.get('type')
            if loader.is_file_storage_type(loader_type):
                conf.pop('type')
                return loader(**conf)
        raise PrintableGenaiError(400, f"Platform type doesnt exist {conf}. "
                         f"Possible values: {ManagerStorage.get_posible_file_storages()}")

    @staticmethod
    def get_posible_file_storages() -> List:
        """ Method to list the document loaders: [IRStorage, LLMStorage]

        :param conf: Model configuration. Example:  {"type":"IRStorage"}
        """
        return [store.MODEL_FORMAT for store in ManagerStorage.MODEL_TYPES]