### This code is property of the GGAO ###


# Native imports
from abc import ABC
from typing import List
import json
import os

# Installed imports
import pandas as pd

# Custom imports
from common.genai_sdk_controllers import load_file, get_dataset, list_files
from common.indexing.parsers import ParserInfoindexing
from common.ir import INDEX_S3


class DocumentLoader(ABC):
    MODEL_FORMAT = "DocumentLoader"

    def __init__(self, workspace, origin):
        self.workspace = workspace
        self.origin = origin

    def load_file(self, origin, file) -> bytes:
        """ Loads a file from S3

        :param origin: Bucket to load file from
        :param file: Filename to load
        """
        pass

    def get_specific_files(self, input_object: ParserInfoindexing):
        """ Gets the files associated to an index

        :param input_object: Inital configuration
        :param workspace, origin: configuration from S3
        """
        pass

    def get_available_embeddings_pools(self):
        """ Get the embedding pools that can be used  """
        pass

    def get_vector_storages(self):
        """ Get the vector_storages that can be used in infoindexing """
        pass

    def get_available_models(self):
        """ Get the models that can be used in infoindexing """
        pass

    def get_state_dict(self, input_object: ParserInfoindexing):
        """ Gets the state_dict for an index """
        pass


    @classmethod
    def is_platform_type(cls, model_type):
        """Checks if a given model type is equel to the model format and thus it must be the one to use.
        """
        return model_type == cls.MODEL_FORMAT


class S3Loader(DocumentLoader):
    MODEL_FORMAT = "s3"

    def __init__(self, workspace, origin):
        super().__init__(workspace, origin)
        self.models_file_path = "src/compose/conf/models_config.json"
        self.vector_storages_config_file_path = "src/ir/conf/vector_storages_config.json"

    def load_file(self, origin, file):
        try:
            loaded_file = load_file(origin, file)
        except Exception as ex:
            return None
        return loaded_file if len(loaded_file) > 0 else None

    def get_specific_files(self, input_object: ParserInfoindexing):
        dataframe_file = self._get_dataframe_file(input_object.csv, input_object.dataset_csv_path,
                                                  input_object.txt_path, input_object.do_titles, input_object.do_tables)
        markdown_files = self._get_markdown_files(input_object.txt_path, dataframe_file, input_object.department,
                                                  input_object.process_id)
        return dataframe_file, markdown_files
        #self._load_doc_per_pages(txt_path)

    def get_available_embeddings_pools(self):
        s3_models_file = self.load_file(self.workspace, self.models_file_path)
        if s3_models_file is None or len(s3_models_file) <= 0:
            raise ValueError(f"Pools can't be downloaded because {self.models_file_path} not found in {self.workspace}")
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
                            ## Check if the embedding model_name is already loaded: "'ada-002-dolffia-westeurope'
                            if not embedding_model_name in available_pools[key][embedding_model][pool]:
                                (available_pools[key][embedding_model][pool].append(embedding_model_name))
            if len(available_pools) == 0:
                raise ValueError(f"Pools were not loaded, maybe the models_config.json is wrong")
            return available_pools

    def get_available_models(self):
        s3_models_file = self.load_file(self.workspace, self.models_file_path)
        if s3_models_file is None or len(s3_models_file) <= 0:
            raise ValueError(f"Models can't be downloaded because {self.models_file_path} not found in {self.workspace}")
        else:
            if json.loads(s3_models_file).get("embeddings"):
                return json.loads(s3_models_file).get("embeddings")
            else:
                raise ValueError(f"Models can't be loaded, maybe the models_config.json is wrong")

    def get_vector_storages(self):
        s3_pool_file = self.load_file(self.workspace, self.vector_storages_config_file_path)
        if s3_pool_file is None or len(s3_pool_file) <= 0:
            raise ValueError(f"Vector storages config file can't be downloaded because "
                             f"{self.vector_storages_config_file_path} not found in {self.workspace}")
        else:
            if json.loads(s3_pool_file).get("vector_storage_supported"):
                return json.loads(s3_pool_file).get("vector_storage_supported")
            else:
                raise ValueError(f"Vector storages can't be loaded, maybe the vector_storages_config.json is wrong")

    def get_state_dict(self, input_object: ParserInfoindexing):
        state_dict = self.load_file(self.workspace, INDEX_S3(input_object.index))
        if state_dict is None:
            state_dict = "{}".encode()
        state_dict = json.loads(state_dict.decode())
        return state_dict




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
            if txt_data is None: raise ValueError(f"File {txt_path} not found in S3")
            txt_data = txt_data.decode().split("\t")
            url, text, lang, n_pags = txt_data[:4]
            text = text.replace("\\\\n", "\n").replace("\\\\t", "\t").replace("\\\\r", "\r")
            metadata = txt_data[4:]
            if do_titles or do_tables:
                mtext_path = os.path.splitext(txt_path)[0] + "_markdowns.txt"
                m_text = self.load_file(self.workspace, mtext_path)
                if m_text is None: raise ValueError(f"File {mtext_path} not found in S3")
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


class ManagerLoader(object):
    MODEL_TYPES = [S3Loader]

    @staticmethod
    def get_file_storage(conf: dict) -> DocumentLoader:
        """ Method to instantiate the document loader class: [S3]

        :param conf: Loader configuration. Example:  {"type":"s3"}
        """
        for loader in ManagerLoader.MODEL_TYPES:
            loader_type = conf.get('type')
            if loader.is_platform_type(loader_type):
                conf.pop('type')
                return loader(**conf)
        raise ValueError(f"Platform type doesnt exist {conf}. "
                         f"Possible values: {ManagerLoader.get_possible_platforms()}")

    @staticmethod
    def get_possible_platforms() -> List:
        """ Method to list the document loaders: [S3]

        :param conf: Model configuration. Example:  {"type":"s3"}
        """
        return [store.MODEL_FORMAT for store in ManagerLoader.MODEL_TYPES]