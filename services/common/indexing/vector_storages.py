### This code is property of the GGAO ###


# Native imports
from abc import ABC
from typing import List, Union, Tuple
import logging
import uuid
import re
import json
import unicodedata, os

# Installed imports
from haystack.nodes import PreProcessor
import pandas as pd
from langdetect import detect
import langdetect
import tiktoken
from elasticsearch.exceptions import ConnectionError as ElasticConnectionError

# Custom imports
from common.ir import modify_index_documents
from common.genai_sdk_controllers import load_file, provider
from common.logging_handler import LoggerHandler
from common.indexing.parsers import Parser
from common.services import VECTOR_DB_SERVICE
from common.indexing.japanese_preprocessor import JapanesePreProcessor
from common.indexing.retrievers import ManagerRetriever
from common.indexing.documentstores import ManagerDocumentStore
from common.indexing.connectors import Connector
from common.dolffia_json_parser import get_exc_info


class VectorDB(ABC):
    MODEL_FORMAT = "VectorDB"

    def __init__(self, connector: Connector, workspace, origin):
        self.connector = connector
        self.retriever = None
        self.document_store = None
        log = logging.getLogger('werkzeug')
        log.disabled = True
        self.origin = origin
        self.workspace = workspace

        logger_handler = LoggerHandler(VECTOR_DB_SERVICE, level=os.environ.get('LOG_LEVEL', "INFO"))
        self.logger = logger_handler.logger

    def establish_connection(self):
        if self.connector is None:
            raise ValueError("The connector is 'None' so the connection can't be established")
        if self.connector.connection is None:
            self.connector.connect()

    def get_processed_data(self, io: Parser, df: pd.DataFrame, markdown_files: List) -> List:
        pass

    def index_documents(self, docs: List, io: Parser, doc_by_pages: List) -> Tuple[dict, List]:
        pass

    def set_document_store(self, index: str, config: dict, vector_storage_type: str, embedding_model: str,
                           similarity_function: str, retriever: str, column_name: str):
        pass

    def set_retriever(self, retriever: str, embedding_model: str, api_key: str, config: dict):
        pass


    @classmethod
    def is_platform_type(cls, model_type):
        """Checks if a given model type is equel to the model format and thus it must be the one to use.
        """
        return model_type == cls.MODEL_FORMAT


class UhiStack(VectorDB):
    MODEL_FORMAT = "UhiStack"
    ELASTIC_KEYS = {"content", "content_type", "id_hash_keys", "snippet_number", "snippet_id", "name", "embedding"}
    URI_BASEPATH = {
        'aws': f"https://{os.environ.get('STORAGE_DATA')}.s3.{os.environ.get('AWS_REGION_NAME')}.amazonaws.com/",
        'azure': f"https://d2astorage.blob.core.windows.net/{os.environ.get('STORAGE_DATA')}"
    }
    EMBEDDING_SPECIFIC_KEYS = ["api_key", "azure_api_version", "azure_base_url", "azure_deployment_name"]
    BATCH_SIZE = 8
    encoding = tiktoken.get_encoding("cl100k_base")

    def __init__(self, connector: Connector, workspace, origin):
        super().__init__(connector, workspace, origin)
        self.retriever_manager = ManagerRetriever()
        self.document_store_manager = ManagerDocumentStore()

        super().establish_connection()

    def get_processed_data(self, io: Parser, df: pd.DataFrame, markdown_files: List) -> List:
        docs = self._get_df_in_elastic_format(df, markdown_files,
                                             io.txt_path, io.csv, io.do_titles, io.do_tables)
        try:
            self._assert_correct_index_metadata(io.index, docs)
        except ElasticConnectionError:
            self.logger.error("Connection to elastic failed. Check if the elastic service is running.",
                              exc_info=get_exc_info())
            raise ValueError(f"Index {io.index} connection to elastic: {io.vector_storage} is not available.")
        return docs

    def index_documents(self, docs: List, io: Parser, doc_by_pages: List) -> Tuple[dict, List]:
        self.set_document_store(index=io.index, config=io.vector_storage,
                                 vector_storage_type=io.vector_storage.get('vector_storage_type'), embedding_model=None,
                                 similarity_function=None)
        self.set_retriever(config={})
        self.logger.debug(f"Document_store and retriver set")
        aux_state_dict = {}
        aux_state_dict['vector_storage'] = io.vector_storage.get('vector_storage_name')
        aux_state_dict['models'] = []
        list_report_to_api = []

        self.logger.debug("Writing documents")
        self._write_documents(docs=docs, modify_index_docs=io.modify_index_docs,
                                       windows_overlap=io.windows_overlap, windows_length=io.windows_length,
                                       origin=self.workspace, do_tables=io.do_tables,
                                       do_titles=io.do_titles, doc_by_pages=doc_by_pages)

        aux_state_dict['models'].append({"alias": "bm25", "embedding_model": "bm25", "column_name": "embedding",
                                         "retriever": "bm25"})

        for model in io.models:
            self.logger.debug(f"Updating embeddings for model: {model['embedding_model']} "
                              f"and deployment name: {model.get('azure_deployment_name', '')}")
            self.set_document_store(index=io.index, config=io.vector_storage,
                                     vector_storage_type=io.vector_storage.get('vector_storage_type'),
                                     embedding_model=model['embedding_model'], retriever=model['retriever'],
                                     column_name=model.get('column_name'),
                                     similarity_function=model.get("similarity", None))
            self.set_retriever(config={key: model.get(key) for key in self.EMBEDDING_SPECIFIC_KEYS},
                                retriever=model['retriever'], embedding_model=model['embedding_model'])

            if not model.get('retriever_model'):
                aux_state_dict['models'].append({
                    "alias": model['alias'],
                    "embedding_model": model['embedding_model'],
                    "column_name": model.get('column_name'),
                    "retriever": model['retriever']
                })
            else:
                aux_state_dict['models'].append({
                    "alias": model['alias'],
                    "embedding_model": model['embedding_model'],
                    "retriever_model": model['retriever_model'],
                    "column_name": model.get('column_name'),
                    "retriever": model['retriever'],
                    "similarity_function": self.document_store.similarity
                })

            n_tokens = 0
            for content in docs:
                text_t = content['content']
                tokens = self.encoding.encode(text_t)
                n_tokens += len(tokens)

            list_report_to_api.append({
                f"{io.process_type}/{model['embedding_model']}/pages": {
                    "num": io.specific.get('document', {}).get('n_pags', 1),
                    "type": "PAGS"
                },
                f"{io.process_type}/{model['embedding_model']}/tokens": {
                    "num":n_tokens,
                    "type": "TOKENS"
                }
            })

            try:
                self._update_documents()
            except Exception as ex:
                self.logger.error(f"Error updating embeddings for model: {model['embedding_model']} "
                                  f"and {model}", exc_info=get_exc_info())
                raise ex

        return aux_state_dict, list_report_to_api

    def set_document_store(self, index:str, config: dict, vector_storage_type: str, embedding_model: str,
                            similarity_function: str, retriever: str = "bm25", column_name: str = "embedding"):
        """Instantiate document store with needed specific variables

        Args:
            specific (dict, optional): specific configuration of the document store. Defaults to {}.

        Returns:
            self
        """

        retriever = self.retriever_manager.get_platform({'type': retriever, 'document_store': None})
        embedding_dim = retriever.get_embedding_dim(embedding_model)
        final_similarity_function = retriever.get_similarity_function(embedding_model) if not similarity_function else similarity_function
        self.document_store = self.document_store_manager.get_platform({'type': vector_storage_type, 'index': index,
                                                                        'embedding_field': column_name,
                                                                        'embedding_dim': embedding_dim,
                                                                        'similarity_function': final_similarity_function})

        self.document_store = self.document_store.set_ds({
            "host": config.get('vector_storage_host'),
            "port": config.get('vector_storage_port'),
            "username": config.get('vector_storage_username'),
            "password": config.get('vector_storage_password'),
        })

    def set_retriever(self, retriever: str = "bm25", embedding_model=None, api_key=None, config: dict = {}):
        """Instantiate retriever with needed specific variables

        Args:
            api_key (str: optional): Api key to external services. Only needed for openai. Defaults to None
            specific (dict, optional): specific variables of the retriever. Defaults to {}.

        Raises:
            ValueError: _description_

        Returns:
            Union[SimilarityRetriever, OpenAIRetriever, BM25Retriever]: _description_
        """
        if isinstance(self.document_store, type):
            raise ValueError("Funciton set_document_store must be called before set_retriever")

        self.retriever = self.retriever_manager.get_platform({'type': retriever, 'document_store': self.document_store,
                                                              'embedding_model': embedding_model, 'api_key': api_key})
        retriever = self.retriever.MODEL_FORMAT

        self.retriever = self.retriever.set_retriever(config)
        self.retriever.mf = retriever
        self.retriever.embedding_model = embedding_model
        return self

    ############################################################################################################
    #                                                                                                          #
    #                                           PRIVATE METHODS                                                #
    #                                                                                                          #
    ############################################################################################################

    @staticmethod
    def _strip_accents(s):
        """Function to delete accents
        """
        chars_origin = "áéíóúÁÉÍÓÚàèìòùÀÈÌÒÙäëïöüÄËÏÖÜâêîôûÂÊÎÔÛ"
        chars_parsed = "aeiouAEIOUaeiouAEIOUaeiouAEIOUaeiouAEIOU"

        return s.translate(str.maketrans(chars_origin, chars_parsed))

    def _initialize_metadata(self, doc: dict, txt_path: str, doc_url: str, csv: bool, do_titles: bool, do_tables: bool):
        meta = doc['meta']
        meta.setdefault('document_id', str(uuid.uuid4()))
        meta.setdefault('uri', f"{self.URI_BASEPATH[provider]}{doc_url}")
        meta.setdefault('sections_headers', "")
        meta.setdefault('tables', "")
        #meta.setdefault('num_pag', "")
        meta.setdefault('filename', "" if csv else os.path.basename(doc_url))

        folder = os.path.splitext(txt_path)[0]
        meta['_header_mapping'] = f"{folder}_headers_mapping.json" if do_titles and not csv else ""
        meta['_csv_path'] = f"{folder.replace('/txt/', '/csvs/')}/{os.path.basename(folder)}_" if do_tables and not csv else ""

    def _get_df_in_elastic_format(self, df: pd.DataFrame, markdown_txts: List, txt_path: str, csv: bool, do_titles: bool, do_tables: bool):
        """ Gets the dataframe in elasticsearch format to index it

        :param df: dataframe to modify the format
        :param markdown_txts: if markdowns txts were found, the model will initialize some metadata
        :param csv: if csv, the model will initialize some metadata
        :param do_titles: if do_titles, the model will initialize some metadata
        :param do_tables: if do_tables, the model will initialize some metadata
        """
        elastic_docs = [
            {
                'content': row['text'],
                'meta': {column: str(row[column]) for column in row.index if
                         column not in ['Url', 'CategoryId', 'text']}
            } for index, row in df.iterrows() if row['text']
        ]

        # Add metadata
        for i, doc in enumerate(elastic_docs):
            # if language is not japanese, remove accents
            if langdetect.detect(doc['content']) != 'ja':
                doc['content'] = self._strip_accents(doc['content'])
            do_titles = do_titles and markdown_txts[i] is not None
            do_tables = do_tables and markdown_txts[i] is not None
            self._initialize_metadata(doc, txt_path, doc_url=df['Url'].iloc[i], csv=csv, do_titles=do_titles, do_tables=do_tables)

        return elastic_docs

    def _assert_correct_index_metadata(self, index: str, docs: list):
        """Raises an error if you try to change metadata for an already created index

        :param index: index name
        :param docs: list of documents to create
        """
        new_index = not self.connector.exist_index(index)
        index_mapping = self.connector.get_index_mapping(index)[index]['mappings']['properties'] if not new_index else {}
        index_meta = [key for key in index_mapping.keys() - self.ELASTIC_KEYS if not key.startswith("_")]

        collection_meta = docs[0]['meta'].keys()
        for doc in docs:
            metadata = doc['meta'].keys()
            if not all([key in collection_meta for key in metadata]):
                raise ValueError(
                    f"Detected metadata discrepancies. Verify that all documents have consistent metadata keys.")
            if new_index:
                continue
            new_meta = [key for key in metadata if key not in index_meta and not key.startswith("_")]
            if new_meta:
                raise ValueError(
                    f"Metadata keys {new_meta} do not match those in the existing index {index}. "
                    f"Check and align metadata keys. Index metadata: {list(index_meta)}")

    def _add_page(self, docs: list, doc_by_pages: List) -> list:
        """Add snippet_number and id to the documents

        Args:
            docs (list): List of documents without snippet_number and id

        Returns:
            list: List of documents with snippet_number and id
        """
        last_id = ""
        counter = 0
        for doc in docs:
            doc_id = doc['meta']['document_id']
            counter = counter + 1 if doc_id == last_id else 0
            doc['meta']['snippet_number'] = counter
            doc['meta']['snippet_id'] = str(uuid.uuid4())
            #TODO add page as metadata
            #doc['meta']['num_pag'] = self._get_num_page(doc['content'], doc_by_pages)
            last_id = doc_id
        return docs

    @staticmethod
    def _get_num_page(text: str, doc_by_pages: List) -> str:
        """ Gets the number of a page based on his content
        """
        last_pharagraph = ""
        for i in text.split("\n\n")[::-1]:
            if len(i) > 10:
                last_pharagraph = i
                break
        for i, page in enumerate(doc_by_pages):
            if page.strip("\n").find(last_pharagraph.strip("\n")) != -1:
                return str(i)
        return ""

    def _get_chunks(self, docs: list, windows_overlap, windows_length, doc_by_pages:List) -> list:
        """If defined, it will split the text into chunks of given length and overlap.

        Args:
            docs (list): List of haystack like documents
            windows_overlap (_type_): _description_
            windows_length (_type_): _description_

        Returns:
            list: _description_
        """
        if windows_length:
            preprocessor = PreProcessor(
                split_overlap=windows_overlap,
                split_length=windows_length,
                split_respect_sentence_boundary=True
            )

            jappreprocessor = JapanesePreProcessor(
                split_overlap=windows_overlap,
                split_length=windows_length
            )

            for doc in docs:
                try:
                    lang = detect(doc['content'])
                    if lang == "ja":  # If one document is japanese, we asume all documents are japanese
                        self.logger.warning(
                            "Japanese detected. Check administrators in case your documents are not japanese.")
                        docs = jappreprocessor.process(docs)
                        break
                except:
                    self.logger.warning("Language detection failed. Check if snippet is text corrupted.")
            else:
                docs = preprocessor.process(docs)

            docs = [d.to_dict() for d in docs]

        docs = self._add_page(docs, doc_by_pages)
        return docs

    @staticmethod
    def _preprocess_metadata(docs: list, origin):
        """ Postprocess chunks with preprocess metadata (section titles, tables or both)

        Args:
            docs (list): Haystack-like list of documents (chunks)
            origin (tuple): Name of bucket where preprocess is located

        Returns:
            list: Haystack-like list of documents (chunks) with titles, tables (or both) processed

        """
        sections = ""
        for doc in docs:
            text = doc['content']
            meta = doc['meta']

            mapping_path = meta.pop('_header_mapping', "")
            if mapping_path:
                headers_mapping = json.loads(load_file(origin, mapping_path))
                titles = re.findall(r"<(pag_\d+_header_\d+)>", text)
                if not titles:
                    meta['sections_headers'] = sections.split("||")[-1]
                else:
                    sections = "||".join([headers_mapping[t] for t in titles])
                    meta['sections_headers'] = sections
                for t in titles:
                    text = text.replace(f"<{t}>", "")
            else:
                meta['sections_headers'] = ""

            csv_path = meta.pop('_csv_path', "")
            if csv_path:
                tables = re.findall(r"<(pag_\d+_table_\d+)>", text)
                for t in tables:
                    csv = load_file(origin, csv_path + t + ".csv").decode()
                    text = text.replace(f"<{t}>", csv)
                meta['tables'] = True if tables else False
            else:
                meta['tables'] = ""

            doc['content'] = text

        return docs

    def _write_documents(self, docs: list, modify_index_docs: dict, windows_overlap: int, windows_length: int,
                        origin, do_titles: bool, do_tables: bool, doc_by_pages: List):
        """Write documents in the document store

        Args:
            docs (list): Haystack like list of documents
            modify_index_docs (dict): Dictionary with how to update weights. Better explanation found in function
            windows_overlap (int): How much the windows will overlap
            windows_length (int): Lenght of the passages to write
            origin (tuple): Name of bucket where preprocess is located
            do_titles (bool): True if section_titles are expected as metadata
            do_tables (bool): True if tables in csv format have to be introduced in chunks


        Raises:
            ex: No documents after filtering, returning error and logging possible causes

        """
        for doc in docs:
            doc['meta'].setdefault('document_id', str(uuid.uuid4()))

        try:
            docs = modify_index_documents(self.document_store, modify_index_docs, docs, self.logger)
            if not docs:
                return False
        except IndexError as ex:
            self.logger.error("There was an error while trying to update documents in document store.",
                              exc_info=get_exc_info())
            self.logger.error(modify_index_documents.__doc__.strip(), exc_info=get_exc_info())
            raise ex

        docs = self._get_chunks(docs, windows_overlap, windows_length, doc_by_pages)
        docs = self._preprocess_metadata(docs, origin)

        self.docs = docs  # Add it as an attribute to delete in case an error raises
        docs_len = len(docs)
        while docs_len > 0:
            self.document_store.write_documents(docs[0:self.BATCH_SIZE], batch_size=self.BATCH_SIZE)
            docs = docs[self.BATCH_SIZE:]
            docs_len = len(docs)

    def _update_documents(self):
        """Update embeddings in the given retriever
        """
        self.document_store.update_embeddings(
            self.retriever,
            batch_size=self.BATCH_SIZE,
            update_existing_embeddings=False
            )


class ManagerVectorDB(object):
    MODEL_TYPES = [UhiStack]

    @staticmethod
    def get_vector_database(conf: dict) -> VectorDB:
        """ Method to instantiate the document store class: [elasticsearch]

        :param conf: Model configuration. Example:  {"type":"elastic"}
        """
        for store in ManagerVectorDB.MODEL_TYPES:
            store_type = conf.get('type')
            if store.is_platform_type(store_type):
                conf.pop('type')
                return store(**conf)
        raise ValueError(f"Platform type doesnt exist {conf}. "
                         f"Possible values: {ManagerVectorDB.get_possible_platforms()}")

    @staticmethod
    def get_possible_platforms() -> List:
        """ Method to list the endpoints: [azure, openai]

        :param conf: Model configuration. Example:  {"platform":"openai"}
        """
        return [store.MODEL_FORMAT for store in ManagerVectorDB.MODEL_TYPES]