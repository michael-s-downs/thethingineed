### This code is property of the GGAO ###


# Native imports
from abc import ABC
from typing import List
import logging
import os
import time

# Installed imports
import pandas as pd
import langdetect
import tiktoken
from llama_index.core import StorageContext, VectorStoreIndex, Settings, Document
from httpx import TimeoutException
from openai import RateLimitError

# Custom imports
from common.ir.utils import get_embed_model
from common.genai_controllers import provider
from common.logging_handler import LoggerHandler
from common.ir.parsers import Parser
from common.services import VECTOR_DB_SERVICE
from common.ir.connectors import Connector
from common.genai_json_parser import get_exc_info
from common.utils import INDEX_NAME
from common.errors.genaierrors import PrintableGenaiError

from chunking_methods import ManagerChunkingMethods

from elasticsearch.exceptions import ConnectionError as ElasticConnectionError
from elasticsearch.helpers.errors import BulkIndexError
from elastic_transport import ConnectionTimeout
from elasticsearch import AsyncElasticsearch
from llama_index.vector_stores.elasticsearch import ElasticsearchStore

from azure.core.exceptions import ServiceRequestError
from llama_index.vector_stores.azureaisearch import AzureAISearchVectorStore
from llama_index.vector_stores.azureaisearch import IndexManagement

class VectorDB(ABC):
    MODEL_FORMAT = "VectorDB"

    def __init__(self, connector: Connector, workspace, origin, aws_credentials):
        self.connector = connector
        log = logging.getLogger('werkzeug')
        log.disabled = True
        self.origin = origin
        self.workspace = workspace
        self.aws_credentials = aws_credentials

        logger_handler = LoggerHandler(VECTOR_DB_SERVICE, level=os.environ.get('LOG_LEVEL', "INFO"))
        self.logger = logger_handler.logger


    def get_processed_data(self, io: Parser, df: pd.DataFrame, markdown_files: List) -> List:
        pass

    def index_documents(self, docs: List, io: Parser) -> List:
        pass


    @classmethod
    def is_vector_database_type(cls, model_type):
        """Checks if a given model type is equel to the model format and thus it must be the one to use.
        """
        return model_type == cls.MODEL_FORMAT

    @staticmethod
    def _strip_accents(s):
        """Function to delete accents
        """
        chars_origin = "áéíóúÁÉÍÓÚàèìòùÀÈÌÒÙäëïöüÄËÏÖÜâêîôûÂÊÎÔÛ"
        chars_parsed = "aeiouAEIOUaeiouAEIOUaeiouAEIOUaeiouAEIOU"

        return s.translate(str.maketrans(chars_origin, chars_parsed))

    def _initialize_metadata(self, doc: dict, txt_path: str, doc_url: str, csv: bool, do_titles: bool, do_tables: bool):
        meta = doc['meta']
        meta.setdefault('uri', f"{self.URI_BASEPATH[provider]}{doc_url}")
        meta.setdefault('sections_headers', "")
        meta.setdefault('tables', "")
        meta.setdefault('filename', "" if csv else os.path.basename(doc_url))

        folder = os.path.splitext(txt_path)[0]
        meta['_header_mapping'] = f"{folder}_headers_mapping.json" if do_titles and not csv else ""
        meta['_csv_path'] = f"{folder.replace('/txt/', '/csvs/')}/{os.path.basename(folder)}_" if do_tables and not csv else ""

    def _get_documents_from_dataframe(self, df: pd.DataFrame, markdown_txts: List, txt_path: str, csv: bool, do_titles: bool, do_tables: bool):
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
        final_docs = []
        for i, doc in enumerate(elastic_docs):
            # if language is not japanese, remove accents
            if langdetect.detect(doc['content']) != 'ja':
                doc['content'] = self._strip_accents(doc['content'])
            do_titles = do_titles and markdown_txts[i] is not None
            do_tables = do_tables and markdown_txts[i] is not None
            self._initialize_metadata(doc, txt_path, doc_url=df['Url'].iloc[i], csv=csv, do_titles=do_titles, do_tables=do_tables)
            llama_doc = Document(text=doc['content'], metadata=doc['meta'])
            final_docs.append(llama_doc)
        return final_docs


    def _manage_indexing_exception(self, index_name, models, docs_filenames):
        self.logger.warning(
            f"Max retries exceeded while indexing {docs_filenames}, deleting nodes and closing connection")
        time.sleep(50)
        for model in models:
            processed_index_name = INDEX_NAME(index_name, model.get('embedding_model'))
            # Documents deletion
            result = self.connector.delete_documents(processed_index_name, {"filename": docs_filenames})
            if len(result.body.get('failures', [])) > 0:
                result = "Error deleting documents"
            elif result.body.get('deleted', 0) == 0:
                result = "Documents not found"
            else:
                result = f"{result.body['deleted']} chunks deleted."
            self.logger.debug(f"Result deleting documents in index {processed_index_name}: {result}")


class LlamaIndexElastic(VectorDB):

    MODEL_FORMAT = "elastic"
    URI_BASEPATH = {
        'aws': f"https://{os.environ.get('STORAGE_DATA')}.s3.{os.environ.get('AWS_REGION_NAME')}.amazonaws.com/",
        'azure': f"https://d2astorage.blob.core.windows.net/{os.environ.get('STORAGE_DATA')}"
    }
    encoding = tiktoken.get_encoding("cl100k_base")

    def __init__(self, connector: Connector, workspace, origin, aws_credentials):
        super().__init__(connector, workspace, origin, aws_credentials)

    def get_processed_data(self, io: Parser, df: pd.DataFrame, markdown_files: List) -> List:
        docs = self._get_documents_from_dataframe(df, markdown_files,
                                             io.txt_path, io.csv, io.do_titles, io.do_tables)
        try:
            for model in io.models:
                self.connector.assert_correct_index_metadata(INDEX_NAME(io.index, model.get('embedding_model')),
                                                             docs,
                                                             ["_node_content", "_node_type","doc_id", "ref_doc_id", "document_id"])
        except ElasticConnectionError:
            self.logger.error("Connection to elastic failed. Check if the elastic service is running.",
                              exc_info=get_exc_info())
            host = io.vector_storage.get("vector_storage_host")
            raise PrintableGenaiError(400, f"Index {io.index} connection to elastic: {host} is not available.")
        return docs

    def index_documents(self, docs: List, io: Parser) -> List:
        """Index documents in the document store"""
        list_report_to_api = []
        n_tokens = sum(len(self.encoding.encode(doc.text)) for doc in docs)
        chunking_method = ManagerChunkingMethods.get_chunking_method({**io.chunking_method, "origin": self.origin,
                                                                      "workspace": self.workspace})

        nodes_per_doc = chunking_method.get_chunks(docs, self.encoding, io)

        # Indexation with the embeddings generation
        for model in io.models:
            index_name = INDEX_NAME(io.index, model.get('embedding_model'))
            embed_model = get_embed_model(model, self.aws_credentials, is_retrieval=False)
            Settings.embed_model = embed_model
            vector_store = ElasticsearchStore(index_name=index_name, es_client=AsyncElasticsearch(hosts=f"{self.connector.scheme}://{self.connector.host}:{self.connector.port}",
                                           basic_auth=(self.connector.username, self.connector.password),
                                           verify_certs=False, request_timeout=30))

            retries = self._write_nodes(nodes_per_doc, embed_model, vector_store, io.models, io.index)

            self.logger.info(f"Model {model.get('embedding_model')} has been indexed in {index_name}")

            list_report_to_api.append({
                f"{io.process_type}/{model['embedding_model']}/pages": {
                    "num": io.specific.get('document', {}).get('n_pags', 1),
                    "type": "PAGS"
                },
                f"{io.process_type}/{model['embedding_model']}/tokens": {
                    "num": n_tokens * retries, # embeddings calculation for each retry
                    "type": "TOKENS"
                }
            })
            vector_store.close()# AsyncElasticsearch connection close
        return list_report_to_api

    def _write_nodes(self, nodes_per_doc: list, embed_model, vector_store, models, index_name, delta=0, max_retries=3):
        """Write documents in the document store

        :param nodes_per_doc: list of nodes
        :param embed_model: embed model
        :param vector_store: vector store
        """
        try:
            if delta >= max_retries:
                raise ConnectionError("Max num of retries reached. Nodes have been deleted.")
            storage_context = StorageContext.from_defaults(vector_store=vector_store)
            index = VectorStoreIndex(nodes=[], storage_context=storage_context,
                                     transformations=[embed_model])
            for nodes in nodes_per_doc:
                index.insert_nodes(nodes, show_progress=True)
            return delta + 1
        except (BulkIndexError, ConnectionTimeout):
            docs_filenames = set([node.metadata.get('filename') for nodes in nodes_per_doc for node in nodes])
            self.logger.warning(f"BulkingIndexError/ConnectionTimeout detected while indexing{list(docs_filenames)}, retrying, try {delta + 1}/{max_retries}")
            time.sleep(4)
            return self._write_nodes(nodes_per_doc, embed_model, vector_store, models, index_name, delta=delta + 1)
        except (TimeoutException, RateLimitError):
            docs_filenames = set([node.metadata.get('filename') for nodes in nodes_per_doc for node in nodes])
            self.logger.warning(f"Timeout/RateLimitError detected while indexing{list(docs_filenames)}, retrying, try {delta + 1}/{max_retries}")
            time.sleep(40)
            return self._write_nodes(nodes_per_doc, embed_model, vector_store, models, index_name, delta=delta + 1)
        except Exception as e:
            docs_filenames = list(set([node.metadata.get('filename') for nodes in nodes_per_doc for node in nodes]))
            self._manage_indexing_exception(index_name, models, docs_filenames)
            vector_store.close()

            self.logger.warning(f"Nodes deleted due to: {type(e).__name__}; {e.args}")
            raise ConnectionError(f"Max num of retries reached while indexing {docs_filenames}")


class LlamaIndexAzureAI(VectorDB):

    MODEL_FORMAT = "ai_search"
    URI_BASEPATH = {
        'aws': f"https://{os.environ.get('STORAGE_DATA')}.s3.{os.environ.get('AWS_REGION_NAME')}.amazonaws.com/",
        'azure': f"https://d2astorage.blob.core.windows.net/{os.environ.get('STORAGE_DATA')}"
    }
    encoding = tiktoken.get_encoding("cl100k_base")

    def __init__(self, connector: Connector, workspace, origin, aws_credentials):
        super().__init__(connector, workspace, origin, aws_credentials)

    def get_processed_data(self, io: Parser, df: pd.DataFrame, markdown_files: List) -> List:
        docs = self._get_documents_from_dataframe(df, markdown_files,
                                             io.txt_path, io.csv, io.do_titles, io.do_tables)
        try:
            for model in io.models:
                self.connector.assert_correct_index_metadata(INDEX_NAME(io.index, model.get('embedding_model')),
                                                             docs,
                                                             ["_node_content", "_node_type", "doc_id", "ref_doc_id", "document_id"])
        except ServiceRequestError:
            self.logger.error("Connection to Azure AI Search failed. Check if the service is running.",
                              exc_info=get_exc_info())
            host = io.vector_storage.get("vector_storage_host")
            raise PrintableGenaiError(400, f"Index {io.index} connection to Azure AI Search: {host} is not available.")
        return docs

    def index_documents(self, docs: List, io: Parser) -> List:
        """Index documents in Azure AI Search"""
        list_report_to_api = []
        n_tokens = sum(len(self.encoding.encode(doc.text)) for doc in docs)
        chunking_method = ManagerChunkingMethods.get_chunking_method({**io.chunking_method, "origin": self.origin,
                                                                      "workspace": self.workspace})

        nodes_per_doc = chunking_method.get_chunks(docs, self.encoding, io)

        # Indexation with the embeddings generation
        for model in io.models:
            index_name = INDEX_NAME(io.index, model.get('embedding_model'))
            embed_model = get_embed_model(model, self.aws_credentials, is_retrieval=False)
            Settings.embed_model = embed_model

            # Initialize Azure Search vector store
            vector_store = AzureAISearchVectorStore(
                search_or_index_client=self.connector.index_client,
                hidden_field_keys=["embedding"],
                index_name=index_name,
                index_management=IndexManagement.CREATE_IF_NOT_EXISTS,
                id_field_key="id",
                chunk_field_key="content",
                embedding_field_key="embeddings",
                embedding_dimensionality=1536,
                metadata_string_field_key="metadata",
                doc_id_field_key="doc_id",
                language_analyzer="en.lucene",
                vector_algorithm_type="exhaustiveKnn",
                semantic_configuration_name="mySemanticConfig",
            )
    
            retries = self._write_nodes(nodes_per_doc, embed_model, vector_store, io.models, io.index)

            self.logger.info(f"Model {model.get('embedding_model')} has been indexed in {index_name}")

            list_report_to_api.append({
                f"{io.process_type}/{model['embedding_model']}/pages": {
                    "num": io.specific.get('document', {}).get('n_pags', 1),
                    "type": "PAGS"
                },
                f"{io.process_type}/{model['embedding_model']}/tokens": {
                    "num": n_tokens * retries,  # embeddings calculation for each retry
                    "type": "TOKENS"
                }
            })
        return list_report_to_api

    def _write_nodes(self, nodes_per_doc: list, embed_model, vector_store, models, index_name, delta=0, max_retries=3):
        """Write documents in Azure AI Search

        :param nodes_per_doc: list of nodes
        :param embed_model: embed model
        :param vector_store: vector store
        """
        try:
            if delta >= max_retries:
                raise ConnectionError("Max num of retries reached. Nodes have been deleted.")
            storage_context = StorageContext.from_defaults(vector_store=vector_store)
            index = VectorStoreIndex(nodes=[], storage_context=storage_context,
                                     transformations=[embed_model])
            for nodes in nodes_per_doc:
                index.insert_nodes(nodes, show_progress=True)
            return delta + 1
        except ServiceRequestError:
            docs_filenames = set([node.metadata.get('filename') for nodes in nodes_per_doc for node in nodes])
            self.logger.warning(f"ServiceRequestError detected while indexing{list(docs_filenames)}, retrying, try {delta + 1}/{max_retries}")
            time.sleep(4)
            return self._write_nodes(nodes_per_doc, embed_model, vector_store, models, index_name, delta=delta + 1)
        except (TimeoutException, RateLimitError):
            docs_filenames = set([node.metadata.get('filename') for nodes in nodes_per_doc for node in nodes])
            self.logger.warning(f"Timeout/RateLimitError detected while indexing{list(docs_filenames)}, retrying, try {delta + 1}/{max_retries}")
            time.sleep(40)
            return self._write_nodes(nodes_per_doc, embed_model, vector_store, models, index_name, delta=delta + 1)
        except Exception as e:
            docs_filenames = list(set([node.metadata.get('filename') for nodes in nodes_per_doc for node in nodes]))
            self._manage_indexing_exception(index_name, models, docs_filenames)

            self.logger.warning(f"Nodes deleted due to: {type(e).__name__}; {e.args}")
            raise ConnectionError(f"Max num of retries reached while indexing {docs_filenames}")



class ManagerVectorDB(object):
    MODEL_TYPES = [LlamaIndexElastic, LlamaIndexAzureAI]

    @staticmethod
    def get_vector_database(conf: dict) -> VectorDB:
        """ Method to instantiate the document store class: [elasticsearch]

        :param conf: Model configuration. Example:  {"type":"elastic"}
        """
        store_type = conf.get('type')
        for store in ManagerVectorDB.MODEL_TYPES:
            if store.is_vector_database_type(store_type):
                conf.pop('type')
                return store(**conf)
        raise PrintableGenaiError(400, f"Platform type doesnt exist {conf}. "
                         f"Possible values: {ManagerVectorDB.get_possible_vector_databases()}")

    @staticmethod
    def get_possible_vector_databases() -> List:
        """ Method to list the endpoints: [azure, openai]

        :param conf: Model configuration. Example:  {"platform":"openai"}
        """
        return [store.MODEL_FORMAT for store in ManagerVectorDB.MODEL_TYPES]
