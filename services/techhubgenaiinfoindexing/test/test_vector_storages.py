
import unittest
from unittest.mock import patch, MagicMock
import pytest

from elasticsearch.helpers import BulkIndexError
from httpx import TimeoutException

from vector_storages import VectorDB, LlamaIndexElastic, ManagerVectorDB, LlamaIndexAzureAI
from elasticsearch.exceptions import ConnectionError as ElasticConnectionError
from common.errors.genaierrors import PrintableGenaiError
from common.ir.connectors import Connector
from common.ir.parsers import Parser
import pandas as pd
from typing import List
from llama_index.core import Document

class MockVectorDB(VectorDB):
    def get_processed_data(self, io: Parser, df: pd.DataFrame, markdown_files: List) -> List:
        processed_data = [f"Processed {file}" for file in markdown_files]
        return processed_data

    def index_documents(self, docs: List, io: Parser) -> List:
        return [f"Indexed {doc}" for doc in docs]

class TextVectorDB(unittest.TestCase):
    def setUp(self):
        self.connector = MagicMock()
        self.workspace = "test_workspace"
        self.origin = "test_origin"
        self.aws_credentials = {"key": "test_key", "secret": "test_secret"}
        self.vector_db = VectorDB(self.connector, self.workspace, self.origin, self.aws_credentials)

class TestLlamaIndex(unittest.TestCase):

    def setUp(self):
        self.connector = MagicMock(spec=Connector)
        self.workspace = "workspace_path"
        self.origin = "origin_path"
        self.aws_credentials = {"key": "dummy_key", "secret": "dummy_secret"}
        self.vector_db = LlamaIndexElastic(self.connector, self.workspace, self.origin, self.aws_credentials)
        self.vector_db.logger = MagicMock()

        self.connector_mock = MagicMock()
        self.logger_mock = MagicMock()
        self.lama_index = LlamaIndexElastic(connector=self.connector_mock, workspace='test_workspace', origin='test_origin', aws_credentials=None)
        self.lama_index.logger = self.logger_mock

    @patch.object(LlamaIndexElastic, 'get_processed_data', return_value=['mock_document'])
    def test_call_get_processed_data(self, mock_get_processed_data):
        """Test that get_processed_data is called with correct parameters."""
        mock_parser = MagicMock()
        mock_parser.models = [{'embedding_model': 'test_model'}]
        mock_parser.index = 'test_index'

        df = pd.DataFrame({'text': ['Test content'], 'Url': ['test_url']})
        markdown_files = [None]

        result = self.vector_db.get_processed_data(mock_parser, df, markdown_files)

        mock_get_processed_data.assert_called_once_with(mock_parser, df, markdown_files)

        self.assertEqual(result, ['mock_document'])

    @patch.object(LlamaIndexElastic, 'index_documents', return_value=[{'status': 'success'}])
    def test_call_index_documents(self, mock_index_documents):
        """Test that index_documents is called with correct parameters."""
        docs = [MagicMock()]
        mock_parser = MagicMock()

        result = self.vector_db.index_documents(docs, mock_parser)

        mock_index_documents.assert_called_once_with(docs, mock_parser)

        self.assertEqual(result, [{'status': 'success'}])

    @patch("vector_storages.logging.getLogger")
    @patch("vector_storages.LoggerHandler")
    def test_vector_db_init(self, mock_logger_handler, mock_get_logger):
        """Test initialization of the VectorDB class."""
        logger_instance = MagicMock()
        mock_logger_handler.return_value.logger = logger_instance

        vector_db = LlamaIndexElastic(self.connector, self.workspace, self.origin, self.aws_credentials)

        self.assertEqual(vector_db.connector, self.connector)
        self.assertEqual(vector_db.workspace, self.workspace)
        self.assertEqual(vector_db.origin, self.origin)
        self.assertEqual(vector_db.aws_credentials, self.aws_credentials)
        self.assertEqual(vector_db.logger, logger_instance)

    def test_is_platform_type(self):
        """Test the is_platform_type method."""
        self.assertTrue(LlamaIndexElastic.is_vector_database_type("elastic"))
        self.assertFalse(LlamaIndexElastic.is_vector_database_type("OtherType"))

    def test_get_processed_data_success(self):
        mock_connector = MagicMock()
        mock_parser = MagicMock()
        mock_logger = MagicMock()

        llama_index = LlamaIndexElastic(mock_connector, workspace="test_workspace", origin="test_origin", aws_credentials={})
        llama_index.logger = mock_logger

        mock_parser.models = [{'embedding_model': 'test_model'}]
        mock_parser.index = 'test_index'
        mock_parser.txt_path = 'test_path'
        mock_parser.csv = False
        mock_parser.do_titles = False
        mock_parser.do_tables = False

        df = pd.DataFrame({'text': ['Test content'], 'Url': ['test_url']})
        markdown_files = [None]

        llama_index._get_documents_from_dataframe = MagicMock(return_value=['mock_document'])
        mock_connector.assert_correct_index_metadata = MagicMock()

        result = llama_index.get_processed_data(mock_parser, df, markdown_files)

        llama_index._get_documents_from_dataframe.assert_called_once()
        mock_connector.assert_correct_index_metadata.assert_called_once()
        assert result == ['mock_document']

    @patch("vector_storages.logging.getLogger")
    @patch("vector_storages.LoggerHandler")
    def test_get_processed_data_exception(self, mock_logger_handler, mock_get_logger):
        """Test the exception handling in get_processed_data when ElasticConnectionError is raised."""

        mock_connector = MagicMock(spec=Connector)
        mock_logger = MagicMock()

        llama_index = LlamaIndexElastic(mock_connector, workspace="test_workspace", origin="test_origin", aws_credentials={})
        llama_index.logger = mock_logger

        mock_parser = MagicMock()
        mock_parser.models = [{'embedding_model': 'test_model'}]
        mock_parser.index = 'test_index'
        mock_parser.txt_path = 'test_path'
        mock_parser.csv = False
        mock_parser.do_titles = False
        mock_parser.do_tables = False

        df = pd.DataFrame({'text': ['Test content'], 'Url': ['test_url']})
        markdown_files = [None]

        llama_index._get_documents_from_dataframe = MagicMock(return_value=['mock_document'])

        mock_connector.assert_correct_index_metadata = MagicMock(side_effect=ElasticConnectionError("ElasticSearch connection failed."))

        with self.assertRaises(PrintableGenaiError) as cm:
            llama_index.get_processed_data(mock_parser, df, markdown_files)

        mock_logger.error.assert_called_with(
            "Connection to elastic failed. Check if the elastic service is running.",
            exc_info=False
        )

        self.assertEqual(cm.exception.status_code, 400)
        self.assertIn("Index test_index connection to elastic", str(cm.exception))

    @patch("vector_storages.get_embed_model")
    @patch("vector_storages.ElasticsearchStore")
    @patch("vector_storages.ManagerChunkingMethods.get_chunking_method")
    @patch("vector_storages.VectorStoreIndex")
    @patch("vector_storages.StorageContext.from_defaults")
    def test_index_documents(self, mock_storage_context, mock_vector_index, mock_get_chunking, mock_es_store, mock_embed_model):
        """Test the index_documents method."""

        # Mock del Parser
        io = MagicMock(spec=Parser)
        io.models = [{"embedding_model": "test_model"}]
        io.index = "test_index"
        io.process_type = "process_type"
        io.specific = {"document": {"n_pags": 1}}
        io.chunking_method = {}
        io.scheme = {}
        io.index_metadata = ["filename"]
        docs = [MagicMock()]

        # Mock para `self.connector`
        self.vector_db.connector.scheme = "http"
        self.vector_db.connector.host = "localhost"
        self.vector_db.connector.port = "9200"
        self.vector_db.connector.username = "test_user"
        self.vector_db.connector.password = "test_pass"

        self.vector_db.connector.exist_index = MagicMock(return_value=False)
        self.vector_db.connector.create_empty_index = MagicMock()

        self.vector_db.encoding.encode = MagicMock(return_value=["token1", "token2"])

        mock_get_chunking.return_value.get_chunks.return_value = [["chunk1"], ["chunk2"]]

        mock_embed_model.return_value = None

        mock_storage_context.return_value = MagicMock()
        mock_vector_index.return_value.insert_nodes = MagicMock()

        mock_es_store.return_value = MagicMock()

        # test execution
        result = self.vector_db.index_documents(docs, io)

        mock_get_chunking.assert_called_once_with({**io.chunking_method, "origin": self.vector_db.origin, "workspace": self.vector_db.workspace})
        self.assertEqual(len(result), 1)
        expected_keys = [
            f"{io.process_type}/{io.models[0]['embedding_model']}/pages",
            f"{io.process_type}/{io.models[0]['embedding_model']}/tokens",
        ]
        for key in expected_keys:
            self.assertIn(key, result[0].keys())

    def test_strip_accents(self):
        """Test the _strip_accents method."""
        input_text = "áéíóúÁÉÍÓÚàèìòùÀÈÌÒÙäëïöüÄËÏÖÜâêîôûÂÊÎÔÛ"
        expected_text = "aeiouAEIOUaeiouAEIOUaeiouAEIOUaeiouAEIOU"
        result = LlamaIndexElastic._strip_accents(input_text)
        self.assertEqual(result, expected_text)

    def test_initialize_metadata(self):
        """Test the _initialize_metadata method."""
        doc = {"meta": {}}
        txt_path = "txt_path/file.txt"
        doc_url = "doc_url.txt"
        csv = False
        do_titles = True
        do_tables = True

        self.vector_db._initialize_metadata(doc, txt_path, doc_url, csv, do_titles, do_tables)

        meta = doc["meta"]
        self.assertIn("uri", meta)
        self.assertIn("sections_headers", meta)
        self.assertIn("tables", meta)
        self.assertIn("filename", meta)
        self.assertIn("_header_mapping", meta)
        self.assertIn("_csv_path", meta)

    @patch("vector_storages.langdetect.detect", return_value="en")
    @patch("vector_storages.Document")
    def test_get_documents_from_dataframe(self, mock_document, mock_langdetect):
        """Test the _get_documents_from_dataframe method."""
        df = pd.DataFrame([{"text": "sample text", "Url": "sample_url"}])
        markdown_txts = [None]
        txt_path = "txt_path"
        csv = False
        do_titles = True
        do_tables = True

        result = self.vector_db._get_documents_from_dataframe(df, markdown_txts, txt_path, csv, do_titles, do_tables)

        self.assertTrue(len(result) > 0)
        mock_document.assert_called()


    @patch("vector_storages.time.sleep", return_value=None)
    @patch("vector_storages.VectorStoreIndex")
    @patch("vector_storages.StorageContext.from_defaults")
    def test_write_nodes(self, mock_storage_context, mock_vector_index, mock_sleep):
        """Test the _write_nodes method."""
        nodes_per_doc = [["node1", "node2"]]
        embed_model = MagicMock()
        vector_store = MagicMock()
        models = []
        index_name = "test_index"

        result = self.vector_db._write_nodes(nodes_per_doc, embed_model, vector_store, models, index_name)

        self.assertEqual(result, 1)
        mock_vector_index.return_value.insert_nodes.assert_called()

    @patch("time.sleep", return_value=None)
    def test_write_nodes_connection_error(self, mock_sleep):
        instance = self.vector_db
        nodes_per_doc = []
        embed_model = MagicMock()
        vector_store = MagicMock()
        models = []
        index_name = "test_index"

        with pytest.raises(ConnectionError, match="Max num of retries reached while indexing"):
            instance._write_nodes(nodes_per_doc, embed_model, vector_store, models, index_name, delta=3, max_retries=3)

    @patch("time.sleep", return_value=None)
    def test_write_nodes_bulk_index_error(self, mock_sleep):
        nodes_per_doc = [[MagicMock(metadata={'filename': 'file1'})]]
        embed_model = MagicMock()
        vector_store = MagicMock()
        models = MagicMock()
        index_name = "test_index"

        with patch("vector_storages.VectorStoreIndex.insert_nodes", side_effect=BulkIndexError("Test", [])):
            with self.assertRaises(ConnectionError):
                self.vector_db._write_nodes(nodes_per_doc, embed_model, vector_store, models, index_name)

        self.vector_db.logger.warning.assert_called()
        mock_sleep.assert_called()

    @patch("time.sleep", return_value=None)
    def test_write_nodes_timeout_exception(self, mock_sleep):
        nodes_per_doc = [[MagicMock(metadata={'filename': 'file3'})]]
        embed_model = MagicMock()
        vector_store = MagicMock()
        models = MagicMock()
        index_name = "test_index"

        with patch("vector_storages.VectorStoreIndex.insert_nodes", side_effect=TimeoutException("Timeout")):
            with self.assertRaises(ConnectionError):
                self.vector_db._write_nodes(nodes_per_doc, embed_model, vector_store, models, index_name)

        self.vector_db.logger.warning.assert_called()
        mock_sleep.assert_called_with(50)

    @patch("time.sleep", return_value=None)
    def test_manage_indexing_exception(self, mock_sleep):
        """Test the _manage_indexing_exception method."""
        index_name = "test_index"
        models = [{"embedding_model": "test_model"}]
        docs_filenames = ["file1", "file2"]

        self.vector_db._manage_indexing_exception(index_name, models, docs_filenames)

        self.connector.delete_documents.assert_called()

    @patch("time.sleep", return_value=None)
    def test_manage_indexing_exception_with_failures(self, mock_sleep):

        self.connector_mock.delete_documents.return_value = MagicMock(
            body={'failures': ['some_failure'], 'deleted': 0}
        )

        self.lama_index._manage_indexing_exception('test_index', [{'embedding_model': 'test_model'}], ['test_file'])

        self.logger_mock.debug.assert_called_with("Result deleting documents in index test_index_test_model: Error deleting documents")

    @patch("time.sleep", return_value=None)
    def test_manage_indexing_exception_with_no_deletions(self, mock_sleep):
        self.connector_mock.delete_documents.return_value = MagicMock(
            body={'failures': [], 'deleted': 0}
        )

        self.lama_index._manage_indexing_exception('test_index', [{'embedding_model': 'test_model'}], ['test_file'])

        self.logger_mock.debug.assert_called_with("Result deleting documents in index test_index_test_model: Documents not found")


class TestManagerVectorDB(unittest.TestCase):
    def setUp(self):
        self.conf_invalid = {"type": "InvalidType", "connector": MagicMock(), "workspace": "workspace", "origin": "origin", "aws_credentials": {}}

    @patch("vector_storages.ManagerVectorDB.MODEL_TYPES", [LlamaIndexElastic])
    def test_get_vector_database(self):
        """Test get_vector_database."""
        conf = {"type": "elastic", "connector": MagicMock(), "workspace": "workspace",
                "origin": "origin", "aws_credentials": {}}
        vector_db = ManagerVectorDB.get_vector_database(conf)
        self.assertIsInstance(vector_db, LlamaIndexElastic)

    def test_get_vector_database_with_invalid_type(self):
        """Test that verifies that an exception is thrown when an invalid type is passed."""
        with self.assertRaises(PrintableGenaiError) as cm:
            ManagerVectorDB.get_vector_database(self.conf_invalid)

        self.assertEqual(cm.exception.status_code, 400)
        self.assertIn("Platform type doesnt exist", str(cm.exception))
        self.assertIn("Possible values", str(cm.exception))

    def test_get_possible_platforms(self):
        """Test get_possible_platforms."""
        platforms = ManagerVectorDB.get_possible_vector_databases()
        self.assertIn("elastic", platforms)

    def test_get_vector_database_with_valid_type(self):
        """Test that verifies that a valid type returns the correct instance."""
        conf = {"type": "elastic", "connector": MagicMock(), "workspace": "workspace",
                "origin": "origin", "aws_credentials": {}}
        vector_db = ManagerVectorDB.get_vector_database(conf)
        self.assertIsInstance(vector_db, LlamaIndexElastic)

    def test_get_possible_vector_databases(self):
        """Test that verifies the possible vector databases are returned correctly."""
        possible_databases = ManagerVectorDB.get_possible_vector_databases()
        self.assertIn("elastic", possible_databases)
        self.assertIn("ai_search", possible_databases)





@pytest.fixture
def mock_connector():
    return MagicMock()


@pytest.fixture
def mock_parser():
    return MagicMock(
        txt_path="dummy_path",
        csv=False,
        do_titles=True,
        do_tables=True,
        index="test_index",
        models=[{'embedding_model': 'model1'}],
        vector_storage={"vector_storage_host": "dummy_host"},
        process_type="test_process",
        specific={"document": {"n_pags": 1}},
        chunking_method={}
    )


@pytest.fixture
def sample_dataframe():
    return pd.DataFrame({
        'text': ['hello world', 'こんにちは世界'],
        'Url': ['doc1.txt', 'doc2.txt'],
        'CategoryId': [1, 2],
        'meta1': ['m1', 'm2']
    })


@pytest.fixture
def markdown_files():
    return [True, False]


@patch("vector_storages.langdetect.detect", side_effect=["en", "ja"])
@patch("vector_storages.LlamaIndexAzureAI._initialize_metadata")
def test_get_documents_from_dataframe(mock_meta, mock_lang, mock_connector, sample_dataframe, markdown_files):
    instance = LlamaIndexAzureAI(mock_connector, "workspace", "azure", {})
    result = instance._get_documents_from_dataframe(sample_dataframe, markdown_files, "dummy_path", False, True, True)
    
    assert isinstance(result[0], Document)
    assert result[0].text == "hello world"
    assert "meta1" in result[0].metadata


def test_is_vector_database_type():
    assert VectorDB.is_vector_database_type("VectorDB")
    assert not VectorDB.is_vector_database_type("OtherDB")


def test_strip_accents():
    s = "áéíóú"
    expected = "aeiou"
    assert VectorDB._strip_accents(s) == expected


@patch("vector_storages.get_exc_info", return_value="trace")
def test_get_processed_data_connection_error(mock_exc, mock_connector, mock_parser, sample_dataframe, markdown_files):
    instance = LlamaIndexAzureAI(mock_connector, "workspace", "azure", {})
    
    with patch("vector_storages.langdetect.detect", return_value="en"):
        with patch.object(instance, '_initialize_metadata'):
            with pytest.raises(Exception):
                instance.get_processed_data(mock_parser, sample_dataframe, markdown_files)


@patch("vector_storages.LlamaIndexAzureAI._write_nodes", return_value=1)
@patch("vector_storages.get_embed_model")
@patch("vector_storages.AzureAISearchVectorStore")
@patch("vector_storages.Settings")
def test_index_documents(mock_settings, mock_store, mock_embed, mock_write, mock_connector, mock_parser, sample_dataframe, markdown_files):
    instance = LlamaIndexAzureAI(mock_connector, "workspace", "azure", {})
    
    with patch.object(instance, '_get_documents_from_dataframe') as mock_docs, \
         patch("vector_storages.ManagerChunkingMethods.get_chunking_method") as mock_chunk:

        doc = Document(text="sample text", metadata={"filename": "doc.txt"})
        mock_docs.return_value = [doc]
        mock_chunk.return_value.get_chunks.return_value = [[doc]]

        reports = instance.index_documents([doc], mock_parser)
        assert isinstance(reports, list)


@patch("vector_storages.StorageContext.from_defaults")
@patch("vector_storages.VectorStoreIndex")
def test_write_nodes_success(mock_index, mock_storage, mock_connector):
    mock_node = Document(text="test", metadata={"filename": "test.txt"})
    vector_store = MagicMock()
    embed_model = MagicMock()
    instance = LlamaIndexAzureAI(mock_connector, "workspace", "azure", {})

    mock_index.return_value.insert_nodes.return_value = None

    result = instance._write_nodes([[mock_node]], embed_model, vector_store, [{}], "test_index")
    assert result == 1


@patch("time.sleep", return_value=None)
@patch("vector_storages.LlamaIndexAzureAI._manage_indexing_exception")
@patch("vector_storages.StorageContext.from_defaults")
@patch("vector_storages.VectorStoreIndex")
def test_write_nodes_exception(mock_sleep, mock_index, mock_storage, mock_manage, mock_connector):
    mock_node = Document(text="test", metadata={"filename": "test.txt"})
    vector_store = MagicMock()
    embed_model = MagicMock()
    instance = LlamaIndexAzureAI(mock_connector, "workspace", "azure", {})

    mock_index.side_effect = Exception("error")

    with pytest.raises(ConnectionError):
        instance._write_nodes([[mock_node]], embed_model, vector_store, [{"embedding_model": "m1"}], "test_index")



@patch("time.sleep", return_value=None)
def test_manage_indexing_exception(mock_sleep, mock_connector):
    instance = LlamaIndexAzureAI(mock_connector, "workspace", "azure", {})
    mock_connector.delete_documents.return_value.body = {"deleted": 1, "failures": []}

    instance._manage_indexing_exception("test_index", [{"embedding_model": "test_model"}], ["doc.txt"])

