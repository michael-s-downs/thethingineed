import unittest
from unittest.mock import patch, MagicMock
import pytest

from elastic_transport import ConnectionTimeout
from elasticsearch.helpers import BulkIndexError
from httpx import TimeoutException
from openai import RateLimitError, APIStatusError

from vector_storages import VectorDB, LlamaIndex, ManagerVectorDB
from elasticsearch.exceptions import ConnectionError as ElasticConnectionError
from common.errors.genaierrors import PrintableGenaiError
from common.utils import ELASTICSEARCH_INDEX
from common.ir.connectors import Connector
from common.ir.parsers import Parser
import pandas as pd
from typing import List

class MockVectorDB(VectorDB):
    def get_processed_data(self, io: Parser, df: pd.DataFrame, markdown_files: List) -> List:
        # Implementación simulada para pruebas
        processed_data = [f"Processed {file}" for file in markdown_files]
        return processed_data

    def index_documents(self, docs: List, io: Parser) -> List:
        # Implementación simulada para pruebas
        return [f"Indexed {doc}" for doc in docs]

class TextVectorDB(unittest.TestCase):
    def setUp(self):
        self.connector = MagicMock()
        self.workspace = "test_workspace"
        self.origin = "test_origin"
        self.aws_credentials = {"key": "test_key", "secret": "test_secret"}
        self.vector_db = VectorDB(self.connector, self.workspace, self.origin, self.aws_credentials)

    def test_get_processed_data(self):
        # Configuración
        io = MagicMock(spec=Parser)
        df = pd.DataFrame({"column": [1, 2, 3]})
        markdown_files = ["file1.md", "file2.md"]

        # Llamada al método
        result = self.vector_db.get_processed_data(io, df, markdown_files)

    def test_index_documents(self):
        # Configuración
        io = MagicMock(spec=Parser)
        docs = ["doc1", "doc2", "doc3"]

        # Llamada al método
        result = self.vector_db.index_documents(docs, io)

class TestLlamaIndex(unittest.TestCase):

    def setUp(self):
        self.connector = MagicMock(spec=Connector)
        self.workspace = "workspace_path"
        self.origin = "origin_path"
        self.aws_credentials = {"key": "dummy_key", "secret": "dummy_secret"}
        self.vector_db = LlamaIndex(self.connector, self.workspace, self.origin, self.aws_credentials)
        self.vector_db.logger = MagicMock()

        self.connector_mock = MagicMock()
        self.logger_mock = MagicMock()
        self.lama_index = LlamaIndex(connector=self.connector_mock, workspace='test_workspace', origin='test_origin', aws_credentials=None)
        self.lama_index.logger = self.logger_mock

    @patch.object(LlamaIndex, 'get_processed_data', return_value=['mock_document'])
    def test_call_get_processed_data(self, mock_get_processed_data):
        """Test that get_processed_data is called with correct parameters."""
        # Mock para Parser
        mock_parser = MagicMock()
        mock_parser.models = [{'embedding_model': 'test_model'}]
        mock_parser.index = 'test_index'

        # Crear un DataFrame y archivos markdown ficticios
        df = pd.DataFrame({'text': ['Test content'], 'Url': ['test_url']})
        markdown_files = [None]

        # Llamar al método get_processed_data
        result = self.vector_db.get_processed_data(mock_parser, df, markdown_files)

        # Verificar que el método fue llamado con los argumentos correctos
        mock_get_processed_data.assert_called_once_with(mock_parser, df, markdown_files)

        # Verificar el resultado esperado
        self.assertEqual(result, ['mock_document'])

    @patch.object(LlamaIndex, 'index_documents', return_value=[{'status': 'success'}])
    def test_call_index_documents(self, mock_index_documents):
        """Test that index_documents is called with correct parameters."""
        # Crear documentos simulados
        docs = [MagicMock()]
        mock_parser = MagicMock()

        # Llamar al método index_documents
        result = self.vector_db.index_documents(docs, mock_parser)

        # Verificar que el método fue llamado con los argumentos correctos
        mock_index_documents.assert_called_once_with(docs, mock_parser)

        # Verificar el resultado esperado
        self.assertEqual(result, [{'status': 'success'}])

    @patch("vector_storages.logging.getLogger")
    @patch("vector_storages.LoggerHandler")
    def test_vector_db_init(self, mock_logger_handler, mock_get_logger):
        """Test initialization of the VectorDB class."""
        logger_instance = MagicMock()
        mock_logger_handler.return_value.logger = logger_instance

        vector_db = LlamaIndex(self.connector, self.workspace, self.origin, self.aws_credentials)

        self.assertEqual(vector_db.connector, self.connector)
        self.assertEqual(vector_db.workspace, self.workspace)
        self.assertEqual(vector_db.origin, self.origin)
        self.assertEqual(vector_db.aws_credentials, self.aws_credentials)
        self.assertEqual(vector_db.logger, logger_instance)

    def test_is_platform_type(self):
        """Test the is_platform_type method."""
        self.assertTrue(LlamaIndex.is_vector_database_type("LlamaIndex"))
        self.assertFalse(LlamaIndex.is_vector_database_type("OtherType"))

    def test_get_processed_data_success(self):
        # Configuración de mocks
        mock_connector = MagicMock()
        mock_parser = MagicMock()
        mock_logger = MagicMock()

        # Crear instancia de LlamaIndex
        llama_index = LlamaIndex(mock_connector, workspace="test_workspace", origin="test_origin", aws_credentials={})
        llama_index.logger = mock_logger

        # Configuración de mocks de entrada
        mock_parser.models = [{'embedding_model': 'test_model'}]
        mock_parser.index = 'test_index'
        mock_parser.txt_path = 'test_path'
        mock_parser.csv = False
        mock_parser.do_titles = False
        mock_parser.do_tables = False

        df = pd.DataFrame({'text': ['Test content'], 'Url': ['test_url']})
        markdown_files = [None]

        # Mocks internos
        llama_index._get_documents_from_dataframe = MagicMock(return_value=['mock_document'])
        mock_connector.assert_correct_index_metadata = MagicMock()

        # Llamar a la función
        result = llama_index.get_processed_data(mock_parser, df, markdown_files)

        # Verificar resultados
        llama_index._get_documents_from_dataframe.assert_called_once()
        mock_connector.assert_correct_index_metadata.assert_called_once()
        assert result == ['mock_document']

    @patch("vector_storages.logging.getLogger")
    @patch("vector_storages.LoggerHandler")
    def test_get_processed_data_exception(self, mock_logger_handler, mock_get_logger):
        """Test the exception handling in get_processed_data when ElasticConnectionError is raised."""

        # Crear mocks
        mock_connector = MagicMock(spec=Connector)
        mock_logger = MagicMock()

        # Instanciar LlamaIndex
        llama_index = LlamaIndex(mock_connector, workspace="test_workspace", origin="test_origin", aws_credentials={})
        llama_index.logger = mock_logger

        # Configurar el mock para el Parser
        mock_parser = MagicMock()
        mock_parser.models = [{'embedding_model': 'test_model'}]
        mock_parser.index = 'test_index'
        mock_parser.txt_path = 'test_path'
        mock_parser.csv = False
        mock_parser.do_titles = False
        mock_parser.do_tables = False

        # Crear un DataFrame y una lista de archivos markdown
        df = pd.DataFrame({'text': ['Test content'], 'Url': ['test_url']})
        markdown_files = [None]

        # Simular el comportamiento de _get_documents_from_dataframe
        llama_index._get_documents_from_dataframe = MagicMock(return_value=['mock_document'])

        # Simular que se lanza una ElasticConnectionError
        mock_connector.assert_correct_index_metadata.side_effect = ElasticConnectionError("ElasticSearch connection failed.")

        # Verificar que se lance PrintableGenaiError y se registre el error
        with self.assertRaises(PrintableGenaiError) as cm:
            llama_index.get_processed_data(mock_parser, df, markdown_files)

        # Asegurarse de que el error se registró
        mock_logger.error.assert_called_with(
            "Connection to elastic failed. Check if the elastic service is running.",
            exc_info=True
        )

        # Verificar que el código de error y el mensaje de la excepción son correctos
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
        io.modify_index_docs = {}  # Configuración del atributo faltante
        io.scheme = {}  # Configuración del atributo faltante
        docs = [MagicMock()]

        # Mock para `self.connector`
        self.vector_db.connector.scheme = "http"
        self.vector_db.connector.host = "localhost"
        self.vector_db.connector.port = "9200"
        self.vector_db.connector.username = "test_user"  # Atributo agregado
        self.vector_db.connector.password = "test_pass"  # Atributo agregado

        # Simulamos que el índice NO existe
        self.vector_db.connector.exist_index = MagicMock(return_value=False)
        self.vector_db.connector.create_empty_index = MagicMock()

        # Mock para encoding
        self.vector_db.encoding.encode = MagicMock(return_value=["token1", "token2"])

        # Mock para chunking
        mock_get_chunking.return_value.get_chunks.return_value = [["chunk1"], ["chunk2"]]

        # Mock del modelo de embedding
        mock_embed_model.return_value = MagicMock()

        # Mock del storage context y vector index
        mock_storage_context.return_value = MagicMock()
        mock_vector_index.return_value.insert_nodes = MagicMock()

        # Mock para evitar que se realicen interacciones reales con ElasticsearchStore
        mock_es_store.return_value = MagicMock()

        # Ejecución del test
        result = self.vector_db.index_documents(docs, io)

        # Verificar que el método exist_index haya sido llamado y que se haya intentado crear el índice vacío
        self.vector_db.connector.exist_index.assert_called_once_with('test_index_test_model')
        self.vector_db.connector.create_empty_index.assert_called_once_with('test_index_test_model')

        # Asserts adicionales si es necesario
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
        result = LlamaIndex._strip_accents(input_text)
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

    @patch("vector_storages.modify_index_documents")
    def test_modify_index_docs(self, mock_modify_index_docs):
        """Test the _modify_index_docs method."""
        docs = [MagicMock()]
        modify_index_docs = {"key": "value"}
        index = "test_index"

        mock_modify_index_docs.return_value = docs

        result = self.vector_db._modify_index_docs(docs, modify_index_docs, index)

        self.assertEqual(result, docs)
        mock_modify_index_docs.assert_called_once()

    @patch("vector_storages.modify_index_documents")
    @patch("vector_storages.logging.getLogger")
    def test_modify_index_docs_handle_exception(self, mock_get_logger, mock_modify_index_docs):
        """Test the _modify_index_docs method when an exception is raised."""

        # Crear mocks de prueba
        docs = [MagicMock()]
        modify_index_docs = {"key": "value"}
        index = "test_index"

        # Configuración para simular una excepción en el mock
        mock_modify_index_docs.side_effect = IndexError("Simulated error")

        # Crear un mock para el logger
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger  # Asignamos el mock del logger

        # Llamar al método y verificar que se maneja la excepción
        with self.assertRaises(IndexError):
            self.vector_db._modify_index_docs(docs, modify_index_docs, index)

        # Verificar que modify_index_documents fue llamado con los parámetros correctos
        mock_modify_index_docs.assert_called_once_with(
            self.vector_db.connector, modify_index_docs, docs, index, self.vector_db.logger
        )

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

    def test_write_nodes_connection_error(self):
        # Configurar mocks
        instance = self.vector_db  # Reemplaza con la clase donde está `_write_nodes`
        nodes_per_doc = []
        embed_model = MagicMock()
        vector_store = MagicMock()
        models = []
        index_name = "test_index"

        # Ejecutar la función y verificar excepción
        with pytest.raises(ConnectionError, match="Max num of retries reached while indexing"):
            instance._write_nodes(nodes_per_doc, embed_model, vector_store, models, index_name, delta=3, max_retries=3)

    @patch("time.sleep", return_value=None)  # Mock para evitar demoras reales
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

    def test_manage_indexing_exception(self):
        """Test the _manage_indexing_exception method."""
        index_name = "test_index"
        models = [{"embedding_model": "test_model"}]
        docs_filenames = ["file1", "file2"]

        self.vector_db._manage_indexing_exception(index_name, models, docs_filenames)

        self.connector.delete_documents.assert_called()

    def test_manage_indexing_exception_with_failures(self):
        # Simulamos que el método `delete_documents` devuelve un resultado con errores en 'failures'
        self.connector_mock.delete_documents.return_value = MagicMock(
            body={'failures': ['some_failure'], 'deleted': 0}
        )

        # Llamamos al método
        self.lama_index._manage_indexing_exception('test_index', [{'embedding_model': 'test_model'}], ['test_file'])

        # Verificamos que el logger registra el mensaje correcto de error
        self.logger_mock.debug.assert_called_with("Result deleting documents in index test_index_test_model: Error deleting documents")

    def test_manage_indexing_exception_with_no_deletions(self):
        # Simulamos que el método `delete_documents` devuelve un resultado con 'deleted' igual a 0
        self.connector_mock.delete_documents.return_value = MagicMock(
            body={'failures': [], 'deleted': 0}
        )

        # Llamamos al método
        self.lama_index._manage_indexing_exception('test_index', [{'embedding_model': 'test_model'}], ['test_file'])

        # Verificamos que el logger registra el mensaje correcto cuando no se encuentran documentos
        self.logger_mock.debug.assert_called_with("Result deleting documents in index test_index_test_model: Documents not found")


class TestManagerVectorDB(unittest.TestCase):
    def setUp(self):
        self.conf_invalid = {"type": "InvalidType", "connector": MagicMock(), "workspace": "workspace", "origin": "origin", "aws_credentials": {}}

    @patch("vector_storages.ManagerVectorDB.MODEL_TYPES", [LlamaIndex])
    def test_get_vector_database(self):
        """Test get_vector_database."""
        conf = {"type": "LlamaIndex", "connector": MagicMock(), "workspace": "workspace",
                "origin": "origin", "aws_credentials": {}}
        vector_db = ManagerVectorDB.get_vector_database(conf)
        self.assertIsInstance(vector_db, LlamaIndex)

    def test_get_vector_database_with_invalid_type(self):
        """Test que verifica que se lanza una excepción cuando se pasa un tipo inválido."""
        # Verificamos que se lanza la excepción PrintableGenaiError con el tipo incorrecto
        with self.assertRaises(PrintableGenaiError) as cm:
            ManagerVectorDB.get_vector_database(self.conf_invalid)

        # Verificamos que el código de error sea 400 y que el mensaje de la excepción sea el esperado
        self.assertEqual(cm.exception.status_code, 400)
        self.assertIn("Platform type doesnt exist", str(cm.exception))
        self.assertIn("Possible values", str(cm.exception))

    def test_get_possible_platforms(self):
        """Test get_possible_platforms."""
        platforms = ManagerVectorDB.get_possible_vector_databases()
        self.assertIn("LlamaIndex", platforms)


if __name__ == "__main__":
    unittest.main()
