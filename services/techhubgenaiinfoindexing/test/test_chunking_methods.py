import unittest
from unittest.mock import MagicMock, patch

import tiktoken
import json

from chunking_methods import Simple, Recursive, SurroundingContextWindow, ManagerChunkingMethods, ChunkingMethod
from llama_index.core.schema import Document
from common.errors.genaierrors import PrintableGenaiError

class TestChunkingMethods(unittest.TestCase):

    def setUp(self):
        self.docs = [
            Document(text="This is a test document. It has several sentences.", metadata={"document_id": "dfgh"})
        ]
        self.encoding = MagicMock()
        self.encoding.encode = lambda text: text.split()

        self.node = MagicMock()
        self.node.text = "This is a test document with <pag_1_header_1> and <pag_1_table_1>"
        self.node.metadata = {}
        self.origin = "some_origin"
        self.sections = "section1||section2"

    @patch("chunking_methods.SentenceSplitter.get_nodes_from_documents")
    def test_simple_get_chunks(self, mock_splitter):
        mock_splitter.return_value = [MagicMock(text="chunk 1", metadata={}), MagicMock(text="chunk 2", metadata={})]

        method = Simple(window_length=5, window_overlap=1, origin=("origin"), workspace=("workspace"))
        result = method.get_chunks(self.docs, self.encoding)

        self.assertEqual(len(result[0]), 2)
        mock_splitter.assert_called_once()

    @patch("chunking_methods.SentenceSplitter.get_nodes_from_documents")
    @patch("os.getenv")
    def test_simple_get_chunks_testing_true(self, mock_getenv, mock_splitter):
        mock_getenv.return_value = "True"
        mock_splitter.return_value = [MagicMock(text="chunk 1", metadata={}), MagicMock(text="chunk 2", metadata={})]

        method = Simple(window_length=5, window_overlap=1, origin=("origin"), workspace=("workspace"))
        result = method.get_chunks(self.docs, self.encoding)

        self.assertEqual(len(result[0]), 2)
        mock_splitter.assert_called_once()

    @patch("chunking_methods.SentenceSplitter.get_nodes_from_documents")
    def test_recursive_get_chunks(self, mock_splitter):
        base_nodes = [MagicMock(text="base chunk", metadata={"index_id": "node-1"})]
        base_nodes[0].node_id = "node-1"
        base_nodes[0].dict.return_value = {
            "text": "base chunk",
            "metadata": {"index_id": "node-1"},
        }

        sub_nodes = [
            MagicMock(text="sub chunk 1", metadata={"index_id": "node-1"}),
            MagicMock(text="sub chunk 2", metadata={"index_id": "node-1"}),
        ]
        for idx, sub_node in enumerate(sub_nodes, start=1):
            sub_node.node_id = f"sub-node-{idx}"
            sub_node.dict.return_value = {
                "text": sub_node.text,
                "metadata": sub_node.metadata,
            }

        mock_splitter.side_effect = [base_nodes, sub_nodes]

        method = Recursive(window_length=10, window_overlap=2, origin=("origin"), workspace=("workspace"),
                           sub_window_length=5, sub_window_overlap=1)

        result = method.get_chunks(self.docs, self.encoding)

        self.assertEqual(len(result[0]), 3)  # 2 sub chunks + 1 base node
        self.assertEqual(result[0][0].metadata["index_id"], result[0][2].metadata["index_id"])

    @patch("chunking_methods.SentenceSplitter.get_nodes_from_documents")
    @patch("os.getenv")
    def test_recursive_get_chunks_testing_true(self, mock_getenv, mock_splitter):
        mock_getenv.return_value = "True"
        base_nodes = [MagicMock(text="base chunk", metadata={"index_id": "node-1"})]
        base_nodes[0].node_id = "node-1"
        base_nodes[0].dict.return_value = {
            "text": "base chunk",
            "metadata": {"index_id": "node-1"},
        }

        sub_nodes = [
            MagicMock(text="sub chunk 1", metadata={"index_id": "node-1"}),
            MagicMock(text="sub chunk 2", metadata={"index_id": "node-1"}),
        ]
        for idx, sub_node in enumerate(sub_nodes, start=1):
            sub_node.node_id = f"sub-node-{idx}"
            sub_node.dict.return_value = {
                "text": sub_node.text,
                "metadata": sub_node.metadata,
            }

        mock_splitter.side_effect = [base_nodes, sub_nodes]

        method = Recursive(window_length=10, window_overlap=2, origin=("origin"), workspace=("workspace"),
                           sub_window_length=5, sub_window_overlap=1)

        result = method.get_chunks(self.docs, self.encoding)

        self.assertEqual(len(result[0]), 3)  # 2 sub chunks + 1 base node
        self.assertEqual(result[0][0].metadata["index_id"], result[0][2].metadata["index_id"])

    @patch("chunking_methods.SentenceSplitter.get_nodes_from_documents")
    def test_surrounding_context_window_get_chunks(self, mock_splitter):
        mock_splitter.return_value = [MagicMock(text="chunk with context", metadata={})]

        method = SurroundingContextWindow(window_length=5, window_overlap=2, origin=("origin"), workspace=("workspace"), windows=1)
        result = method.get_chunks(self.docs, self.encoding)

        self.assertEqual(len(result[0]), 2)
        self.assertEqual(result[0][0].text, "This is a test document.")

    @patch("chunking_methods.SentenceSplitter.get_nodes_from_documents")
    @patch("os.getenv")
    def test_surrounding_context_window_get_chunks_testing_true(self, mock_getenv, mock_splitter):
        mock_getenv.return_value = "True"
        mock_splitter.return_value = [MagicMock(text="chunk with context", metadata={})]

        method = SurroundingContextWindow(window_length=5, window_overlap=2, origin=("origin"), workspace=("workspace"), windows=1)
        result = method.get_chunks(self.docs, self.encoding)

        self.assertEqual(len(result[0]), 2)
        self.assertEqual(result[0][0].text, "This is a test document.")

    def test_manager_get_chunking_method(self):
        conf_simple = {"method": "simple", "window_length": 5, "window_overlap": 1, "origin": ("origin"), "workspace": ("workspace")}
        conf_recursive = {"method": "recursive", "window_length": 10, "window_overlap": 2, "origin": ("origin"),
                         "workspace": ("workspace"), "sub_window_length": 5, "sub_window_overlap": 1}

        simple_method = ManagerChunkingMethods.get_chunking_method(conf_simple)
        recursive_method = ManagerChunkingMethods.get_chunking_method(conf_recursive)

        self.assertIsInstance(simple_method, Simple)
        self.assertIsInstance(recursive_method, Recursive)

    def test_manager_invalid_chunking_method(self):
        conf_invalid = {"method": "invalid"}
        with self.assertRaises(PrintableGenaiError):
            ManagerChunkingMethods.get_chunking_method(conf_invalid)

    def test_manager_get_possible_methods(self):
        methods = ManagerChunkingMethods.get_possible_chunking_methods()
        self.assertIn("simple", methods)
        self.assertIn("recursive", methods)
        self.assertIn("surrounding_context_window", methods)

    @patch("chunking_methods.load_file")
    def test_add_titles_and_tables_with_mapping_and_csv(self, mock_load_file):
        node = MagicMock()
        node.text = "This is a test document with <pag_1_header_1> and <pag_1_table_1>"
        node.metadata = {
            "_header_mapping": "header_mapping.json",
            "_csv_path": "csv_data/",
        }

        # Simulate loading the header mapping file
        mock_load_file.side_effect = [
            json.dumps({"pag_1_header_1": "Header 1"}).encode(),  # Header mapping
            b"CSV content for table 1",  # CSV file content
        ]

        origin = "origin_path"
        sections = "section1||section2"

        method = Simple(window_length=5, window_overlap=1, origin=("origin"), workspace=("workspace"))
        result_node, updated_sections = method._add_titles_and_tables(node, sections, origin)

        # Assertions for headers
        self.assertEqual(result_node.metadata["sections_headers"], "Header 1")
        self.assertEqual(result_node.text, "This is a test document with  and CSV content for table 1")
        self.assertEqual(updated_sections, "Header 1")

        # Assertions for CSV
        self.assertEqual(result_node.metadata["tables"], True)
        self.assertEqual(result_node.text, "This is a test document with  and CSV content for table 1")

        # Ensure load_file was called correctly
        mock_load_file.assert_any_call(origin, "header_mapping.json")
        mock_load_file.assert_any_call(origin, "csv_data/pag_1_table_1.csv")

    @patch("chunking_methods.load_file")
    def test_add_titles_and_tables_no_titles(self, mock_load_file):
        # Setup del nodo
        node = MagicMock()
        node.text = "This is a test document without any titles"
        node.metadata = {
            "_header_mapping": "header_mapping.json",
        }

        # Simula cargar el archivo de mapeo de encabezados
        mock_load_file.return_value = json.dumps({"pag_1_header_1": "Header 1"}).encode()

        origin = "origin_path"
        sections = "section1||section2"

        # Instancia y llamada al método
        method = Simple(window_length=5, window_overlap=1, origin=("origin"), workspace=("workspace"))
        result_node, updated_sections = method._add_titles_and_tables(node, sections, origin)

        # Verificaciones
        self.assertEqual(result_node.metadata["sections_headers"], "section2")
        self.assertEqual(result_node.text, "This is a test document without any titles")
        self.assertEqual(updated_sections, sections)

        # Verifica que load_file fue llamado correctamente
        mock_load_file.assert_called_once_with(origin, "header_mapping.json")


if __name__ == "__main__":
    unittest.main()
