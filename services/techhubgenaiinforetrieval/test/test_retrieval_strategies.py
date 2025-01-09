import unittest

import pytest
from unittest.mock import MagicMock, patch, AsyncMock, Mock

from services.common.ir.parsers import ManagerParser
from retrieval_strategies import (
    SimpleStrategy,
    GenaiStrategy,
    GenaiRecursiveStrategy,
    GenaiSurroundingStrategy,
    LlamaIndexFusionStrategy,
    ManagerRetrievalStrategies,
)
from llama_index.core.schema import NodeWithScore, BaseNode, TextNode
from common.errors.genaierrors import PrintableGenaiError


ada_002_germany = {
    'embedding_model_name': 'ada-002-germany',
    'embedding_model': 'text-embedding-ada-002',
    'azure_api_version': '2022-12-01',
    'azure_deployment_name': 'ada-002-germany',
    'zone': 'techhubinc-GermanyWestCentral',
    'model_pool': ['ada-pool'],
    'platform': 'azure'
}


models_credentials = {"URLs": {"AZURE_EMBEDDINGS_URL": "https://$ZONE.openai.azure.com/"},
                    "api-keys": {"azure": {
                        "techhubinc-GermanyWestCentral": "test_key",
                        "techhubinc-AustraliaEast": "test_key"}}
                      }

documents_ada = [
    NodeWithScore(node=TextNode(text="Test", metadata={"filename": "test", "snippet_id": "0"}), score=0.5),
    NodeWithScore(node=TextNode(text="Test-2", metadata={"filename": "test-2", "snippet_id": "1"}), score=0.55),
    NodeWithScore(node=TextNode(text="Test-2", metadata={"filename": "test-2", "snippet_id": "1"}), score=0.7),
    NodeWithScore(node=TextNode(text="Test-4", metadata={"filename": "test-4", "snippet_id": "3"}), score=0.34)
]
documents_bm25 = [
    NodeWithScore(node=TextNode(text="Test", metadata={"filename": "test", "snippet_id": "0", "window": "window_test"}), score=0.23),
    NodeWithScore(node=TextNode(text="Test-2", metadata={"filename": "test-2", "snippet_id": "1", "window": "window_test"}), score=0.778),
    NodeWithScore(node=TextNode(text="Test-2", metadata={"filename": "test-2", "snippet_id": "1", "window": "window_test"}), score=0.72)
]
def get_connector():
    connector = MagicMock(scheme="https", host="localhost", port=9200, username="test", password="test")
    connector.exist_index.return_value = True
    connector.close.return_value = None
    connector.get_documents_filenames.return_value = ('finished', [{'filename': 'test', 'chunks': 1},
                                                                  {'filename': 'test2', 'chunks': 1}], 200)
    connector.get_documents.return_value = ('finished', ['doc1', 'doc2'], 200)
    magic_object = MagicMock(body={'failures': [], 'deleted': 2})
    magic_object.get.return_value = 2
    connector.delete_documents.return_value = magic_object
    connector.delete_index.return_value = True
    connector.assert_correct_chunking_method = MagicMock(return_value=None)
    connector.list_indices.return_value = (['index1_model1', 'index2_model2', 'index2_model2'])
    connector.get_full_index.return_value =([{
    "_source": {
        "content": "Origenes y formacion\r\n\r\nLionel Andres Messi nacio el 24 de junio de 1987 en el Hospital Italiano Garibaldi de la ciudad de Rosario, en la provincia de Santa Fe. Es el tercer hijo de Jorge Horacio Messi y Celia Maria Cuccittini.",
        "metadata": {
            "uri": "https://storage.com",
            "sections_headers": "",
            "tables": "",
            "filename": "PEPSICO_2022_10K.pdf",
            "document_id": "4a5da574-d129-4209-bcb6-64ee1b26881a",
            "snippet_number": 0.1,
            "snippet_id": "6f824c85589e89106c3af9a4ecb2944a",
            "index_id": "5ef6f3127b1501cf42454ea7b9cf2623",
            "_node_content": "{\"id_\": \"6f824c85589e89106c3af9a4ecb2944a\", \"embedding\": null, \"metadata\": {\"uri\": \"https://storage.com\", \"sections_headers\": \"\", \"tables\": \"\", \"filename\": \"PEPSICO_2022_10K.pdf\", \"document_id\": \"4af046e6-6763-41bf-9c0e-96a110a21799\", \"snippet_number\": 0.1, \"snippet_id\": \"6f824c85589e89106c3af9a4ecb2944a\", \"index_id\": \"5ef6f3127b1501cf42454ea7b9cf2623\"}, \"excluded_embed_metadata_keys\": [\"uri\", \"sections_headers\", \"tables\", \"filename\", \"document_id\", \"snippet_number\", \"snippet_id\", \"index_id\"], \"excluded_llm_metadata_keys\": [\"uri\", \"sections_headers\", \"tables\", \"filename\", \"document_id\", \"snippet_number\", \"snippet_id\", \"index_id\"], \"relationships\": {\"1\": {\"node_id\": \"4a5da574-d129-4209-bcb6-64ee1b26881a\", \"node_type\": \"4\", \"metadata\": {\"uri\": \"https://storage.com\", \"sections_headers\": \"\", \"tables\": \"\", \"filename\": \"PEPSICO_2022_10K.pdf\", \"_header_mapping\": \"\", \"_csv_path\": \"\", \"document_id\": \"4af046e6-6763-41bf-9c0e-96a110a21799\"}, \"hash\": \"134533b6e8178e5cb67c0d9f25280a5065daf9bc21abffa2f1bd5285187fe199\", \"class_name\": \"RelatedNodeInfo\"}}, \"text\": \"\", \"mimetype\": \"text/plain\", \"start_char_idx\": 0, \"end_char_idx\": 1626, \"text_template\": \"{metadata_str}\\n\\n{content}\", \"metadata_template\": \"{key}: {value}\", \"metadata_seperator\": \"\\n\", \"index_id\": \"5ef6f3127b1501cf42454ea7b9cf2623\", \"obj\": null, \"class_name\": \"IndexNode\"}",
            "_node_type": "IndexNode",
            "doc_id": "4a5da574-d129-4209-bcb6-64ee1b26881a",
            "ref_doc_id": "4a5da574-d129-4209-bcb6-64ee1b26881a"},
            "embedding": [-0.040039062, -0.020462036 ]
        },
    "_id": "6f824c85589e89106c3af9a4ecb2944a"
},
{
    "_source": {
        "content": "Origenes y formacion\r\n\r\nLionel Andres Messi nacio el 24 de junio de 1987 en el Hospital Italiano Garibaldi de la ciudad de Rosario, en la provincia de Santa Fe. Es el tercer hijo de Jorge Horacio Messi y Celia Maria Cuccittini.",
        "metadata": {
            "uri": "https://storage.com",
            "sections_headers": "",
            "tables": "",
            "filename": "PEPSICO_2022_10K.pdf",
            "document_id": "4a5da574-d129-4209-bcb6-64ee1b26881a",
            "snippet_number": 0.1,
            "snippet_id": "6f824c85589e89106c3af9a4ecb2943a",
            "index_id": "6f824c85589e89106c3af9a4ecb2943a",
            "_node_content": "{\"id_\": \"6f824c85589e89106c3af9a4ecb2943a\", \"embedding\": null, \"metadata\": {\"uri\": \"https://storage.com\", \"sections_headers\": \"\", \"tables\": \"\", \"filename\": \"PEPSICO_2022_10K.pdf\", \"document_id\": \"4af046e6-6763-41bf-9c0e-96a110a21799\", \"snippet_number\": 0.1, \"snippet_id\": \"6f824c85589e89106c3af9a4ecb2944a\", \"index_id\": \"5ef6f3127b1501cf42454ea7b9cf2623\"}, \"excluded_embed_metadata_keys\": [\"uri\", \"sections_headers\", \"tables\", \"filename\", \"document_id\", \"snippet_number\", \"snippet_id\", \"index_id\"], \"excluded_llm_metadata_keys\": [\"uri\", \"sections_headers\", \"tables\", \"filename\", \"document_id\", \"snippet_number\", \"snippet_id\", \"index_id\"], \"relationships\": {\"1\": {\"node_id\": \"4a5da574-d129-4209-bcb6-64ee1b26881a\", \"node_type\": \"4\", \"metadata\": {\"uri\": \"https://storage.com\", \"sections_headers\": \"\", \"tables\": \"\", \"filename\": \"PEPSICO_2022_10K.pdf\", \"_header_mapping\": \"\", \"_csv_path\": \"\", \"document_id\": \"4af046e6-6763-41bf-9c0e-96a110a21799\"}, \"hash\": \"134533b6e8178e5cb67c0d9f25280a5065daf9bc21abffa2f1bd5285187fe199\", \"class_name\": \"RelatedNodeInfo\"}}, \"text\": \"\", \"mimetype\": \"text/plain\", \"start_char_idx\": 0, \"end_char_idx\": 1626, \"text_template\": \"{metadata_str}\\n\\n{content}\", \"metadata_template\": \"{key}: {value}\", \"metadata_seperator\": \"\\n\", \"index_id\": \"5ef6f3127b1501cf42454ea7b9cf2623\", \"obj\": null, \"class_name\": \"IndexNode\"}",
            "_node_type": "IndexNode",
            "doc_id": "4a5da574-d129-4209-bcb6-64ee1b26881a",
            "ref_doc_id": "4a5da574-d129-4209-bcb6-64ee1b26881a"},
            "embedding": [-0.040039062, -0.020462036 ]
        },
    "_id": "6f824c85589e89106c3af9a4ecb2943a"
}])
    return connector


@pytest.fixture
def mock_node_with_score():
    """Fixture to mock a NodeWithScore instance."""
    node = MagicMock(spec=NodeWithScore)
    node.metadata = {"snippet_id": "123"}
    node.score = 1.0
    return node

@pytest.fixture
def mock_vector_store():
    """Fixture to mock a vector store."""
    vector_store = MagicMock()
    return vector_store

@pytest.fixture
def mock_embed_model():
    """Fixture to mock an embedding model."""
    embed_model = MagicMock()
    return embed_model

@pytest.fixture
def mock_input_object():
    """Fixture to mock an input object."""
    return MagicMock(
        query="test query",
        filters={"key": "value"},
        top_k=5,
        rescoring_function=MagicMock(),
        index="test_index",
        strategy_mode="test_mode",
    )

@pytest.fixture
def retrievers_arguments(mock_vector_store, mock_embed_model):
    """Fixture to mock retrievers arguments."""
    return [
        (mock_vector_store, mock_embed_model, [0.1, 0.2, 0.3], "bm25--score"),
        (mock_vector_store, mock_embed_model, None, "vector--score"),
    ]


# Tests for SimpleStrategy----------------------------
def test_simple_strategy_generate_llama_filters():
    filters = {"key": "value", "list_key": ["val1", "val2"]}
    result = SimpleStrategy.generate_llama_filters(filters)
    assert result.filters[0].filters[0].key == "key"
    assert result.filters[0].filters[0].value == "value"
    assert result.filters[1].filters[0].key == "list_key"
    assert len(result.filters[1].filters) == 2

def test_simple_strategy_is_strategy_type():
    assert SimpleStrategy.is_strategy_type("SimpleStrategy")
    assert not SimpleStrategy.is_strategy_type("AnotherStrategy")


# Tests for GenaiStrategy--------------------------

class MockDoc:
    def __init__(self, metadata, score=0):
        self.metadata = metadata
        self.score = score

def test_genai_strategy_add_retrieved_document(mock_node_with_score):
    docs = {}
    GenaiStrategy.add_retrieved_document(docs, mock_node_with_score, "bm25")
    assert "123" in docs
    assert docs["123"].metadata["bm25"] == 1.0


def test_genai_strategy_add_retrieved_document_existing_document(mock_node_with_score):
    docs = {
        "123": mock_node_with_score
    }

    mock_node_with_score.metadata["snippet_id"] = "123"
    mock_node_with_score.metadata["bm25"] = 1.0
    mock_node_with_score.score = 1.0

    GenaiStrategy.add_retrieved_document(docs, mock_node_with_score, "new_retriever")

    assert "123" in docs
    assert docs["123"].metadata["new_retriever"] == 1.0
    assert docs["123"].score == 2.0


def test_genai_strategy_get_ids_empty_scores():
    ids_dict = {
        "bm25": [MockDoc({"snippet_id": "123"}), MockDoc({"snippet_id": "124"})]
    }
    all_ids = {"123", "124", "125"}
    result = GenaiStrategy.get_ids_empty_scores(ids_dict, all_ids)
    expected = {"bm25": ["125"]}
    assert result == expected

def test_genai_strategy_do_retrieval_strategy(mock_input_object, retrievers_arguments):
    strategy = GenaiStrategy()

    mock_input_object.query = "test query"
    mock_input_object.rescoring_function = "length"
    mock_input_object.unique_docs = {"doc1": {}, "doc2": {}}

    retrievers_arguments = [
        (retrievers_arguments[0][0], retrievers_arguments[0][1], [0.1, 0.2, 0.3], "bm25--score"),
        (retrievers_arguments[1][0], retrievers_arguments[1][1], None, "vector--score")
    ]
    strategy.basic_genai_retrieval = MagicMock()

    result = strategy.do_retrieval_strategy(mock_input_object, retrievers_arguments)

    assert result is not None
    assert isinstance(result, list)

@pytest.fixture
def mock_docs():
    class MockDocument:
        def __init__(self):
            self.metadata = {}
            self.score = 1.0

    return {
        "doc1": MockDocument(),
        "doc2": MockDocument(),
        "doc3": MockDocument()
    }

def test_complete_empty_scores( mock_node_with_score, mock_vector_store, mock_embed_model, mock_input_object, retrievers_arguments):
    retriever = GenaiStrategy()

    mock_docs_by_retrieval = {
        "bm25--score": [mock_node_with_score],
        "vector--score": [["", 1]]
    }

    mock_unique_docs = {
        "123": mock_node_with_score
    }

    mock_retrievers = ["bm25--score", "vector--score"]
    retriever.get_ids_empty_scores = MagicMock()

    retriever.complete_empty_scores(mock_docs_by_retrieval, mock_unique_docs, retrievers_arguments, mock_input_object, mock_retrievers)


# Tests for GenaiRecursiveStrategy----------------------------
def test_genai_recursive_strategy_recursive_retrieval(mock_embed_model):
    strategy = GenaiRecursiveStrategy(connector=MagicMock())
    with patch('llama_index.core.retrievers.RecursiveRetriever.retrieve') as mock_retriever, \
        patch('retrieval_strategies.VectorStoreIndex'):
        mock_retriever.return_value = documents_ada
        result = strategy.recursive_retrieval(
            embed_model=mock_embed_model,
            retriever_type="vector--score",
            top_k=5,
            docs={},
            embed_query=None,
            query="test query",
            all_nodes_dict={},
            all_nodes=[],
        )
        assert isinstance(result, list)

def test_genai_recursive_do_retrieval_strategy():
    strategy = GenaiRecursiveStrategy(connector=get_connector())
    json_input = {
        "project_conf": {
            "x-tenant": "develop",
            "x-department": "main",
            "x-reporting": "",
        },
        "index_conf": {
            "top_k": 1,
            "filters": {},
            "query": "query",
            "index": "test",
            "strategy": "surrounding_genai_retrieval",
            "rescoring_function": "mean",
            "models": ["bm25"]
        }
    }
    with patch('llama_index.core.retrievers.RecursiveRetriever.retrieve') as mock_retriever:
        mock_retriever.return_value = documents_ada
        retrievers_arguments = [
            (MagicMock(), MagicMock(), "query", "bm25--score")
        ]
        input_object = ManagerParser().get_parsed_object({"type": "inforetrieval", "json_input": json_input,
                                                          "available_models": [ada_002_germany],
                                                          "available_pools": {'ada-pool': ['ada-002-germany']},
                                                          "models_credentials": models_credentials})
        result = strategy.do_retrieval_strategy(input_object=input_object, retrievers_arguments=retrievers_arguments)



# test for GenaiSurroundingStrategy-----------------------------------------


def test_genai_surrounding_strategy_do_retrieval_strategy():
    strategy = GenaiSurroundingStrategy()

    json_input = {
        "project_conf": {
            "x-tenant": "develop",
            "x-department": "main",
            "x-reporting": "",
        },
        "index_conf": {
            "top_k": 1,
            "filters": {},
            "query": "query",
            "index": "test",
            "strategy": "surrounding_genai_retrieval",
            "rescoring_function": "mean",
            "models": ["bm25"]
        }
    }
    input_object = ManagerParser().get_parsed_object({"type": "inforetrieval", "json_input": json_input,
                                                      "available_models": [ada_002_germany],
                                                      "available_pools": {'ada-pool': ['ada-002-germany']},
                                                      "models_credentials": models_credentials})
    retrievers_arguments = [
        (MagicMock(), MagicMock(), "query", "bm25--score"),
        (MagicMock(), MagicMock(), [0.1231, 0.2323], "ada--score")
    ]
    strategy.basic_genai_retrieval = MagicMock()

    with patch('llama_index.core.retrievers.RecursiveRetriever.retrieve') as mock_retriever:
        mock_retriever.return_value = documents_bm25
        result = strategy.do_retrieval_strategy(input_object=input_object, retrievers_arguments=retrievers_arguments)



# Tests for LlamaIndexFusionStrategy----------------------------------------
class TestLlamaIndexFusionStrategy(unittest.TestCase):

    def setUp(self):
        """Setup para los tests"""
        self.strategy = LlamaIndexFusionStrategy()

    @patch('retrieval_strategies.VectorStoreIndex.from_vector_store')
    @patch('retrieval_strategies.QueryFusionRetriever')
    @patch('retrieval_strategies.MockLLM')
    def test_do_retrieval_strategy_success(self, MockLLM, QueryFusionRetriever, from_vector_store):
        """Prueba exitosa del método do_retrieval_strategy"""
        # Mocking dependencies
        mock_vector_store_index = MagicMock()
        mock_vector_store_index.as_retriever.return_value = MagicMock()
        from_vector_store.return_value = mock_vector_store_index

        mock_retriever = MagicMock()
        QueryFusionRetriever.return_value = mock_retriever
        mock_retriever.retrieve.return_value = ["mocked_result"]

        # Mock input_object y retrievers_arguments
        input_object = MagicMock()
        input_object.filters = {"mock_filter": "value"}
        input_object.top_k = 5
        input_object.strategy_mode = "mock_mode"
        input_object.query = "mock_query"

        retrievers_arguments = [
            ("vector_store_1", "embed_model_1", None, "retriever_type_1"),
            ("vector_store_2", "embed_model_2", None, "retriever_type_2"),
        ]

        # Call method
        result = self.strategy.do_retrieval_strategy(input_object, retrievers_arguments)

        # Assertions
        self.assertEqual(result, ["mocked_result"])
        self.assertEqual(mock_vector_store_index.as_retriever.call_count, 2)
        QueryFusionRetriever.assert_called_once()
        mock_retriever.retrieve.assert_called_once_with("mock_query")

    @patch('retrieval_strategies.VectorStoreIndex.from_vector_store', side_effect=Exception("Mocked Error"))
    def test_do_retrieval_strategy_vector_store_error(self, from_vector_store):
        """Prueba manejo de errores al inicializar VectorStoreIndex"""
        input_object = MagicMock()
        retrievers_arguments = [("vector_store_1", "embed_model_1", None, "retriever_type_1")]

        with self.assertRaises(Exception) as context:
            self.strategy.do_retrieval_strategy(input_object, retrievers_arguments)

        self.assertEqual(str(context.exception), "Mocked Error")
        from_vector_store.assert_called_once()

if __name__ == "__main__":
    unittest.main()



# Tests for ManagerRetrievalStrategies
def test_manager_retrieval_strategies_get_retrieval_strategy():
    conf = {"strategy": "genai_retrieval"}
    strategy = ManagerRetrievalStrategies.get_retrieval_strategy(conf)
    assert isinstance(strategy, GenaiStrategy)

def test_manager_retrieval_strategies_get_possible_retrieval_strategies():
    strategies = ManagerRetrievalStrategies.get_possible_retrieval_strategies()
    assert "genai_retrieval" in strategies


def test_manager_retrieval_strategies_invalid_strategy():
    with pytest.raises(PrintableGenaiError):
        ManagerRetrievalStrategies.get_retrieval_strategy({"strategy": "non_existent_strategy"})
