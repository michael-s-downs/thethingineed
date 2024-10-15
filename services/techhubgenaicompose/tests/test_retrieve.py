import pytest
from compose.actions.retrieve import (
    RetrieveMethod,
    DoNothing,
    ChunksRetriever,
    DocumentsRetriever,
    RetrieverFactory,
)
import json
from common.errors.genaierrors import PrintableGenaiError
from unittest.mock import patch, MagicMock


@pytest.fixture
def streamlist_params():
    return {
        "streamlist": [
            {
                "content": "example content",
                "meta": {"field1": "value1"},
                "scores": {"bm25": 1, "sim-example": 0.9},
            }
        ]
    }


@pytest.fixture
def chunks_params():
    return {
        "generic": {
            "process_type": "ir_retrieve",
            "index_conf": {
                "task": "retrieve",
                "template_name": "system_query_and_context",
                "query": "example query",
            },
        },
        "credentials": {},
        "specific": {"dataset": {"dataset_key": ""}},
        "headers_config": {"Authorization": "Bearer test_token"},
    }


@pytest.fixture
def documents_params():
    return {
        "index": "example_index",
        "filters": {},
        "headers_config": {"Authorization": "Bearer test_token"},
    }


class RetrieveMethodTest(RetrieveMethod):
    def process(self):
        super().process()

    def _get_example(self):
        return super()._get_example()


class TestRetrieveMethod:
    # Test that an attempt to instantiate the abstract RetrieveMethod class raises a TypeError
    def test_retrieve_method_subclass(self):
        with pytest.raises(TypeError):
            RetrieveMethod(params={})

    # Test that the process method in the RetrieveMethodTest returns None
    def test_retrieve_method_process(self):
        retrieve_method = RetrieveMethodTest(params={})
        result = retrieve_method.process()
        assert result is None

    # Test that the _get_example method returns an empty dictionary
    def test_retrieve_method_get_example(self):
        retrieve_method = RetrieveMethodTest(params={})
        result = retrieve_method._get_example()
        assert result == {}


class TestDoNothing:
    # Test that the DoNothing class returns the original streamlist
    def test_do_nothing(self, streamlist_params):
        do_nothing = DoNothing(streamlist_params)
        result = do_nothing.process()
        assert result == streamlist_params["streamlist"]

    # Test that the get_example method returns the correct example type
    def test_do_nothing_get_example(self):
        do_nothing = DoNothing({})
        example = json.loads(do_nothing.get_example())
        assert example["type"] == "streamlist"


class TestChunksRetriever:
    @patch("requests.post")
    # Test that the ChunksRetriever processes a valid request and returns the expected document
    def test_chunks_retriever(self, mock_post, chunks_params):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "result": {
                "docs": [
                    {
                        "content": "example content",
                        "meta": {"field1": "value1", "bm25--score": 0.9},
                        "score": 1,
                        "answer": "example answer",
                    }
                ]
            }
        }
        mock_post.return_value = mock_response

        chunks_retriever = ChunksRetriever(chunks_params)
        result = chunks_retriever.process()
        assert len(result) == 1
        assert result[0]["content"] == "example content"
        assert result[0]["meta"] == {"field1": "value1"}
        assert result[0]["score"] == 1
        assert result[0]["answer"] == "example answer"

    @patch("requests.post")
    # Test that the ChunksRetriever raises an error when the query is empty
    def test_chunks_retriever_empty_query(self, mock_post, chunks_params):
        chunks_params["generic"]["index_conf"]["query"] = ""
        chunks_retriever = ChunksRetriever(chunks_params)

        with pytest.raises(PrintableGenaiError) as excinfo:
            chunks_retriever.process()

        assert excinfo.value.status_code == 400
        assert "Query is empty" in str(excinfo.value)

    # Test that the get_example method returns the correct example type for chunks retriever
    def test_chunks_retriever_get_example(self):
        chunks_retriever = ChunksRetriever({})
        example = json.loads(chunks_retriever.get_example())
        assert example["type"] == "get_chunks"

    @patch("requests.post")
    # Test that the ChunksRetriever raises an error when the query is missing
    def test_chunks_retriever_missing_query(self, mock_post, chunks_params):
        del chunks_params["generic"]["index_conf"]["query"]
        chunks_retriever = ChunksRetriever(chunks_params)

        with pytest.raises(PrintableGenaiError) as excinfo:
            chunks_retriever.process()

        assert excinfo.value.status_code == 400
        assert "Query not found in the template" in str(excinfo.value)

    @patch("requests.post")
    # Test that the ChunksRetriever raises an error on HTTP request failure
    def test_chunks_retriever_http_error(self, mock_post, chunks_params):
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.content = b"Server Error"
        mock_post.return_value = mock_response

        chunks_retriever = ChunksRetriever(chunks_params)

        with pytest.raises(PrintableGenaiError) as excinfo:
            chunks_retriever.process()

        assert excinfo.value.status_code == 500
        assert "Error from genai-inforetrieval: b'Server Error'" in str(excinfo.value)

    @patch("requests.post")
    # Test that the ChunksRetriever raises an error when no documents are found
    def test_chunks_retriever_no_documents(self, mock_post, chunks_params):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"result": {"docs": []}}
        mock_post.return_value = mock_response

        chunks_retriever = ChunksRetriever(chunks_params)

        with pytest.raises(PrintableGenaiError) as excinfo:
            chunks_retriever.process()

        assert excinfo.value.status_code == 404
        assert "Error after calling retrieval. NO documents found" in str(excinfo.value)


class TestDocumentsRetriever:
    @patch("requests.post")
    # Test that the DocumentsRetriever processes a valid request and returns the expected documents
    def test_documents_retriever(self, mock_post, documents_params):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "result": {
                "docs": {
                    "doc1": [
                        {"content": "example content 1", "meta": {"field1": "value1"}},
                        {"content": "example content 2", "meta": {"field1": "value1"}},
                    ]
                }
            }
        }
        mock_post.return_value = mock_response

        documents_retriever = DocumentsRetriever(documents_params)
        result = documents_retriever.process()
        assert len(result) == 1
        assert result[0]["content"] == "example content 1example content 2"
        assert result[0]["meta"] == {"field1": "value1"}

    @patch("requests.post")
    # Test that the DocumentsRetriever raises an error on HTTP request failure
    def test_documents_retriever_http_error(self, mock_post, documents_params):
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.content = b"Server Error"
        mock_post.return_value = mock_response

        documents_retriever = DocumentsRetriever(documents_params)

        with pytest.raises(PrintableGenaiError) as excinfo:
            documents_retriever.process()

        assert excinfo.value.status_code == 500
        assert "Error from Retrieval: b'Server Error'" in str(excinfo.value)

    # Test that the get_example method returns the correct example type for documents retriever
    def test_documents_retriever_get_example(self):
        documents_retriever = DocumentsRetriever({})
        example = json.loads(documents_retriever.get_example())
        assert example["type"] == "get_documents"


class TestRetrieverFactory:
    # Test that the RetrieverFactory creates a DoNothing retriever and returns the original streamlist
    def test_retriever_factory_do_nothing(self, streamlist_params):
        factory = RetrieverFactory("streamlist")
        result = factory.process(streamlist_params)
        assert result == streamlist_params["streamlist"]

    # Test that the RetrieverFactory creates a ChunksRetriever and processes it correctly
    def test_retriever_factory_chunks(self, chunks_params):
        factory = RetrieverFactory("get_chunks")
        with patch.object(
            ChunksRetriever, "process", return_value="mock_chunks_result"
        ) as mock_process:
            result = factory.process(chunks_params)
            assert result == "mock_chunks_result"

    # Test that the RetrieverFactory creates a DocumentsRetriever and processes it correctly
    def test_retriever_factory_documents(self, documents_params):
        factory = RetrieverFactory("get_documents")
        with patch.object(
            DocumentsRetriever, "process", return_value="mock_documents_result"
        ) as mock_process:
            result = factory.process(documents_params)
            assert result == "mock_documents_result"

    # Test that the RetrieverFactory raises an error for an invalid retriever type
    def test_retriever_factory_invalid_type(self):
        with pytest.raises(PrintableGenaiError) as excinfo:
            RetrieverFactory("invalid_type")
        assert excinfo.value.status_code == 404
        assert "Provided retriever type does not match" in str(excinfo.value)
