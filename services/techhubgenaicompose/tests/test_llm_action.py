### This code is property of the GGAO ###

import pytest
from compose.actions.llm_action import (
    LLMMethod,
    LLMSummarize,
    # LLMSummarizeContent,
    # LLMSummarizeAnswer,
    # LLMSummarizeSegments,
    # LLMFactory,
)

from unittest.mock import patch, Mock, MagicMock

# from compose.streamchunk import StreamChunk
from common.errors.genaierrors import PrintableGenaiError
# import json


@pytest.fixture
def dummy_llm_method():
    """Fixture para crear una instancia de DummyLLMMethod."""
    streamlist = ["item1", "item2", "item3"]
    return DummyLLMMethod(streamlist)


@pytest.fixture
def mock_streamlist():
    """Fixture to create a mock streamlist."""
    return [MagicMock(content="Sample content", meta={"title": "Sample title"})]


@pytest.fixture
def mock_PD():
    """Fixture to create a mock PD object."""
    pd_mock = MagicMock()
    pd_mock.get_conversation.return_value = [
        {"user": "User message", "assistant": "Assistant message"}
    ]
    return pd_mock


@pytest.fixture
def mock_params():
    """Fixture to create mock parameters."""
    return {
        "headers_config": {},
        "session_id": "session123",
        "PD": MagicMock(),
        "llm_action": [],
        "query_type": "text",
    }


class DummyLLMMethod(LLMMethod):
    TYPE = "dummy"
    URL = "http://dummy.url"
    TEMPLATE = {"prompt": "default prompt"}

    def process(self, param) -> list:
        return self.streamlist

    def get_example(self) -> dict:
        """Devuelve un ejemplo de diccionario."""
        return {"type": self.TYPE, "params": self.TEMPLATE}


class TestLLMMethod:
    def test_init(self, dummy_llm_method):
        """Test para cubrir la inicialización de LLMMethod."""
        assert dummy_llm_method.streamlist == ["item1", "item2", "item3"]

    def test_get_example(self, dummy_llm_method):
        """Test para asegurar que get_example devuelve el resultado esperado."""
        expected_result = {
            "type": "dummy",
            "params": {"prompt": "default prompt"},
        }
        assert dummy_llm_method.get_example() == expected_result

    def test_process(self, dummy_llm_method):
        """Test para cubrir el método process."""
        assert dummy_llm_method.process(None) == ["item1", "item2", "item3"]

    def test_get_example_method(self, dummy_llm_method):
        """Test to ensure _get_example returns the expected dictionary."""

        dummy_llm_method.TYPE = "example_type"
        dummy_llm_method.TEMPLATE = {"key1": "value1", "key2": "value2"}

        expected_result = {
            "type": "example_type",
            "params": {"key1": "value1", "key2": "value2"},
        }

        assert dummy_llm_method._get_example() == expected_result

    def test_adapt_query_for_model(self, dummy_llm_method):
        """Test para cubrir el método adapt_query_for_model."""

        llm_action = [
            {"query": "http://example.com/image.png", "query_type": "image_url"},
            {"query": "data:image/png;base64,exampledata", "query_type": "image_b64"},
            {"query": "This is a text query.", "query_type": "text"},
        ]
        query_type = "image_url"
        template = {"query_metadata": {"query": "http://template.com/image.png"}}

        expected_result = {
            "query_metadata": {
                "query": [
                    {
                        "type": "image_url",
                        "image": {
                            "url": "http://example.com/image.png",
                            "detail": "high",
                        },
                    },
                    {
                        "type": "image_b64",
                        "image": {
                            "base64": "data:image/png;base64,exampledata",
                            "detail": "high",
                        },
                    },
                    {"type": "text", "text": "This is a text query."},
                    {
                        "type": "image_url",
                        "image": {
                            "url": "http://template.com/image.png",
                            "detail": "high",
                        },
                    },
                ]
            }
        }

        result = dummy_llm_method.adapt_query_for_model(
            llm_action, query_type, template
        )

        assert result == expected_result

    def test_adapt_query_for_model_image_b64(self, dummy_llm_method):
        """Test para cubrir el caso donde query_type es 'image_b64'."""

        llm_action = [
            {"query": "data:image/png;base64,exampledata", "query_type": "image_b64"}
        ]
        query_type = "image_b64"
        template = {"query_metadata": {"query": "base64_encoded_data"}}

        expected_result = {
            "query_metadata": {
                "query": [
                    {
                        "type": "image_b64",
                        "image": {
                            "base64": "data:image/png;base64,exampledata",
                            "detail": "high",
                        },
                    },
                    {
                        "type": "image_b64",
                        "image": {
                            "base64": "base64_encoded_data",
                            "detail": "high",
                        },
                    },
                ]
            }
        }

        result = dummy_llm_method.adapt_query_for_model(
            llm_action, query_type, template
        )

        assert result == expected_result

    def test_adapt_query_for_model_text(self, dummy_llm_method):
        """Test para cubrir el caso donde query_type es 'text'."""

        llm_action = [{"query": "This is a text query.", "query_type": "text"}]
        query_type = "text"
        template = {"query_metadata": {"query": "This is a template text."}}

        expected_result = {
            "query_metadata": {
                "query": [
                    {
                        "type": "text",
                        "text": "This is a text query.",
                    },
                    {
                        "type": "text",
                        "text": "This is a template text.",
                    },
                ]
            }
        }

        result = dummy_llm_method.adapt_query_for_model(
            llm_action, query_type, template
        )

        assert result == expected_result

    @patch("compose.actions.llm_action.requests.post")
    @patch("compose.actions.llm_action.LLMP")
    def test_call_llm_success(self, mock_llmp, mock_post, dummy_llm_method):
        """Test para cubrir la llamada exitosa a LLM."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"result": "success"}
        mock_post.return_value = mock_response

        mock_llmp.parse_response.return_value = {"result": "success"}

        template = {"prompt": "Test prompt"}
        headers = {"Content-type": "application/json"}

        result = dummy_llm_method.call_llm(template, headers)

        assert result == {"result": "success"}
        mock_post.assert_called_once_with(
            dummy_llm_method.URL, json=template, headers=headers, verify=True
        )
        mock_llmp.parse_response.assert_called_once_with(mock_response)

    @patch("compose.actions.llm_action.requests.post")
    def test_call_llm_error(self, mock_post, dummy_llm_method):
        """Test para verificar el manejo de errores en call_llm."""

        mock_post.side_effect = Exception("Network error")

        template = {"prompt": "Test prompt"}
        headers = {"Content-type": "application/json"}

        with pytest.raises(PrintableGenaiError) as excinfo:
            dummy_llm_method.call_llm(template, headers)

        assert excinfo.value.status_code == 500
        assert "Error calling GENAI-LLMAPI: Network error" in str(excinfo.value)

    @patch("compose.actions.llm_action.requests.post")
    def test_call_llm_invalid_response(self, mock_post, dummy_llm_method):
        """Test para verificar el manejo de errores en la respuesta de call_llm."""

        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.text = "Bad request"
        mock_post.return_value = mock_response

        template = {"prompt": "Test prompt"}
        headers = {"Content-type": "application/json"}

        with pytest.raises(PrintableGenaiError) as excinfo:
            dummy_llm_method.call_llm(template, headers)

        assert excinfo.value.status_code == 400
        assert "Error from GENAI-LLMAPI: Bad request" in str(excinfo.value)


class TestLLMSummarize:
    def test_add_hystoric(mock_streamlist, mock_PD, mock_params):
        llm_summarize = LLMSummarize(mock_streamlist)
        template = {"query_metadata": {}, "PD": mock_PD}
        session_id = "session123"

        result_template = llm_summarize.add_hystoric(session_id, template, mock_PD)

        assert "persistence" in result_template["query_metadata"]
        assert len(result_template["query_metadata"]["persistence"]) > 0
