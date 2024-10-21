### This code is property of the GGAO ###

import pytest
from compose.actions.llm_action import (
    LLMMethod,
    LLMSummarize,
    # LLMSummarizeContent,
    LLMSummarizeAnswer,
    LLMSummarizeSegments,
    LLMFactory,
    # LLMP,
)

from unittest.mock import patch, Mock, MagicMock, AsyncMock

# from compose.streamchunk import StreamChunk
from common.errors.genaierrors import PrintableGenaiError
import json
import aiohttp


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

    def get_example_json(self) -> str:
        """Devuelve un ejemplo como string JSON."""
        return json.dumps(self.get_example())


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

    def test_add_hystoric_with_system(mock_streamlist, mock_PD, mock_params):
        llm_summarize = LLMSummarize(mock_streamlist)
        session_id = "session123"

        mock_PD.get_conversation.return_value = [
            {"user": "Hello, how are you?", "system": "I'm fine, thank you."},
            {"assistant": "Glad to hear that!"},
        ]

        template = {"query_metadata": {}, "PD": mock_PD}

        result_template = llm_summarize.add_hystoric(session_id, template, mock_PD)

        assert "persistence" in result_template["query_metadata"]
        assert len(result_template["query_metadata"]["persistence"]) > 0
        assert len(result_template["query_metadata"]["persistence"]) == 1
        assert result_template["query_metadata"]["persistence"][0][0]["role"] == "user"
        assert (
            result_template["query_metadata"]["persistence"][0][0]["content"]
            == "Hello, how are you?"
        )
        assert (
            result_template["query_metadata"]["persistence"][0][1]["role"] == "system"
        )
        assert (
            result_template["query_metadata"]["persistence"][0][1]["content"]
            == "I'm fine, thank you."
        )

    def test_process_no_texts(self, mock_streamlist, mock_PD, mock_params):
        """Test para el caso donde no hay textos para sumarizar."""
        llm_summarize = LLMSummarize(mock_streamlist)
        mock_streamlist.clear()
        mock_params["query_type"] = "text"
        template = {
            "query_metadata": {},
            "PD": mock_PD,
            "query_type": "text",
            "llm_action": [],
        }
        session_id = "session123"

        with patch.object(
            llm_summarize,
            "call_llm",
            return_value={
                "answer": "Summary",
                "input_tokens": 50,
                "output_tokens": 30,
                "query_tokens": [10],
            },
        ):
            result_streamlist = llm_summarize.process(mock_params)

        assert len(result_streamlist) == 0

    def test_process_error_in_llm(self, mock_streamlist, mock_PD, mock_params):
        """Test para el caso donde ocurre un error al llamar a LLM."""
        llm_summarize = LLMSummarize(mock_streamlist)
        mock_params["query_type"] = "text"
        template = {
            "query_metadata": {},
            "PD": mock_PD,
            "query_type": "text",
            "llm_action": [],
        }
        session_id = "session123"

        with patch.object(
            llm_summarize, "call_llm", side_effect=PrintableGenaiError(500, "Error")
        ):
            with pytest.raises(PrintableGenaiError) as excinfo:
                llm_summarize.process(mock_params)

        assert excinfo.value.status_code == 500


class TestLLMSummarizeAnswer:
    @pytest.fixture
    def mock_streamlist_with_answers(self):
        """Fixture para crear un mock streamlist con respuestas."""
        return [
            {"content": "Some content", "answer": "This is an answer."},
            {"content": "Another content", "answer": "This is another answer."},
        ]

    def test_clear_output(self, mock_streamlist_with_answers):
        """Test para asegurar que clear_output elimina las respuestas de streamlist."""
        llm_summarize_answer = LLMSummarizeAnswer(mock_streamlist_with_answers)

        llm_summarize_answer.clear_output()

        for sl in llm_summarize_answer.streamlist:
            assert "answer" not in sl


class MockLLMSummarizeContent:
    TYPE = "llm_content"

    def __call__(self, streamlist):
        return self

    def process(self, params):
        return "Processed with llm_content"


class TestLLMSummarizeSegments:
    @pytest.mark.asyncio
    async def test_async_call_llm(mock_streamlist):
        llm_summarize = LLMSummarizeSegments(mock_streamlist)

        template = {"query_metadata": {}}
        headers = {"Authorization": "Bearer token"}

        mock_response = AsyncMock()
        mock_response.json.return_value = {"result": "mocked_result"}
        mock_response.status = 200
        mock_response.__aenter__.return_value = mock_response
        mock_response.__aexit__.return_value = False

        with patch(
            "aiohttp.ClientSession.post", return_value=mock_response
        ) as mock_post:
            async with aiohttp.ClientSession() as session:
                result = await llm_summarize.async_call_llm(template, headers, session)

                assert result == "mocked_result"

                mock_post.assert_called_once_with(
                    llm_summarize.URL, json=template, headers=headers, verify_ssl=False
                )

    @pytest.mark.asyncio
    async def test_parallel_calls(mock_streamlist):
        llm_summarize = LLMSummarizeSegments(mock_streamlist)

        templates = [
            {"query_metadata": {"context": "Content 1"}},
            {"query_metadata": {"context": "Content 2"}},
        ]
        headers = {"Authorization": "Bearer token"}

        with patch.object(
            llm_summarize,
            "async_call_llm",
            side_effect=[{"result": "mocked_result_1"}, {"result": "mocked_result_2"}],
        ):
            with patch("aiohttp.ClientSession") as mock_session:
                mock_session.return_value.__aenter__.return_value = AsyncMock()
                responses = await llm_summarize.parallel_calls(templates, headers)

                assert responses[0]["result"] == "mocked_result_1"
                assert responses[1]["result"] == "mocked_result_2"

                assert llm_summarize.async_call_llm.call_count == 2
                llm_summarize.async_call_llm.assert_any_call(
                    templates[0],
                    headers,
                    mock_session.return_value.__aenter__.return_value,
                )
                llm_summarize.async_call_llm.assert_any_call(
                    templates[1],
                    headers,
                    mock_session.return_value.__aenter__.return_value,
                )


class TestLLMFactory:
    @pytest.fixture
    def valid_llm_action(self):
        return "llm_content"

    @pytest.fixture
    def invalid_llm_action(self):
        return "invalid_action"

    @pytest.fixture
    def valid_streamlist(self):
        return []

    @pytest.fixture
    def valid_params(self):
        return {
            "param1": "value1",
            "param2": "value2",
        }

    def test_llm_factory_initialization_valid(self, valid_llm_action):
        factory = LLMFactory(valid_llm_action)
        assert factory is not None

    def test_process_success(self, valid_llm_action, valid_streamlist, valid_params):
        LLMFactory.SUMMARIES = [MockLLMSummarizeContent()]
        factory = LLMFactory(valid_llm_action)
        response = factory.process(valid_streamlist, valid_params)
        assert response == "Processed with llm_content"

    def test_llm_factory_initialization_invalid(self, invalid_llm_action):
        with pytest.raises(
            PrintableGenaiError,
            match="Provided llm_action does not match any of the possible ones",
        ):
            LLMFactory(invalid_llm_action)
