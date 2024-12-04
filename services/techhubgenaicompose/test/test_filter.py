### This code is property of the GGAO ###

import os
os.environ['URL_LLM'] = "test_url"
os.environ['URL_RETRIEVE'] = "test_retrieve"
import pytest
from compose.actions.filter import (
    FilterMethod,
    TopKFilter,
    PermissionFilter,
    RelatedToFilter,
    MetadataFilter,
    FilterFactory,
)
import json
from common.errors.genaierrors import PrintableGenaiError
from datetime import datetime
from unittest.mock import AsyncMock, patch, MagicMock
from aioresponses import aioresponses
from copy import deepcopy


class DummyFilter(FilterMethod):
    def process(self):
        """Implementación del método process de FilterMethod."""
        return super().process()

    def _get_example(self):
        """Implementación del método _get_example de FilterMethod."""
        return super()._get_example()


@pytest.fixture
def dummy_filter():
    """Fixture para crear una instancia de DummyFilter."""
    return DummyFilter(streamlist=["x", "y", "z"])


@pytest.fixture
def top_k_filter():
    """Fixture para crear una instancia de TopKFilter con una lista de ejemplo."""
    streamlist = ["a", "b", "c", "d", "e"]
    return TopKFilter(streamlist)


@pytest.fixture
def metadata_filter():
    """Fixture para crear una instancia de MetadataFilter con una lista de ejemplo."""
    streamlist = [{"meta": {"some_key": "some_value"}}]
    return MetadataFilter(streamlist)


class TestFilterMethod:
    def test_process_abstract_method(self, dummy_filter):
        """Test para cubrir el pass en el método abstracto process."""
        result = dummy_filter.process()
        assert result is None

    def test_get_example(self, dummy_filter):
        """Test que cubre el método get_example de FilterMethod."""
        result = dummy_filter.get_example()
        expected_result = json.dumps({})
        assert result == expected_result

    def test_get_example_with_super(self, dummy_filter):
        """Test para cubrir el return {} en _get_example usando super()."""
        assert dummy_filter._get_example() == {}

    def test_get_example_json(self, dummy_filter):
        """Test para asegurar que el método get_example llame correctamente a _get_example."""
        result = dummy_filter.get_example()
        assert result == json.dumps({})


class TestTopKFilter:
    def test_process_valid_top_k(self, top_k_filter):
        """Test processing with valid top_k parameter."""
        result = top_k_filter.process({"top_k": 3})
        assert result == ["a", "b", "c"]

    def test_process_invalid_top_k(self, top_k_filter):
        """Test processing with invalid top_k parameter."""
        with pytest.raises(ValueError):
            top_k_filter.process({"top_k": "invalid"})

    def test_get_example(self, top_k_filter):
        """Test the _get_example method."""
        expected_example = {"type": "top_k", "params": {"top_k": 5}}
        assert top_k_filter._get_example() == expected_example


class MockStreamChunk:
    """Mock class to simulate StreamChunk objects."""

    def __init__(self, meta):
        self.meta = meta


@pytest.fixture
def sample_data():
    return [
        {"id": 1, "meta": {"category": "news", "type": "article"}},
        {"id": 2, "meta": {"category": "news", "type": "opinion"}},
        {"id": 3, "meta": {"category": "sports", "type": "article"}},
    ]


class TestMetadataFilter:
    @pytest.fixture
    def metadata_filter(self):
        """Fixture to create a MetadataFilter instance with sample streamlist."""
        streamlist = [
            MockStreamChunk({"city": "New York", "age": 35, "date": "2023-10-01"}),
            MockStreamChunk({"city": "London", "age": 28, "date": "2024-01-01"}),
            MockStreamChunk({"city": "Paris", "age": 40, "date": "2022-07-15"}),
        ]
        return MetadataFilter(streamlist)

    def test_apply_subfilter_eq(self, metadata_filter):
        """Test apply_subfilter with 'eq' condition."""
        result = metadata_filter.apply_subfilter(
            {"city": "New York"}, {"eq": ("city", "New York")}
        )
        assert result is True

    def test_apply_subfilter_gt(self, metadata_filter):
        """Test apply_subfilter with 'gt' condition."""
        result = metadata_filter.apply_subfilter({"age": 35}, {"gt": ("age", 30)})
        assert result is True

    def test_apply_subfilter_invalid_key(self, metadata_filter):
        """Test apply_subfilter raises error with invalid key."""
        with pytest.raises(Exception):
            metadata_filter.apply_subfilter(
                {"city": "New York"}, {"invalid": ("city", "New York")}
            )

    def test_metadata_to_datetime_valid(self, metadata_filter):
        """Test metadata_to_datetime with valid date string."""
        result = metadata_filter.metadata_to_datetime("2023-10-01")
        assert isinstance(result, datetime)

    def test_metadata_to_datetime_invalid(self, metadata_filter):
        """Test metadata_to_datetime raises error with invalid date format."""
        with pytest.raises(Exception):
            metadata_filter.metadata_to_datetime("invalid-date")

    def test_operator_and(self, metadata_filter):
        """Test operator with 'and' condition."""
        result = metadata_filter.operator(True, True, "and")
        assert result is True

    def test_operator_or(self, metadata_filter):
        """Test operator with 'or' condition."""
        result = metadata_filter.operator(True, False, "or")
        assert result is True

    def test_operator_invalid(self, metadata_filter):
        """Test operator raises error with invalid operator."""
        with pytest.raises(Exception):
            metadata_filter.operator(True, False, "invalid")

    def test_apply_filter_with_and(self, metadata_filter):
        """Test apply_filter with 'and' operator."""
        filter_conditions = {"and": [{"gt": ("age", 30)}, {"lt": ("age", 40)}]}
        result = metadata_filter.apply_filter({"age": 35}, filter_conditions, "and")
        assert result is True

    def test_apply_filter_with_or(self, metadata_filter):
        """Test apply_filter with 'or' operator."""
        filter_conditions = {
            "or": [{"eq": ("city", "New York")}, {"eq": ("city", "London")}]
        }
        result = metadata_filter.apply_filter(
            {"city": "New York"}, filter_conditions, "or"
        )
        assert result is True

    def test_filter_data(self, metadata_filter):
        """Test filter_data filters the streamlist based on conditions."""
        filter_conditions = {"and": [{"gt": ("age", 30)}, {"lt": ("age", 40)}]}
        result = metadata_filter.filter_data(filter_conditions, "and")
        assert len(result) == 1
        assert result[0].meta["city"] == "New York"

    def test_process(self, metadata_filter):
        """Test process filters the streamlist using filter conditions."""
        params = {
            "filter_conditions": {"and": [{"gt": ("age", 30)}, {"lt": ("age", 40)}]},
            "operator": "and",
        }
        result = metadata_filter.process(params)
        assert len(result) == 1
        assert result[0].meta["city"] == "New York"

    def test_process_invalid_operator(self, metadata_filter):
        """Test process raises error with invalid operator."""
        params = {
            "filter_conditions": {"and": [{"gt": ("age", 30)}, {"lt": ("age", 40)}]},
            "operator": "invalid",
        }
        with pytest.raises(AssertionError):
            metadata_filter.process(params)

    def test_get_example(self, metadata_filter):
        """Test _get_example provides the correct example format."""
        example = metadata_filter._get_example()
        expected = {
            "type": "top_k",
            "params": {
                "filter_conditions": {
                    "or": [
                        {"eq": ("city", "New York")},
                        {"in": ("city", ["New York", "London"])},
                        {"textinmeta": ("city", "New Yor")},
                        {
                            "metaintext": (
                                "city",
                                "The city of New York is known as the big apple.",
                            )
                        },
                    ],
                    "and": [{"gt": ("age", 30)}, {"lt": ("age", 40)}],
                },
                "operator": "and",
            },
        }
        assert example == expected

    def test_apply_subfilter_key_not_exist(self, metadata_filter):
        """Test apply_subfilter raises PrintableGenaiError with non-existent key."""
        non_existent_key = {"invalid_key": ("invalid_key", "some_value")}

        with pytest.raises(PrintableGenaiError) as exc_info:
            metadata_filter.apply_subfilter({"city": "New York"}, non_existent_key)

        assert exc_info.value.status_code == 404
        assert "Metadata key invalid_key does not exist" in str(exc_info.value)

    def test_apply_subfilter_in(self, metadata_filter):
        """Test apply_subfilter with 'in' condition."""

        result = metadata_filter.apply_subfilter(
            {"city": "New York"}, {"in": ("city", ["New York", "London"])}
        )
        assert result is True

        result = metadata_filter.apply_subfilter(
            {"city": "Paris"}, {"in": ("city", ["New York", "London"])}
        )
        assert result is False

    def test_apply_subfilter_textinmeta(self, metadata_filter):
        """Test apply_subfilter with 'textinmeta' condition."""
        metadata = {"description": "The city of New York is known as the big apple."}
        sub_filter = {"textinmeta": ("description", "big apple")}

        result = metadata_filter.apply_subfilter(metadata, sub_filter)

        assert result is True

    def test_apply_subfilter_metaintext(self, metadata_filter):
        """Test apply_subfilter with 'metaintext' condition."""
        result = metadata_filter.apply_subfilter(
            {"city": "New York"}, {"metaintext": ("city", "New York is a big city.")}
        )
        assert result is True

    def test_apply_subfilter_eq_date(self, metadata_filter):
        """Test apply_subfilter with 'eq_date' condition."""

        metadata = {"event_date": "2024-10-17"}

        sub_filter = {"eq_date": ("event_date", "2024-10-17")}

        result = metadata_filter.apply_subfilter(metadata, sub_filter)

        assert result is True

    def test_apply_subfilter_gt_date(self, metadata_filter):
        """Test apply_subfilter with 'gt_date' condition."""

        metadata = {"event_date": "2024-10-18"}

        sub_filter = {"gt_date": ("event_date", "2024-10-17")}

        result = metadata_filter.apply_subfilter(metadata, sub_filter)

        assert result is True

    def test_apply_subfilter_lt_date(self, metadata_filter):
        """Test apply_subfilter with 'lt_date' condition."""

        metadata = {"event_date": "2024-10-16"}

        sub_filter = {"lt_date": ("event_date", "2024-10-17")}

        result = metadata_filter.apply_subfilter(metadata, sub_filter)

        assert result is True

    def test_process_with_no_keys(self, metadata_filter):
        """Test process raises an error when no filter conditions are provided."""
        params = {
            "filter_conditions": {},
            "operator": "and",
        }
        with pytest.raises(PrintableGenaiError) as exc_info:
            metadata_filter.process(params)

        assert exc_info.value.status_code == 500
        assert "Only 'and' or 'or' keys must be defined." in str(exc_info.value)

    def test_apply_filter_and_or_combined(self, sample_data):
        streamlist = sample_data
        filter = MetadataFilter(streamlist)

        filters = {
            "and": [
                {"eq": ("category", "news")},
                {"eq": ("type", "article")},
            ],
            "or": [{"in": ("type", ["article", "opinion"])}],
        }

        filtered_result = [
            item
            for item in sample_data
            if filter.apply_filter(item["meta"], filters, "or")
        ]

        expected_result = [
            {
                "id": 1,
                "meta": {"category": "news", "type": "article"},
            },
            {
                "id": 2,
                "meta": {"category": "news", "type": "opinion"},
            },
        ]

        assert len(filtered_result) == len(expected_result) or len(filtered_result) == 3
        assert all(item in filtered_result for item in expected_result)


class TestFilterFactory:
    @pytest.fixture
    def streamlist(self):
        """Fixture to create a sample streamlist."""
        return ["a", "b", "c", "d", "e"]

    def test_valid_filter_selection(self):
        """Test that a valid filter type is selected properly."""
        factory = FilterFactory("top_k")
        assert factory.filtermethod == TopKFilter

    def test_invalid_filter_selection(self):
        """Test that an invalid filter type raises a PrintableGenaiError."""
        with pytest.raises(Exception) as excinfo:
            FilterFactory("invalid_filter")
        assert "Provided filter does not match any" in str(excinfo.value)

    def test_process_valid(self, streamlist):
        """Test processing with a valid filter and valid params."""
        factory = FilterFactory("top_k")
        result = factory.process(streamlist, {"top_k": 3})
        assert result == ["a", "b", "c"]

    def test_process_empty_result(self, streamlist):
        """Test that an empty result after processing raises PrintableGenaiError."""
        factory = FilterFactory("top_k")
        with pytest.raises(Exception) as excinfo:
            factory.process(streamlist, {"top_k": 0})
        assert "NO documents passed the filters" in str(excinfo.value)


@pytest.fixture
def permission_filter():
    streamlist = [
        MagicMock(meta={"knowler_id": "1"}),
        MagicMock(meta={"knowler_id": "2"}),
        MagicMock(meta={"knowler_id": "3"}),
    ]

    return PermissionFilter(streamlist)


@pytest.mark.asyncio
class TestPermissionFilter:
    @pytest.fixture(autouse=True)
    def setup_method(self, monkeypatch):
        monkeypatch.setenv("URL_ALLOWED_DOCUMENTS_KNOWLER", "http://example.com")

    async def mock_send_post_request(self, url, body, headers):
        """Simula la respuesta de la función asíncrona send_post_request."""
        return {"allowed": {"1": True, "2": False, "3": True}}

    def test_update_params(self, permission_filter):
        """Test para el método update_params."""
        params = {
            "headers_config": {"Authorization": "Bearer token"},
            "additional_param": "value",
        }
        headers, template = permission_filter.update_params(params)

        assert headers["Authorization"] == "Bearer token"
        assert template["additional_param"] == "value"

    def test_get_example(self, permission_filter):
        """Test que verifica que get_example retorna el formato correcto."""
        expected_example = {
            "type": permission_filter.TYPE,
            "params": permission_filter.TEMPLATE,
        }

        result = permission_filter.get_example()
        assert json.loads(result) == expected_example

    def test_get_example_structure(self, permission_filter):
        """Test que verifica la estructura del ejemplo retornado."""
        example = permission_filter._get_example()
        assert "type" in example
        assert "params" in example
        assert example["type"] == permission_filter.TYPE
        assert example["params"] == permission_filter.TEMPLATE

    def test_permission_filter_url_none(self, permission_filter):
        """Test para cubrir el caso donde URL es None."""
        permission_filter.URL = None

        with pytest.raises(PrintableGenaiError) as exc_info:
            permission_filter.process({})

        assert exc_info.value.status_code == 400
        assert "Variable URL_ALLOWED_DOCUMENT not found" in str(exc_info.value)

    async def test_send_post_request_success(self, permission_filter):
        """Test para send_post_request cuando la respuesta es exitosa."""
        url = "http://example.com"
        body = {"internalIds": ["1", "2", "3"]}
        headers = {"Authorization": "Bearer token"}

        with aioresponses() as m:
            m.post(url, payload={"allowed": {"1": True, "2": False, "3": True}})
            response = await permission_filter.send_post_request(url, body, headers)

            assert response == {"allowed": {"1": True, "2": False, "3": True}}

    async def test_send_post_request_failure(self, permission_filter):
        """Test para send_post_request cuando la respuesta falla."""
        url = "http://example.com"
        body = {"internalIds": ["1", "2", "3"]}
        headers = {"Authorization": "Bearer token"}

        with aioresponses() as m:
            m.post(url, status=400, payload="Bad Request")
            with pytest.raises(PrintableGenaiError) as exc_info:
                await permission_filter.send_post_request(url, body, headers)

            assert exc_info.value.status_code == 400
            assert "Bad Request" in str(exc_info.value)

    def test_permission_filter_process(self, permission_filter):
        """Test para cubrir el método process."""
        permission_filter.URL = "http://example.com"
        permission_filter.streamlist = [
            MockStreamChunk(meta={"knowler_id": "1"}),
            MockStreamChunk(meta={"knowler_id": "2"}),
            MockStreamChunk(meta={"knowler_id": "3"}),
        ]

        permission_filter.send_post_request = self.mock_send_post_request

        result = permission_filter.process({})

        assert len(result) == 2
        assert result[0].meta["knowler_id"] == "1"
        assert result[1].meta["knowler_id"] == "3"


class MockStream:
    """Mock class to simulate StreamChunk objects."""

    def __init__(self, content):
        self.content = content


class TestRelatedToFilter:
    @pytest.fixture
    def filter_instance(self):
        streamlist = []
        return RelatedToFilter(streamlist)

    def test_update_params_without_headers_config(self, filter_instance):
        params = {"param1": "value1", "param2": "value2"}
        expected_headers = deepcopy(filter_instance.HEADERS)
        expected_template = deepcopy(filter_instance.TEMPLATE)
        expected_template.update(params)

        headers, template = filter_instance.update_params(params)

        assert headers == expected_headers
        assert template == expected_template

    def test_update_params_empty_params(self, filter_instance):
        params = {}
        expected_headers = deepcopy(filter_instance.HEADERS)
        expected_template = deepcopy(filter_instance.TEMPLATE)

        headers, template = filter_instance.update_params(params)

        assert headers == expected_headers
        assert template == expected_template

    def test_update_params_with_missing_key(self, filter_instance):
        params = {"headers_config": {"Authorization": "Bearer token"}}
        expected_headers = deepcopy(filter_instance.HEADERS)
        expected_headers.update(params["headers_config"])
        expected_template = deepcopy(filter_instance.TEMPLATE)

        headers, template = filter_instance.update_params(params)

        assert headers == expected_headers
        assert template == expected_template

    def test_get_example_dict(self, filter_instance):
        expected_result = {
            "type": filter_instance.TYPE,
            "params": filter_instance.TEMPLATE,
        }

        assert filter_instance._get_example() == expected_result

    def test_get_example_json(self, filter_instance):
        expected_result = json.dumps(
            {"type": filter_instance.TYPE, "params": filter_instance.TEMPLATE}
        )

        assert filter_instance.get_example() == expected_result

    @pytest.mark.asyncio
    async def test_parallel_calls(self, filter_instance):
        templates = [{"query_metadata": {"context": "test context"}}]
        headers = {"Content-type": "application/json"}

        mock_response = {"answer": "Yes"}

        with patch("aiohttp.ClientSession") as mock_session:
            mock_session_instance = mock_session.return_value.__aenter__.return_value
            with patch.object(
                filter_instance,
                "async_call_llm",
                new=AsyncMock(return_value=mock_response),
            ) as mock_async_call_llm:
                responses = await filter_instance.parallel_calls(templates, headers)

                mock_async_call_llm.assert_called_once_with(
                    templates[0], headers, mock_session_instance
                )

        assert responses == [mock_response]

    def test_process(self, filter_instance):
        filter_instance.streamlist = [
            MockStream(content="test content 1"),
            MockStream(content="test content 2"),
        ]

        params = {}
        headers, template = filter_instance.update_params(params)

        mock_response = [{"answer": "Yes"}, {"answer": "No"}]

        with patch.object(
            filter_instance, "parallel_calls", new=AsyncMock(return_value=mock_response)
        ):
            output = filter_instance.process(params)

        assert len(output) == 1
        assert output[0].content == "test content 1"

    @pytest.mark.asyncio
    async def test_async_call_llm(self, filter_instance):
        template = {"query_metadata": {"context": "test context"}}
        headers = {"Content-type": "application/json"}

        mock_response_data = {"result": {"answer": "Yes"}}

        with patch("aiohttp.ClientSession") as mock_session:
            mock_session_instance = mock_session.return_value

            mock_response = AsyncMock()
            mock_response.__aenter__.return_value.json = AsyncMock(
                return_value=mock_response_data
            )
            mock_response.__aenter__.return_value.status = 200

            mock_session_instance.post.return_value = mock_response

            result = await filter_instance.async_call_llm(
                template, headers, mock_session_instance
            )

            assert result == mock_response_data["result"]

            mock_session_instance.post.assert_called_once_with(
                filter_instance.URL, json=template, headers=headers, verify_ssl=False
            )
