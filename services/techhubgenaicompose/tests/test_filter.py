import pytest
from compose.actions.filter import TopKFilter, MetadataFilter, FilterFactory
from datetime import datetime


class TestTopKFilter:
    @pytest.fixture
    def top_k_filter(self):
        """Fixture to create a TopKFilter instance with a sample streamlist."""
        streamlist = ["a", "b", "c", "d", "e"]
        return TopKFilter(streamlist)

    def test_process_valid_top_k(self, top_k_filter):
        """Test processing with valid top_k parameter."""
        result = top_k_filter.process({"top_k": 3})
        assert result == ["a", "b", "c"]

    def test_process_invalid_top_k(self, top_k_filter):
        """Test processing with invalid top_k parameter."""
        with pytest.raises(ValueError):
            top_k_filter.process({"top_k": "invalid"})

    # def test_process_no_top_k(self, top_k_filter):
    #    """Test processing without top_k parameter."""
    #   result = top_k_filter.process({})
    #    assert result == []

    def test_get_example(self, top_k_filter):
        """Test the _get_example method."""
        expected_example = {"type": "top_k", "params": {"top_k": 5}}
        assert top_k_filter._get_example() == expected_example


# class TestPermissionFilter:
#   @pytest.fixture
#    def permission_filter(self):
#        """Fixture to create a PermissionFilter instance with a sample streamlist."""
#        streamlist = [
#            {"meta": {"knowler_id": "doc1"}},
#            {"meta": {"knowler_id": "doc2"}},
#        ]
#        return PermissionFilter(streamlist)

#   @pytest.mark.asyncio
#    async def test_process_valid_permissions(self, permission_filter, mocker):
#        """Test processing with valid permissions."""
#        mock_response = {"allowed": {"doc1": True, "doc2": False}}
#        mocker.patch.object(
#            permission_filter, "send_post_request", return_value=mock_response
#        )

#       params = {
#            "url_allowed_documents": "http://ejemplo.com",
#            "headers_config": {"user-token": "fake-token"},
#        }
#        result = await permission_filter.process(params)

#       assert len(result) == 1
#        assert result[0]["meta"]["knowler_id"] == "doc1"


class MockStreamChunk:
    """Mock class to simulate StreamChunk objects."""

    def __init__(self, meta):
        self.meta = meta


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
