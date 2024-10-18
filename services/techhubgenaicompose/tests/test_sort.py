### This code is property of the GGAO ###

import pytest
from compose.actions.sort import (
    SortMethod,
    SortScore,
    SortDocumentID,
    SortSnippetNumber,
    SortLength,
    SortDate,
    SortMeta,
    SortFactory,
)
from common.errors.genaierrors import PrintableGenaiError
from dateutil.parser import parse
from typing import Dict
import json


class MockStreamChunk:
    def __init__(self, document_id, score, snippet_number, date, content, metadata):
        self.document_id = document_id
        self.score = score
        self.snippet_number = snippet_number
        self.date = date
        self.content = content
        self.metadata = metadata

    def get_mean_score(self):
        return self.score

    def get_metadata(self, key):
        return self.metadata.get(key)


@pytest.fixture
def mock_streamlist():
    return [
        MockStreamChunk(
            "doc1",
            0.9,
            1,
            "2023-10-10",
            "content1",
            {"document_id": "doc1", "snippet_number": 1, "date": "2023-10-10"},
        ),
        MockStreamChunk(
            "doc1",
            0.7,
            2,
            "2023-10-10",
            "content2",
            {"document_id": "doc1", "snippet_number": 2, "date": "2023-10-10"},
        ),
        MockStreamChunk(
            "doc2",
            0.85,
            1,
            "2023-10-09",
            "content3",
            {"document_id": "doc2", "snippet_number": 1, "date": "2023-10-09"},
        ),
        MockStreamChunk(
            "doc2",
            0.75,
            2,
            "2023-10-09",
            "content4",
            {"document_id": "doc2", "snippet_number": 2, "date": "2023-10-09"},
        ),
    ]


class TestSortMethod:
    class DummySortMethod(SortMethod):
        def _sort(self, documents):
            pass

        def _get_example(self) -> Dict:
            return super()._get_example()

        def process(self, documents):
            pass

    def test_sort_method_get_example(self):
        """Test para cubrir el método _get_example() de la clase base SortMethod."""
        sorter = self.DummySortMethod([])
        example = sorter.get_example()
        assert example == json.dumps({})


class TestSortScore:
    def test_sort_score(self, mock_streamlist):
        """Test the SortScore process method."""
        sorter = SortScore(mock_streamlist)
        result = sorter.process({"desc": True})

        assert result[0].get_mean_score() == 0.9
        assert result[-1].get_mean_score() == 0.7

    def test_sort_score_no_params(self, mock_streamlist):
        """Test the SortScore process method with no params (default values)."""
        sorter = SortScore(mock_streamlist)
        result = sorter.process(None)

        assert result[0].get_mean_score() == 0.9
        assert result[-1].get_mean_score() == 0.7

    def test_sort_score_get_example(self):
        """Test the SortScore get_example method."""
        sorter = SortScore([])
        result = sorter.get_example()
        expected_result = '{"type": "score", "params": {"desc": true}}'
        assert result == expected_result


class TestSortDocumentID:
    def test_sort_doc_id(self, mock_streamlist):
        """Test the SortDocumentID process method."""
        sorter = SortDocumentID(mock_streamlist)
        result = sorter.process({"desc": True})

        assert result[0].get_metadata("document_id") == "doc2"
        assert result[-1].get_metadata("document_id") == "doc1"

    def test_sort_doc_id_no_params(self, mock_streamlist):
        """Test the SortDocumentID process method with no params (default values)."""
        sorter = SortDocumentID(mock_streamlist)
        result = sorter.process(None)

        assert result[0].get_metadata("document_id") == "doc2"
        assert result[-1].get_metadata("document_id") == "doc1"

    def test_sort_document_id_get_example(self):
        """Test the SortDocumentID get_example method."""
        sorter = SortDocumentID([])
        result = sorter.get_example()
        expected_result = '{"type": "doc_id", "params": {"desc": true}}'
        assert result == expected_result


class TestSortSnippetNumber:
    def test_sort_snippet_number(self, mock_streamlist):
        """Test the SortSnippetNumber process method."""
        sorter = SortSnippetNumber(mock_streamlist)
        result = sorter.process({"desc": True})

        assert result[0].get_metadata("snippet_number") == 2
        assert result[-1].get_metadata("snippet_number") == 1

    def test_sort_snippet_number_no_params(self, mock_streamlist):
        """Test the SortSnippetNumber process method with no params (default values)."""
        sorter = SortSnippetNumber(mock_streamlist)
        result = sorter.process(None)

        assert result[0].get_metadata("snippet_number") == 2
        assert result[-1].get_metadata("snippet_number") == 1

    def test_sort_snippet_number_get_example(self):
        """Test the SortSnippetNumber get_example method."""
        sorter = SortSnippetNumber([])
        result = sorter.get_example()
        expected_result = '{"type": "sn_number", "params": {"desc": true}}'
        assert result == expected_result


class TestSortLength:
    def test_sort_length(self, mock_streamlist):
        """Test the SortLength process method."""
        sorter = SortLength(mock_streamlist)
        result = sorter.process({"desc": True})

        assert result[0].content == "content1"
        assert result[1].content == "content2"

    def test_sort_length_no_params(self, mock_streamlist):
        """Test the SortLength process method with no params (default values)."""
        sorter = SortLength(mock_streamlist)
        result = sorter.process(None)

        assert result[0].content == "content1"
        assert result[1].content == "content2"

    def test_sort_length_get_example(self):
        """Test the SortLength get_example method."""
        sorter = SortLength([])
        result = sorter.get_example()
        expected_result = '{"type": "length", "params": {"desc": true}}'
        assert result == expected_result


class TestSortDate:
    def test_sort_date(self, mock_streamlist):
        """Test the SortDate process method."""
        sorter = SortDate(mock_streamlist)
        result = sorter.process({"desc": True})

        assert parse(result[0].get_metadata("date")) == parse("2023-10-10")
        assert parse(result[-1].get_metadata("date")) == parse("2023-10-09")

    def test_sort_date_no_params(self, mock_streamlist):
        """Test the SortDate process method with no params (default values)."""
        sorter = SortDate(mock_streamlist)
        result = sorter.process(None)

        assert parse(result[0].get_metadata("date")) == parse("2023-10-10")
        assert parse(result[-1].get_metadata("date")) == parse("2023-10-09")

    def test_sort_date_get_example(self):
        """Test the SortDate get_example method."""
        sorter = SortDate([])
        result = sorter.get_example()
        expected_result = '{"type": "date", "params": {"desc": true}}'
        assert result == expected_result


class TestSortMeta:
    def test_sort_meta(self, mock_streamlist):
        """Test the SortMeta process method with a specific metadata value."""
        sorter = SortMeta(mock_streamlist)
        result = sorter.process({"desc": True, "value": "document_id"})

        assert result[0].get_metadata("document_id") == "doc2"
        assert result[-1].get_metadata("document_id") == "doc1"

    def test_sort_meta_no_params(self, mock_streamlist):
        """Test the SortMeta process method with no params (default values)."""
        sorter = SortMeta(mock_streamlist)

        for chunk in mock_streamlist:
            chunk.get_metadata = lambda key: "valid_meta_value"

        result = sorter.process({})

        assert result[0].get_metadata("value") == "valid_meta_value"
        assert result[1].get_metadata("value") == "valid_meta_value"

    def test_sort_meta_get_example(self):
        """Test the SortMeta get_example method."""
        sorter = SortMeta([])
        result = sorter.get_example()
        expected_result = '{"type": "meta", "params": {"desc": true}}'
        assert result == expected_result


class TestSortFactory:
    def test_sort_factory_score(self, mock_streamlist):
        """Test the SortFactory for the 'score' sorting method."""
        factory = SortFactory("score")
        result = factory.process(mock_streamlist, {"desc": True})

        assert result[0].get_mean_score() == 0.9
        assert result[-1].get_mean_score() == 0.7

    def test_sort_factory_invalid_type(self):
        """Test the SortFactory raises an error for an invalid sort type."""
        with pytest.raises(
            PrintableGenaiError, match="Provided sorting method does not match"
        ):
            SortFactory("invalid_type")
