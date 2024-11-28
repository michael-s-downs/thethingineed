### This code is property of the GGAO ###

import pytest
from compose.actions.rescore import (
    RescoreMethod,
    AverageRescore,
    GenaiRescorer,
    RescoreFactory,
)
from unittest.mock import patch
from common.errors.genaierrors import PrintableGenaiError
import json


class MockStream:
    def __init__(self, snippet_id, scores):
        self.meta = {"snippet_id": snippet_id}
        self.scores = scores


class MockStreamMissingSnippetId:
    """Mock class to simulate a stream without snippet_id in the metadata."""

    def __init__(self, scores):
        self.meta = {}
        self.scores = scores


@pytest.fixture
def streamlist():
    """Fixture to provide a list of mocked streams for testing."""
    return [
        MockStream("id1", {"score1": 0.8, "score2": 0.6}),
        MockStream("id2", {"score1": 0.4, "score2": 0.5}),
    ]


class RescoreMethodTest(RescoreMethod):
    """Subclass of RescoreMethod for testing purposes."""

    def process(self):
        super().process()

    def _get_example(self):
        return super()._get_example()


class TestRescoreMethod:
    # Test that ensures attempting to instantiate RescoreMethod directly raises a TypeError
    def test_rescore_method_subclass(self):
        with pytest.raises(TypeError):
            RescoreMethod(streamlist=[])

    # Test that ensures the process method in RescoreMethodTest returns None
    def test_rescore_method_process(self):
        rescore_method = RescoreMethodTest(streamlist=[])
        result = rescore_method.process()
        assert result is None

    # Test that ensures the _get_example method returns None
    def test_rescore_method_get_example(self):
        rescore_method = RescoreMethodTest(streamlist=[])
        result = rescore_method._get_example()
        assert result is None


class TestAverageRescore:
    # Test the process method of AverageRescore
    def test_process(self, streamlist):
        rescore = AverageRescore(streamlist)
        processed_streams = rescore.process()
        assert len(processed_streams) == len(streamlist)
        assert all("mean" in stream.scores for stream in processed_streams)

    # Test the get_example method of AverageRescore
    def test_get_example(self):
        rescore = AverageRescore([])
        example = rescore.get_example()
        assert example == '{"type": "mean", "params": null}'


class TestGenaiRescorer:
    # Test the process method of GenaiRescorer
    def test_process(self, streamlist):
        with patch("requests.post") as mock_post:
            mock_post.return_value.status_code = 200
            mock_post.return_value.json.return_value = {
                "result": {
                    "docs": [
                        {
                            "content": "doc1",
                            "meta": {"snippet_id": "id1", "score1--score": 0.8},
                        },
                        {
                            "content": "doc2",
                            "meta": {"snippet_id": "id2", "score1--score": 0.4},
                        },
                    ]
                }
            }

            rescore = GenaiRescorer(streamlist)
            processed_streams = rescore.process(params={})
            assert len(processed_streams) == 2
            assert all("content" in stream for stream in processed_streams)

    # Test the get_document_ids method of GenaiRescorer
    def test_get_document_ids(self, streamlist):
        rescore = GenaiRescorer(streamlist)
        doc_ids = list(rescore.get_document_ids())
        assert doc_ids == ["id1", "id2"]

    # Test that ensures an exception is raised when the process request fails
    def test_process_request_failed(self, streamlist):
        with patch("requests.post") as mock_post:
            mock_post.return_value.status_code = 500
            mock_post.return_value.content = b"Error"
            rescore = GenaiRescorer(streamlist)
            with pytest.raises(PrintableGenaiError, match="Error"):
                rescore.process(params={})

    # Test that ensures get_example returns the correct JSON structure
    def test_get_example(self):
        rescore = GenaiRescorer([])
        example = rescore.get_example()
        assert (
            example
            == '{"type": "genai", "params": ' + json.dumps(rescore.TEMPLATE) + "}"
        )


class TestGenaiRescorerMissingSnippetID:
    # Test that ensures an exception is raised when a stream is missing "snippet_id" in its metadata
    def test_get_document_ids_missing_snippet_id(self):
        streamlist_missing_id = [
            MockStreamMissingSnippetId({"score1": 0.8, "score2": 0.6}),
            MockStream("id2", {"score1": 0.4, "score2": 0.5}),
        ]

        rescore = GenaiRescorer(streamlist_missing_id)

        # Verifica que se lanza la excepción PrintableGenaiError cuando falta "snippet_id"
        with pytest.raises(
            PrintableGenaiError,
            match="Streamlist must have a 'snippet_id' key that identifies the passage on an index.",
        ):
            list(rescore.get_document_ids())


class TestRescoreFactory:
    # Test that ensures the factory processes an average rescore correctly
    def test_process_average(self, streamlist):
        factory = RescoreFactory("mean")
        processed_streams = factory.process(streamlist, {})
        assert len(processed_streams) == len(streamlist)

    # Test that ensures the factory processes a GenaiRescorer rescore correctly
    def test_process_genai(self, streamlist):
        with patch("requests.post") as mock_post:
            mock_post.return_value.status_code = 200
            mock_post.return_value.json.return_value = {
                "result": {
                    "docs": [
                        {
                            "content": "doc1",
                            "meta": {"snippet_id": "id1", "score1--score": 0.8},
                        },
                        {
                            "content": "doc2",
                            "meta": {"snippet_id": "id2", "score1--score": 0.4},
                        },
                    ]
                }
            }
            factory = RescoreFactory("genai_rescorer")
            processed_streams = factory.process(streamlist, {})
            assert len(processed_streams) == 2

    # Test that ensures an invalid rescore type raises an appropriate error
    def test_invalid_type(self, streamlist):
        with pytest.raises(
            PrintableGenaiError,
            match="Provided rescore does not match any of the possible ones",
        ):
            RescoreFactory("invalid_type")
