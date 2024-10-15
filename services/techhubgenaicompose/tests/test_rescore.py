### This code is property of the GGAO ###

import pytest
from compose.actions.rescore import (
    # RescoreMethod,
    AverageRescore,
    GenaiRescorer,
    RescoreFactory,
)
from unittest.mock import patch
from common.errors.genaierrors import PrintableGenaiError


class MockStream:
    def __init__(self, snippet_id, scores):
        self.meta = {"snippet_id": snippet_id}
        self.scores = scores


@pytest.fixture
def streamlist():
    return [
        MockStream("id1", {"score1": 0.8, "score2": 0.6}),
        MockStream("id2", {"score1": 0.4, "score2": 0.5}),
    ]


class TestAverageRescore:
    def test_process(self, streamlist):
        rescore = AverageRescore(streamlist)
        processed_streams = rescore.process()
        assert len(processed_streams) == len(streamlist)
        assert all("mean" in stream.scores for stream in processed_streams)

    def test_get_example(self):
        rescore = AverageRescore([])
        example = rescore.get_example()
        assert example == '{"type": "mean", "params": null}'


class TestGenaiRescorer:
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

    def test_get_document_ids(self, streamlist):
        rescore = GenaiRescorer(streamlist)
        doc_ids = list(rescore.get_document_ids())
        assert doc_ids == ["id1", "id2"]


class TestRescoreFactory:
    def test_process_average(self, streamlist):
        factory = RescoreFactory("mean")
        processed_streams = factory.process(streamlist, {})
        assert len(processed_streams) == len(streamlist)

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

    def test_invalid_type(self, streamlist):
        with pytest.raises(
            PrintableGenaiError,
            match="Provided rescore does not match any of the possible ones",
        ):
            RescoreFactory("invalid_type")
