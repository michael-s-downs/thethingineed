### This code is property of the GGAO ###

import os
os.environ['URL_LLM'] = "test_url"
os.environ['URL_RETRIEVE'] = "test_retrieve"
import pytest
from common.errors.genaierrors import GenaiError
from compose.batchactions.sortbatch import BatchSortScore, BatchSortLength, BatchSortFactory, BatchSortMethod
from compose.streamlist import StreamList, StreamChunk


@pytest.fixture
def stream_list_score_1():
    sl = StreamList()
    sl.append(StreamChunk({"content": "Chunk 1", "scores": {'text-embedding-ada-002--score': 0.5, 'bm25--score': 0}}))
    sl.append(StreamChunk({"content": "Chunk 2", "scores": {'text-embedding-ada-002--score': 0.8, 'bm25--score': 0}}))
    return sl


@pytest.fixture
def stream_list_score_2():
    sl = StreamList()
    sl.append(StreamChunk({"content": "Chunk 3", "scores": {'text-embedding-ada-002--score': 0.6, 'bm25--score': 0}}))
    sl.append(StreamChunk({"content": "Chunk 4", "scores": {'text-embedding-ada-002--score': 0.9, 'bm25--score': 0}}))
    return sl


@pytest.fixture
def stream_list_length_1():
    sl = StreamList()
    sl.append(StreamChunk({"content": "Short", "scores": {'text-embedding-ada-002--score': 0.1, 'bm25--score': 0}}))  # Length 5
    sl.append(StreamChunk({"content": "A bit longer", "scores": {'text-embedding-ada-002--score': 0.2, 'bm25--score': 0}}))  # Length 13
    return sl


@pytest.fixture
def stream_list_length_2():
    sl = StreamList()
    sl.append(StreamChunk({"content": "Tiny", "scores": {'text-embedding-ada-002--score': 0.3, 'bm25--score': 0}}))  # Length 4
    sl.append(StreamChunk({"content": "This is a much longer chunk.", "scores": {'text-embedding-ada-002--score': 0.4, 'bm25--score': 0}}))  # Length 31
    return sl


def test_batch_sort_score_success(stream_list_score_1, stream_list_score_2):
    bf = BatchSortFactory("score")
    sorted_streams = bf.process([stream_list_score_1, stream_list_score_2], {})

    # Expected mean scores: 
    # stream_list_score_1: (0.5 + 0.8) / 2 = 0.65
    # stream_list_score_2: (0.6 + 0.9) / 2 = 0.75
    assert sorted_streams[0][0].content == "Chunk 3"  # Highest score
    assert sorted_streams[1][0].content == "Chunk 1"  # Second highest score


def test_batch_sort_score_descending(stream_list_score_1, stream_list_score_2):
    sort_method = BatchSortScore([stream_list_score_1, stream_list_score_2])
    sorted_streams = sort_method.process({'desc': True})

    # Check the highest score
    assert sorted_streams[0][0].scores['text-embedding-ada-002--score'] == 0.6 
    assert sorted_streams[1][0].scores['text-embedding-ada-002--score'] == 0.5


def test_batch_sort_score_ascending(stream_list_score_1, stream_list_score_2):
    sort_method = BatchSortScore([stream_list_score_1, stream_list_score_2])
    sorted_streams = sort_method.process({'desc': False})

    # Should sort in ascending order based on scores
    assert sorted_streams[0][0].scores['text-embedding-ada-002--score'] == 0.5
    assert sorted_streams[1][0].scores['text-embedding-ada-002--score'] == 0.5


def test_batch_sort_length_success(stream_list_length_1, stream_list_length_2):
    sort_method = BatchSortLength([stream_list_length_1, stream_list_length_2])
    sorted_streams = sort_method.process({})

    assert sorted_streams[0][0].content == "Tiny"  # Length 31
    assert sorted_streams[1][0].content == "Short"  # Length 13


def test_batch_sort_length_descending(stream_list_length_1, stream_list_length_2):
    sort_method = BatchSortLength([stream_list_length_1, stream_list_length_2])
    sorted_streams = sort_method.process({'desc': True})

    assert sorted_streams[0][0].content == "Tiny"
    assert sorted_streams[1][0].content == "Short"


def test_batch_sort_length_ascending(stream_list_length_1, stream_list_length_2):
    sort_method = BatchSortLength([stream_list_length_1, stream_list_length_2])
    sorted_streams = sort_method.process({'desc': False})

    # Should sort in ascending order
    assert sorted_streams[0][0].content == "Short"  # Length 4
    assert sorted_streams[1][0].content == "Short"  # Length 5


def test_batch_sort_factory_success(stream_list_score_1, stream_list_score_2):
    factory = BatchSortFactory("score")
    params = {'desc': True}
    result = factory.process([stream_list_score_1, stream_list_score_2], params)

    # Expect to get sorted stream batches based on score
    assert len(result) == 3


def test_batch_sort_factory_invalid_type():
    with pytest.raises(GenaiError, match="Provided batchsort does not match any of the possible ones"):
        BatchSortFactory("invalid_sort_type")

def test_batch_sort_method_streambatch_error():
    with pytest.raises(GenaiError, match="Error 500: Cant sort 1 or less streambatch"):
        BatchSortMethod([[]])

def test_init():
    bsm = BatchSortMethod([[], []])
    bsm.process()
    assert bsm.streambatch == [[], [], []]