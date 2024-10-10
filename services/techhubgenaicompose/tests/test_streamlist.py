### This code is property of the GGAO ###

import pytest
from compose.streamlist import StreamList
from compose.streamchunk import StreamChunk
from common.errors.genaierrors import GenaiError
from unittest.mock import MagicMock


@pytest.fixture
def stream_chunk():
    return StreamChunk({
        "content": "This is a test chunk",
        "meta": {"source": "test_source", "id": 1},
        "scores": {"relevance": 0.9, "accuracy": 0.8},
        "answer": "Example answer 123"
    })

@pytest.fixture
def stream_list():
    return StreamList()

def test_append_valid_chunk(stream_list, stream_chunk):
    stream_list.append(stream_chunk)
    assert len(stream_list) == 1
    assert stream_list[0] == stream_chunk

def test_append_invalid_chunk(stream_list):
    with pytest.raises(GenaiError):
        stream_list.append("invalid_chunk")

def test_check_valid_chunk(stream_list, stream_chunk):
    # Should not raise an error
    stream_list.check(stream_chunk)

def test_check_invalid_chunk(stream_list):
    with pytest.raises(GenaiError):
        stream_list.check("invalid_chunk")

def test_getitem(stream_list, stream_chunk):
    stream_list.append(stream_chunk)
    assert stream_list[0] == stream_chunk

def test_delitem(stream_list, stream_chunk):
    stream_list.append(stream_chunk)
    del stream_list[0]
    assert len(stream_list) == 0

def test_str_method(stream_list, stream_chunk):
    stream_list.append(stream_chunk)
    # Test if __str__ returns the expected string representation
    expected_str = str([stream_chunk])
    assert str(stream_list) == expected_str

def test_repr_method(stream_list, stream_chunk):
    stream_list.append(stream_chunk)
    # Test if __repr__ returns the expected string representation (same as __str__)
    expected_repr = str([stream_chunk])
    assert repr(stream_list) == expected_repr

def test_iter_method(stream_list, stream_chunk):
    stream_list.append(stream_chunk)
    stream_list.append(StreamChunk({
        "content": "Second chunk",
        "meta": {},
        "scores": {},
        "answer": "Second answer"
    }))
    
    # Collecting elements via iteration
    iterated_elements = [chunk for chunk in stream_list]

    assert iterated_elements == stream_list.to_list()

def test_setitem_valid(stream_list, stream_chunk):
    stream_list.append(stream_chunk)
    new_chunk = StreamChunk({
        "content": "New chunk content",
        "meta": {"source": "new_source", "id": 2},
        "scores": {"relevance": 0.95, "accuracy": 0.85},
        "answer": "New answer"
    })
    stream_list[0] = new_chunk
    assert stream_list[0] == new_chunk

def test_setitem_invalid(stream_list, stream_chunk):
    stream_list.append(stream_chunk)
    with pytest.raises(GenaiError):
        stream_list[0] = "invalid_chunk"

def test_insert_valid(stream_list, stream_chunk):
    # Add an initial stream chunk to the list
    new_chunk = StreamChunk({
        "content": "New chunk content",
        "meta": {"source": "new_source", "id": 2},
        "scores": {"relevance": 0.95, "accuracy": 0.85},
        "answer": "New answer"
    })
    
    stream_list.append(stream_chunk)
    # Insert a new chunk at the beginning of the list
    stream_list.insert(0, new_chunk)
    
    assert len(stream_list) == 2
    assert stream_list[0] == new_chunk
    assert stream_list[1] == stream_chunk

def test_insert_invalid(stream_list, stream_chunk):
    stream_list.append(stream_chunk)
    with pytest.raises(GenaiError):
        stream_list.insert(0, "invalid_chunk")

def test_to_list(stream_list, stream_chunk):
    stream_list.append(stream_chunk)
    assert stream_list.to_list() == [stream_chunk]

def test_to_list_serializable(stream_list, stream_chunk):
    stream_list.append(stream_chunk)
    expected_output = [vars(stream_chunk)]
    assert stream_list.to_list_serializable() == expected_output

def test_join_get_content(stream_list, stream_chunk):
    stream_list.append(stream_chunk)
    stream_list.append(StreamChunk({
        "content": "Second chunk",
        "meta": {},
        "scores": {},
        "answer": "Second answer"
    }))
    assert stream_list.join_get_content() == "This is a test chunk Second chunk"

# Tests using MagicMock to mock the factory methods
def test_retrieve(mocker, stream_list):
    mock_retriever_factory = MagicMock()
    mock_retriever_factory.process.return_value = [{"content": "mock_chunk", "meta": {}, "scores": {}, "answer": ""}]
    mocker.patch('compose.streamlist.RetrieverFactory', return_value=mock_retriever_factory)

    stream_list.retrieve("mock_type", {"param": "value"})
    assert len(stream_list) == 1
    assert stream_list[0].content == "mock_chunk"

def test_filter(mocker, stream_list, stream_chunk):
    stream_list.append(stream_chunk)
    mock_filter_factory = MagicMock()
    mock_filter_factory.process.return_value = [StreamChunk({"content": "mock_chunk", "meta": {}, "scores": {}, "answer": ""})]
    mocker.patch('compose.streamlist.FilterFactory', return_value=mock_filter_factory)

    stream_list.filter("mock_type", {"param": "value"})
    assert len(stream_list) == 1
    assert stream_list[0].content == "mock_chunk"

def test_rescore(mocker, stream_list, stream_chunk):
    stream_list.append(stream_chunk)
    mock_rescore_factory = MagicMock()
    mock_rescore_factory.process.return_value = [StreamChunk({"content": "mock_chunk", "meta": {}, "scores": {}, "answer": ""})]
    mocker.patch('compose.streamlist.RescoreFactory', return_value=mock_rescore_factory)

    stream_list.rescore("mock_type", {"param": "value"})
    assert len(stream_list) == 1
    assert stream_list[0].content == "mock_chunk"

def test_merge(mocker, stream_list, stream_chunk):
    stream_list.append(stream_chunk)
    mock_merge_factory = MagicMock()
    mock_merge_factory.process.return_value = [StreamChunk({"content": "mock_chunk", "meta": {}, "scores": {}, "answer": ""})]
    mocker.patch('compose.streamlist.MergeFactory', return_value=mock_merge_factory)

    stream_list.merge("mock_type", {"param": "value"})
    assert len(stream_list) == 1
    assert stream_list[0].content == "mock_chunk"

def test_llm_action(mocker, stream_list, stream_chunk):
    stream_list.append(stream_chunk)
    mock_llm_factory = MagicMock()
    mock_llm_factory.process.return_value = [StreamChunk({"content": "mock_chunk", "meta": {}, "scores": {}, "answer": ""})]
    mocker.patch('compose.streamlist.LLMFactory', return_value=mock_llm_factory)

    stream_list.llm_action("mock_type", {"param": "value"})
    assert len(stream_list) == 1
    assert stream_list[0].content == "mock_chunk"

def test_sort(mocker, stream_list, stream_chunk):
    stream_list.append(stream_chunk)
    mock_sort_factory = MagicMock()
    mock_sort_factory.process.return_value = [StreamChunk({"content": "mock_chunk", "meta": {}, "scores": {}, "answer": ""})]
    mocker.patch('compose.streamlist.SortFactory', return_value=mock_sort_factory)

    stream_list.sort("mock_type", {"param": "value"})
    assert len(stream_list) == 1
    assert stream_list[0].content == "mock_chunk"

def test_groupby(mocker, stream_list, stream_chunk):
    stream_list.append(stream_chunk)
    mock_groupby_factory = MagicMock()
    mock_groupby_factory.process.return_value = [StreamChunk({"content": "mock_chunk", "meta": {}, "scores": {}, "answer": ""})]
    mocker.patch('compose.streamlist.GroupByFactory', return_value=mock_groupby_factory)

    stream_list.groupby("mock_type", {"param": "value"})
    assert len(stream_list) == 1
    assert stream_list[0].content == "mock_chunk"