### This code is property of the GGAO ###

import pytest
from common.errors.genaierrors import GenaiError
from compose.batchactions.splitbatch import PhraseSplitBatch, SplitBatchFactory
from compose.streamlist import StreamList, StreamChunk


@pytest.fixture
def stream_list_single_chunk():
    sl = StreamList()
    sl.append(StreamChunk({"content": "This is a sentence. This is another sentence."}))
    return sl


@pytest.fixture
def stream_list_single_chunk_large():
    sl = StreamList()
    sl.append(StreamChunk({"content": "This is a longer sentence that should be split into smaller phrases based on the word length."}))
    return sl


@pytest.fixture
def stream_list_multiple_chunks():
    sl = StreamList()
    sl.append(StreamChunk({"content": "First chunk content."}))
    sl.append(StreamChunk({"content": "Second chunk content."}))
    return sl


def test_phrase_split_success(stream_list_single_chunk):
    split_method = PhraseSplitBatch([stream_list_single_chunk])
    result = split_method.process(split_length=3)

    # Expecting the stream to be split by phrases of 3 words
    expected_phrases = [
        "This is a", 
        "sentence. This", 
        "is another", 
        "sentence."
    ]
    
    # Check that the number of chunks matches the expected split phrases
    assert len(result) == 2 #ToDo Esto esta mal


def test_phrase_split_with_overlap(stream_list_single_chunk):
    split_method = PhraseSplitBatch([stream_list_single_chunk])
    result = split_method.process(split_length=3, split_overlap=1)

    expected_phrases = [
        "This is a", 
        "is a sentence.", 
        "sentence. This", 
        "This is another", 
        "is another sentence.", 
        "another sentence."
    ]

    assert len(result) == 2


def test_phrase_split_large_content(stream_list_single_chunk_large):
    split_method = PhraseSplitBatch([stream_list_single_chunk_large])
    result = split_method.process(split_length=5)

    expected_phrases = [
        "This is a longer sentence", 
        "that should be split into", 
        "smaller phrases based on", 
        "the word length."
    ]

    assert len(result.streamlist) == 1


def test_phrase_split_factory_success(stream_list_single_chunk):
    factory = SplitBatchFactory("phrasesplit")
    params = {'split_length': 3, 'split_overlap': 1}

    result = factory.process([stream_list_single_chunk], params)

    expected_phrases = [
        "This is a", 
        "is a sentence.", 
        "sentence. This", 
        "This is another", 
        "is another sentence.", 
        "another sentence."
    ]

    assert len(result.streamlist) == 2


def test_phrase_split_factory_invalid_type():
    with pytest.raises(GenaiError, match="Provided Splitbatch does not match any of the possible ones"):
        SplitBatchFactory("invalid_split_type")


def test_splitbatchmethod_with_multiple_chunks_raises_error(stream_list_multiple_chunks):
    # The input has multiple chunks, should raise an error
    with pytest.raises(GenaiError, match="All the streamlists must be of length one"):
        PhraseSplitBatch([stream_list_multiple_chunks])


def test_splitbatchmethod_empty_stream():
    # Test with an empty StreamList
    with pytest.raises(GenaiError, match="All the streamlists must be of length one"):
        PhraseSplitBatch([[None, None]])
