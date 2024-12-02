### This code is property of the GGAO ###

import os
os.environ['URL_LLM'] = "test_url"
os.environ['URL_RETRIEVE'] = "test_retrieve"
import pytest
from compose.streamlist import StreamList, StreamChunk
from compose.utils.defaults import EMPTY_STREAM
from common.errors.genaierrors import GenaiError
from compose.batchactions.combinebatch import JoinCombine2, CombineJoin2, CombineBatchFactory, CombineBatchMethod


@pytest.fixture
def stream_list_1():
    sls = StreamList()
    sls.append(StreamChunk({"content": "Chunk 1 from SL1"}))
    sls.append(StreamChunk({"content": "Chunk 2 from SL1"}))
    return sls

@pytest.fixture
def stream_list_2():
    sls = StreamList()
    sls.append(StreamChunk({"content": "Chunk 1 from SL2"}))
    sls.append(StreamChunk({"content": "Chunk 2 from SL2"}))
    return sls

@pytest.fixture
def stream_list_3():
    sls = StreamList()
    sls.append(StreamChunk({"content": "Chunk 1 from SL3"}))
    sls.append(StreamChunk({"content": "Chunk 2 from SL3"}))
    sls.append(StreamChunk({"content": "Chunk 3 from SL3"}))
    return sls

@pytest.fixture
def empty_stream():
    return {"content": ""}


def test_join_combine_two_success(stream_list_1, stream_list_2, empty_stream):
    template = "$s1\n$s2"
    combine_method = JoinCombine2([stream_list_1, stream_list_2])
    
    result = combine_method.process(template, SEP="\n")
    
    # Expected output after combining
    expected_content = "Chunk 1 from SL1\nChunk 2 from SL1\nChunk 1 from SL2\nChunk 2 from SL2"
    
    assert result[0].content == expected_content

def test_combine_join_two_success(stream_list_1, stream_list_2, empty_stream):
    template = "$s1 + $s2"
    combine_method = CombineJoin2([stream_list_1, stream_list_2])

    result = combine_method.process(template, unique_streamlist=False)

    # Expected output after combining
    expected_contents = [
        "Chunk 1 from SL1 + Chunk 1 from SL2",
        "Chunk 2 from SL1 + Chunk 2 from SL2"
    ]

    for idx, chunk in enumerate(result):
        assert chunk.content == expected_contents[idx]

def test_combine_with_incorrect_streamlist_length():
    stream_list_1 = StreamList()
    stream_list_1.append(StreamChunk({"content": "Content from SL1"}))

    # Expect failure when passing only one StreamList instead of two
    with pytest.raises(GenaiError, match="CombineTwoSlBatch can only be used with two streamlists"):
        JoinCombine2([stream_list_1])

def test_combinejoin_with_incorrect_streamlist_length():
    stream_list_1 = StreamList()
    stream_list_1.append(StreamChunk({"content": "Content from SL1"}))

    # Expect failure when passing only one StreamList instead of two
    with pytest.raises(GenaiError, match="CombineTwoSlBatch can only be used with two streamlists"):
        CombineJoin2([stream_list_1])

def test_combinebatch_factory_success(stream_list_1, stream_list_2):
    # Factory setup
    factory = CombineBatchFactory("combinejoin2")
    
    params = {
        "template": "$s1 + $s2",
        "unique_streamlist": True
    }
    
    result = factory.process([stream_list_1, stream_list_2], params)
    
    expected_content = "Chunk 1 from SL1 + Chunk 1 from SL2\nChunk 2 from SL1 + Chunk 2 from SL2"
    
    assert result[0].content == expected_content

def test_combinebatch_factory_invalid_type():
    # Invalid combine batch type should raise an error
    with pytest.raises(GenaiError, match="Provided combinebatch does not match any of the possible ones"):
        CombineBatchFactory("invalidtype")

def test_combinebatchmethod_init(stream_list_1, stream_list_2):
    cbm = CombineBatchMethod([stream_list_1, stream_list_2])
    cbm.process()
    assert cbm.streambatch == [stream_list_1, stream_list_2]

def test_combine_join_two_different_size(stream_list_1, stream_list_3):
    template = "$s1 + $s2"
    combine_method = CombineJoin2([stream_list_1, stream_list_3])

    result = combine_method.process(template, unique_streamlist=False)

    # Expected output after combining
    expected_contents = [
        "Chunk 1 from SL1 + Chunk 1 from SL3",
        "Chunk 2 from SL1 + Chunk 2 from SL3"
    ]

    for idx, chunk in enumerate(result):
        assert chunk.content == expected_contents[idx]

    combine_method = CombineJoin2([stream_list_3, stream_list_1])

    result = combine_method.process(template, unique_streamlist=False)

    # Expected output after combining
    expected_contents = [
        "Chunk 1 from SL3 + Chunk 1 from SL1",
        "Chunk 2 from SL3 + Chunk 2 from SL1"
    ]

    for idx, chunk in enumerate(result):
        assert chunk.content == expected_contents[idx]