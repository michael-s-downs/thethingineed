
import pytest
from compose.streamlist import StreamList, StreamChunk
from common.errors.genaierrors import GenaiError
from compose.batchactions.mergebatch import AddMergeBatch, MergeBatchFactory, MergeBatchMethod


@pytest.fixture
def stream_list_with_duplicates():
    sls = StreamList()
    sls.append(StreamChunk({"content": "Chunk A", "meta":{"snippet_id": "1"}}))
    sls.append(StreamChunk({"content": "Chunk B", "meta":{"snippet_id": "2"}}))
    sls.append(StreamChunk({"content": "Chunk A", "meta":{"snippet_id": "1"}}))  # Duplicate
    return sls


@pytest.fixture
def stream_list_without_duplicates():
    sls = StreamList()
    sls.append(StreamChunk({"content": "Chunk A", "meta":{"snippet_id": "1"}}))
    sls.append(StreamChunk({"content": "Chunk B", "meta":{"snippet_id": "2"}}))
    return sls


def test_add_merge_batch_no_duplicates(stream_list_without_duplicates):
    merge_batch = AddMergeBatch([stream_list_without_duplicates])
    result = merge_batch.process()

    # Since there are no duplicates, expect all chunks to be in the result
    assert len(result) == 2
    assert result[0].content == "Chunk A"
    assert result[1].content == "Chunk B"


def test_add_merge_batch_with_duplicates(stream_list_with_duplicates):
    merge_batch = AddMergeBatch([stream_list_with_duplicates])
    result = merge_batch.process()

    # Expect only unique chunks in the result
    assert len(result) == 2
    assert result[0].content == "Chunk A"
    assert result[1].content == "Chunk B"


def test_merge_batch_factory_success(stream_list_with_duplicates):
    factory = MergeBatchFactory("add")
    params = {}
    result = factory.process([stream_list_with_duplicates], params)

    # Expect only unique chunks in the result
    assert len(result) == 2
    assert result[0].content == "Chunk A"
    assert result[1].content == "Chunk B"


def test_merge_batch_factory_invalid_type():
    with pytest.raises(GenaiError, match="Provided mergebatch does not match any of the possible ones"):
        MergeBatchFactory("invalid_merge_type")


def test_merge_batch_factory_no_streams():
    factory = MergeBatchFactory("add")
    
    # Expect to handle empty list gracefully
    result = factory.process([], {})
    assert len(result) == 0


def test_merge_batch_factory_multiple_streams_with_duplicates():
    sl1 = StreamList()
    sl1.append(StreamChunk({"content": "Chunk A", "meta":{"snippet_id": "1"}}))
    sl1.append(StreamChunk({"content": "Chunk B", "meta":{"snippet_id": "2"}}))

    sl2 = StreamList()
    sl2.append(StreamChunk({"content": "Chunk A", "meta":{"snippet_id": "1"}}))  # Duplicate
    sl2.append(StreamChunk({"content": "Chunk C", "meta":{"snippet_id": "3"}}))

    factory = MergeBatchFactory("add")
    params = {}
    result = factory.process([sl1, sl2], params)

    # Expect only unique chunks in the result
    assert len(result) == 3
    assert result[0].content == "Chunk A"
    assert result[1].content == "Chunk B"
    assert result[2].content == "Chunk C"


def test_add_merge_batch_with_empty_stream():
    empty_stream = StreamList()
    non_empty_stream = StreamList()
    non_empty_stream.append(StreamChunk({"content": "Chunk 1", "meta": {"snippet_id": "1"}}))

    merge_batch = AddMergeBatch([empty_stream, non_empty_stream])
    result = merge_batch.process()

    # Expect the non-empty stream's chunk to be in the result
    assert len(result) == 1
    assert result[0].content == "Chunk 1"


def test_add_merge_batch_multiple_empty_streams():
    empty_stream1 = StreamList()
    empty_stream2 = StreamList()
    
    merge_batch = AddMergeBatch([empty_stream1, empty_stream2])
    result = merge_batch.process()

    # Expect an empty result since all streams are empty
    assert len(result) == 0

def test_init():
    merge_m = MergeBatchMethod([])
    merge_m.process()
    merge_m.streambatch = []