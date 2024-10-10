### This code is property of the GGAO ###

import pytest
from compose.streamlist import StreamList
from compose.streambatch import StreamBatch
from common.errors.genaierrors import GenaiError
from unittest.mock import MagicMock

@pytest.fixture
def mock_streamlist():
    mock_sl = MagicMock(spec=StreamList)
    return mock_sl

@pytest.fixture
def stream_batch(mock_streamlist):
    return StreamBatch(mock_streamlist, mock_streamlist)

def test_init_empty():
    # Test initializing StreamBatch with no StreamList objects
    batch = StreamBatch()
    assert len(batch) == 0

def test_init_with_streamlists(mock_streamlist):
    # Test initializing StreamBatch with StreamList objects
    batch = StreamBatch(mock_streamlist, mock_streamlist)
    assert len(batch) == 2

def test_str_repr(stream_batch, mock_streamlist):
    # Test the string and repr methods
    expected_str = str([mock_streamlist, mock_streamlist])
    assert str(stream_batch) == expected_str
    assert repr(stream_batch) == expected_str

def test_len(stream_batch):
    # Test the length of the StreamBatch
    assert len(stream_batch) == 2

def test_getitem(stream_batch, mock_streamlist):
    # Test getting an item from the StreamBatch
    assert stream_batch[0] == mock_streamlist

def test_to_list(stream_batch, mock_streamlist):
    # Test converting the StreamBatch to a list of lists
    mock_streamlist.to_list.return_value = ['chunk1', 'chunk2']
    result = stream_batch.to_list()
    assert result == [['chunk1', 'chunk2'], ['chunk1', 'chunk2']]

def test_to_list_serializable(stream_batch, mock_streamlist):
    # Test converting the StreamBatch to a serializable list
    mock_streamlist.to_list_serializable.return_value = [{'chunk1': 'data1'}, {'chunk2': 'data2'}]
    result = stream_batch.to_list_serializable()
    assert result == [
        [{'chunk1': 'data1'}, {'chunk2': 'data2'}],
        [{'chunk1': 'data1'}, {'chunk2': 'data2'}]
    ]

def test_shape(stream_batch, mock_streamlist):
    # Test getting the shape of the StreamBatch
    mock_streamlist.__len__.return_value = 2
    result = stream_batch.shape()
    assert result == (2, 2)

def test_add(mock_streamlist):
    # Test adding a StreamList to the StreamBatch
    batch = StreamBatch(mock_streamlist)
    new_streamlist = MagicMock(spec=StreamList)
    batch.add(new_streamlist)
    assert len(batch) == 2
    assert batch[1] == new_streamlist

def test_invalid_list_argument(mock_streamlist):
    # Test that passing a list of StreamList objects raises a ValueError
    with pytest.raises(ValueError, match="Do not pass a list of elements, pass each element as a separated argument"):
        StreamBatch([mock_streamlist, mock_streamlist])

def test_retrieve(stream_batch, mocker):
    # Test the retrieve method for StreamBatch
    mock_streamlist = MagicMock(spec=StreamList)
    mocker.patch('compose.streambatch.StreamList', return_value=mock_streamlist)
    stream_batch.retrieve('test_type', {'param': 'value'})
    assert mock_streamlist.retrieve.called
    assert mock_streamlist.retrieve.call_args == (('test_type', {'param': 'value'}),)

def test_filter(stream_batch):
    # Test filtering the StreamBatch
    stream_batch.filter('filter_type', {'param': 'value'})
    for streamlist in stream_batch:
        streamlist.filter.assert_called_with('filter_type', {'param': 'value'})

def test_sort(stream_batch):
    # Test sorting the StreamBatch
    stream_batch.sort('sort_type', {'param': 'value'})
    for streamlist in stream_batch:
        streamlist.sort.assert_called_with('sort_type', {'param': 'value'})

def test_groupby(stream_batch):
    # Test groupby method for StreamBatch
    stream_batch.groupby('group_type', {'param': 'value'})
    for streamlist in stream_batch:
        streamlist.groupby.assert_called_with('group_type', {'param': 'value'})

def test_rescore(stream_batch):
    # Test rescoring method for StreamBatch
    stream_batch.rescore('rescore_type', {'param': 'value'})
    for streamlist in stream_batch:
        streamlist.rescore.assert_called_with('rescore_type', {'param': 'value'})

def test_merge(stream_batch):
    # Test merge method for StreamBatch
    stream_batch.merge('merge_type', {'param': 'value'})
    for streamlist in stream_batch:
        streamlist.merge.assert_called_with('merge_type', {'param': 'value'})

def test_llm_action(stream_batch):
    # Test LLM action for StreamBatch
    stream_batch.llm_action('summary_type', {'param': 'value'})
    for streamlist in stream_batch:
        streamlist.llm_action.assert_called_with('summary_type', {'param': 'value'})

def test_batchmerge(mocker, stream_batch):
    # Test batch merging in StreamBatch
    merge_factory_mock = mocker.patch('compose.streambatch.MergeBatchFactory')
    merge_factory_mock.return_value.process.return_value = mocker.MagicMock()
    stream_batch.batchmerge('merge_type', {'param': 'value'})
    merge_factory_mock.assert_called_once_with('merge_type')
    merge_factory_mock.return_value.process.assert_called_once()

def test_batchsplit(mocker, stream_batch):
    # Test batch splitting in StreamBatch
    split_factory_mock = mocker.patch('compose.streambatch.SplitBatchFactory')
    split_factory_mock.return_value.process.return_value = mocker.MagicMock()
    stream_batch.batchsplit('split_type', {'param': 'value'})
    split_factory_mock.assert_called_once_with('split_type')
    split_factory_mock.return_value.process.assert_called_once()

def test_batchcombine(mocker, stream_batch):
    # Test batch combining in StreamBatch
    combine_factory_mock = mocker.patch('compose.streambatch.CombineBatchFactory')
    combine_factory_mock.return_value.process.return_value = mocker.MagicMock()
    stream_batch.batchcombine('combine_type', {'param': 'value'})
    combine_factory_mock.assert_called_once_with('combine_type')
    combine_factory_mock.return_value.process.assert_called_once()

def test_batchsort(mocker, stream_batch):
    # Test batch sorting in StreamBatch
    sort_factory_mock = mocker.patch('compose.streambatch.BatchSortFactory')
    sort_factory_mock.return_value.process.return_value = mocker.MagicMock()
    stream_batch.batchsort('sort_type', {'param': 'value'})
    sort_factory_mock.assert_called_once_with('sort_type')
    sort_factory_mock.return_value.process.assert_called_once()
