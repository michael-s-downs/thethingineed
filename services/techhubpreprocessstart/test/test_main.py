### This code is property of the GGAO ###

import pytest
import pandas as pd
from copy import deepcopy
from unittest.mock import patch, MagicMock
from main import PreprocessStartDeployment

# Sample JSON input for testing
sample_input = {
    'process_type': 'ir_index',
    'project_type': 'sample_project',
    'dataset_conf': {
        'dataset_csv_path': 'sample_dataset.csv',
    },
    'preprocess_conf': {
        'some_preprocess_param': 'value',
    },
    'headers': {
        'x-tenant': 'tenant1',
        'x-department': 'dept1',
        'x-reporting': 'http://reporting.url',
    },
    'tracking': {}
}

@pytest.fixture
def deployment():
    """Creates instance of PreprocessStartDeployment for testing."""
    with patch('main.load_file', return_value=b'{}') as mock_load_file:
        with patch('main.db_dbs', return_value={'status': ('redis', None), 'timeout': ('redis', None), 'session': ('redis', '4')}):
            with patch("main.set_queue"):
                pdm = PreprocessStartDeployment()
                pdm.json_base = {
                    "generic": {
                        "project_conf": {
                            "force_ocr": False,
                            "laparams": "none"
                        },
                        "ocr_conf": {
                            "batch_length": 16,
                            "files_size": 10485760,
                            "calls_per_minute": 400
                        }
                    }
                }
                return pdm

@patch('main.load_file')
@patch('main.provider')
@patch('main.set_queue')
def test_init(mock_set_queue, mock_provider, mock_load_file, deployment):
    """Test the constructor and setup in the __init__."""
    mock_load_file.return_value.decode.return_value = '{"generic": {}}'
    deployment.__init__()
    mock_set_queue.assert_called()

def test_must_continue_property(deployment):
    """Test that must_continue property returns False."""
    assert deployment.must_continue is False

def test_service_name_property(deployment):
    """Test the service_name property."""
    assert deployment.service_name == "preprocess_start"

def test_max_num_queue_property(deployment):
    """Test the max_num_queue property."""
    assert deployment.max_num_queue == 1

@patch('main.deepcopy')
def test_get_json_generic(mock_deepcopy, deployment):
    """Test get_json_generic returns correct output."""
    mock_deepcopy.return_value = {
        'generic': {
            'project_conf':{}, 
            'url_sender':"http:test.com"
        }, 
        'origins':{},
    }
    sample_input['csv'] = False
    sample_input['force_ocr'] = False
    sample_input['extract_tables'] = False
    sample_input['origins'] = {}
    json_output = deployment.get_json_generic(sample_input, 'tenant1', 'dept1', 'http://reporting.url')
    
    assert 'project_conf' in json_output
    assert json_output['project_conf']['process_id'] is not None

@patch('main.get_dataset_config')
def test_get_json_specific(mock_get_dataset_config, deployment):
    """Test get_json_specific returns correct output."""
    mock_get_dataset_config.return_value = {'dataset_id': 'sample_id'}
    generic = {'project_conf': {'process_id': 'process1', "department":"main"}, 'dataset_conf': {'dataset_id': 'dataset1'}}
    
    specific = deployment.get_json_specific(generic)
    assert 'path_txt' in specific
    assert specific['dataset']['dataset_key'] == "process1:sample_id"

@patch('main.get_dataset')
@patch('main.get_exc_info')
def test_get_dataset_files_from_storage(mock_exc_info, mock_get_dataset, deployment):
    """Test get_dataset_files retrieves files from cloud storage."""
    dataset_conf = {'dataset_csv_path': 'some_path'}
    mock_get_dataset.return_value = MagicMock()
    
    df = deployment.get_dataset_files(dataset_conf, 'dataset_status_key')
    assert df is not None
    mock_get_dataset.assert_called_with(origin=('azure', None), dataset_type='csv', path_name='some_path')

def test_get_json_generic_missing_keys(deployment):
    """Test get_json_generic with missing keys in input JSON."""
    incomplete_input = deepcopy(sample_input)
    del incomplete_input['dataset_conf']
    deployment.json_base = {}
    
    with pytest.raises(KeyError):
        deployment.get_json_generic(incomplete_input, 'tenant1', 'dept1', 'http://reporting.url')

@patch('main.get_dataset_config')
@patch('main.generate_dataset_status_key')
@patch('main.update_status')
@patch('main.PreprocessStartDeployment.list_documents')
def test_process_success(mock_update_status, mock_generate_dataset_status_key, mock_get_dataset_config, mock_list_documents, deployment):
    mock_generate_dataset_status_key.return_value = "key123"
    mock_get_dataset_config.return_value = {'dataset_csv_path': 'some_path'}
    sample_input['csv'] = False
    sample_input['force_ocr'] = False
    sample_input['extract_tables'] = False
    sample_input['origins'] = {}
    
    with patch.object(deployment, 'get_dataset_files', return_value=pd.DataFrame()):
        with patch.object(deployment, 'get_json_specific', return_value={'path_txt': 'some_path'}):
            with patch.object(deployment, 'add_status'):
                must_continue, _, next_service = deployment.process(sample_input)
                assert must_continue is False

@patch('main.update_status')
def test_process_keyerror_config(mock_update_status, deployment):
    """Test the main process method when it fails."""
    with patch.object(deployment, 'adapt_input', return_value ={'generic': {}}):
        with patch('main.get_project_config', side_effect=KeyError("Error")):
            must_continue, _, next_service = deployment.process(sample_input)
            assert must_continue is True
            assert next_service == 'preprocess_end'

@patch('main.update_status')
def test_process_keyerror_dataset(mock_update_status, deployment):
    """Test the main process method when it fails."""
    with patch.object(deployment, 'adapt_input', return_value ={'generic': {}}):
        with patch('main.get_project_config', return_value={}):
            with patch('main.get_dataset_config', return_value={}):
                with patch('main.get_dataset_status_key', side_effect=KeyError("Error")):
                    must_continue, _, next_service = deployment.process(sample_input)
                    assert must_continue is True
                    assert next_service == 'preprocess_end'

@patch('main.update_status')
def test_process_keyerror_dataset_files(mock_update_status, deployment):
    """Test the main process method when it fails."""
    with patch.object(deployment, 'adapt_input', return_value ={'generic': {}}):
        with patch('main.get_project_config', return_value={}):
            with patch('main.get_dataset_config', return_value={}):
                with patch('main.get_dataset_status_key', return_value={}):
                    with patch.object(deployment, 'get_dataset_files', side_effect=KeyError("Error")):
                        must_continue, _, next_service = deployment.process(sample_input)
                        assert must_continue is True
                        assert next_service == 'preprocess_end'

@patch('main.update_status')
def test_process_keyerror_update_status(mock_update_status, deployment):
    """Test the main process method when it fails."""
    with patch.object(deployment, 'adapt_input', return_value ={'generic': {}}):
        with patch('main.get_project_config', return_value={}):
            with patch('main.get_dataset_config', return_value={}):
                with patch('main.get_dataset_status_key', return_value={}):
                    with patch.object(deployment, 'get_dataset_files'):
                        with patch.object(deployment, 'list_documents', side_effect=Exception("Error")):
                            must_continue, _, next_service = deployment.process(sample_input)
                            assert must_continue is True
                            assert next_service == 'preprocess_end'

@patch('main.update_status')
def test_process_keyerror_add_status(mock_update_status, deployment):
    """Test the main process method when it fails."""
    with patch.object(deployment, 'adapt_input', return_value ={'generic': {}}):
        with patch('main.get_project_config', return_value={}):
            with patch('main.get_dataset_config', return_value={}):
                with patch('main.get_dataset_status_key', return_value={}):
                    with patch.object(deployment, 'get_dataset_files', return_value={}):
                        with patch.object(deployment, 'add_status', side_effect=KeyError, return_value=KeyError):
                            must_continue, _, next_service = deployment.process(sample_input)
                            assert must_continue is True
                            assert next_service == 'preprocess_end'

@patch('main.write_to_queue')
@patch('main.update_status')
def test_list_documents(mock_update_status, mock_write_to_queue, deployment):
    """Test list_documents method sends data to the queue."""
    message = MagicMock()
    df = MagicMock()
    df.__len__ = MagicMock(return_value=1)
    df.apply = MagicMock(return_value=None)
    
    deployment.list_documents({'status':{}}, df, 'status_key', {}, message, csv_method=False)
    df.apply.assert_called()

@patch('main.write_to_queue')
@patch('main.update_status')
def test_list_documents_csv_method(mock_update_status, mock_write_to_queue, deployment):
    """Test list_documents method sends data to the queue."""
    message = MagicMock()
    df = MagicMock()
    df.__len__ = MagicMock(return_value=1)
    df.apply = MagicMock(return_value=None)
    
    deployment.list_documents({'status':{}}, df, 'status_key', {}, message, csv_method=True)
    mock_write_to_queue.assert_called()


@patch('main.write_to_queue')
@patch('main.update_status')
def test_list_documents_empty_df(mock_update_status, mock_write_to_queue, deployment):
    """Test list_documents raises exception for empty dataframe."""
    df = pd.DataFrame()
    message = MagicMock()
    
    with pytest.raises(Exception):
        deployment.list_documents({}, df, 'status_key', {}, message, csv_method=False)


@patch('main.get_dataset')
def test_get_dataset_files_csv(mock_get_dataset, deployment):
    """Test get_dataset_files when dataset is of type 'csv'."""
    dataset_conf = {'dataset_csv_path': 'sample_path.csv'}
    mock_get_dataset.return_value = pd.DataFrame() 

    df = deployment.get_dataset_files(dataset_conf, 'dataset_status_key')
    
    mock_get_dataset.assert_called_once_with(origin=('azure', None), dataset_type='csv', path_name='sample_path.csv')
    assert isinstance(df, pd.DataFrame), "Expected a DataFrame to be returned"

@patch('main.get_dataset')
def test_get_dataset_files_invalid_type(mock_get_dataset, deployment):
    """Test get_dataset_files when dataset type is invalid."""
    dataset_conf = {'dataset_csv_path': 'sample_path.unknown'}
    mock_get_dataset.side_effect = ValueError("Unsupported dataset type")  # Simulate an exception for invalid types

    with pytest.raises(Exception):
        deployment.get_dataset_files(dataset_conf, 'dataset_status_key')


@patch('main.get_dataset')
def test_get_dataset_files_multiple_files(mock_get_dataset, deployment):
    """Test get_dataset_files when multiple files are retrieved from storage."""
    dataset_conf = {'dataset_csv_path': 'sample_path'}
    mock_get_dataset.return_value = pd.DataFrame([{"col1": "val1"}, {"col1": "val2"}])  # Simulate multiple records
    
    df = deployment.get_dataset_files(dataset_conf, 'dataset_status_key')
    
    assert df.shape[0] == 2, "Expected 2 records in the DataFrame"

@patch('main.get_dataset')
def test_get_dataset_files_inconf(mock_get_dataset, deployment):
    """Test get_dataset_files when dataset is of type 'csv'."""
    dataset_conf = {'dataset_csv_path': 'sample_path.csv', 'files':{}}
    mock_get_dataset.return_value = pd.DataFrame() 

    df = deployment.get_dataset_files(dataset_conf, 'dataset_status_key')
    
    assert isinstance(df, pd.DataFrame), "Expected a DataFrame to be returned"

@patch('main.get_dataset')
def test_get_dataset_files_inconf_exception(mock_get_dataset, deployment):
    """Test get_dataset_files when dataset is of type 'csv'."""
    dataset_conf = {'dataset_csv_path': 'sample_path.csv', 'files':{'hola':'test'}, 'path_col':[], 'label_col':[]}

    with pytest.raises(Exception):
        deployment.get_dataset_files(dataset_conf, 'dataset_status_key')

@patch('main.update_status')
@patch('main.create_status')
def test_add_status(mock_update_status, mock_create_status, deployment):
    project_conf = {'project_conf':{}, 'timeout_id': 'timeout_id'}
    dataset_conf = {'dataset_conf':{}, 'path_col': 'path_col', 'label_col': 'label_col'}
    df = MagicMock()
    df.iloc = MagicMock(return_value=[{'path_col': 'path1', 'label_col': 'label1'}, {'path_col': 'path2', 'label_col': 'label2'}])
    with patch('main.json.dumps', return_value='{}'):
        deployment.add_status({'status':{}, 'timeout':{}}, 'dataset_status_key', project_conf, dataset_conf, df, {})

@patch('main.update_status')
@patch('main.create_status', side_effect=Exception("Error"))
def test_add_status_exception(mock_update_status, mock_create_status, deployment):
    project_conf = {'project_conf':{}, 'timeout_id': 'timeout_id'}
    dataset_conf = {'dataset_conf':{}, 'path_col': 'path_col', 'label_col': 'label_col'}
    df = MagicMock()
    df.iloc = MagicMock(return_value=[{'path_col': 'path1', 'label_col': 'label1'}, {'path_col': 'path2', 'label_col': 'label2'}])

    with pytest.raises(Exception):
        with patch('main.json.dumps', return_value='{}'):
            deployment.add_status({'status':{}}, 'dataset_status_key', project_conf, dataset_conf, df, {})

@patch('main.update_status', side_effect=Exception("Error"))
def test_add_status_exception2(mock_update_status, deployment):
    project_conf = {'project_conf':{}, 'timeout_id': 'timeout_id'}
    dataset_conf = {'dataset_conf':{}, 'path_col': 'path_col', 'label_col': 'label_col'}
    df = MagicMock()
    df.iloc = MagicMock(return_value=[{'path_col': 'path1', 'label_col': 'label1'}, {'path_col': 'path2', 'label_col': 'label2'}])

    with pytest.raises(Exception):
        with patch('main.json.dumps', return_value='{}'):
            deployment.add_status({'status':{}}, 'dataset_status_key', project_conf, dataset_conf, df, {})

@patch('main.write_to_queue')
@patch('main.list_files')
@patch('main.update_status')
def test_process_row(mock_update_status, mock_list_files, mock_write_to_queue, deployment):
    row = MagicMock()
    row.__getitem__ = MagicMock(return_value='file1')
    dataset_status_key = 'dataset_status_key'
    redis_status = {'status':{}, 'timeout':{}}
    dataset_conf = {'dataset_conf':{}, 'dataset_path':'test_data_path', 'path_col': 'path_col', 'label_col': 'label_col'}
    mesasge = {'specific':{}}
    mock_list_files.return_value = ['file1', 'file2']
    with patch.object(deployment, 'generate_tracking_message'):
        deployment.process_row(row, dataset_status_key, redis_status, dataset_conf, mesasge)

    
@patch('main.write_to_queue')
@patch('main.list_files')
@patch('main.update_status')
def test_process_row_exception(mock_update_status, mock_list_files, mock_write_to_queue, deployment):
    row = MagicMock()
    row.__getitem__ = MagicMock(return_value='file1')
    dataset_status_key = 'dataset_status_key'
    redis_status = {'status':{}, 'timeout':{}}
    dataset_conf = {'dataset_conf':{}, 'dataset_path':'test_data_path', 'path_col': 'path_col', 'label_col': 'label_col'}
    mesasge = {'specific':{}}
    mock_list_files.return_value = ['file1', 'file2']
    mock_write_to_queue.side_effect = Exception("Error")
    with pytest.raises(Exception):
        with patch.object(deployment, 'generate_tracking_message'):
            deployment.process_row(row, dataset_status_key, redis_status, dataset_conf, mesasge)