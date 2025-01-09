### This code is property of the GGAO ###

import os
import pytest
import pandas
from unittest.mock import patch, MagicMock
from genai_controllers import (
    set_db, set_queue, write_to_queue, read_from_queue, delete_from_queue, 
    set_storage, check_file, list_files, download_files, upload_files, 
    delete_files, delete_file, get_mimetype, get_number_pages, extract_ocr_files,
    get_dataset, get_sizes, download_file, download_directory, load_file, upload_object, delete_folder,
    get_texts_from_file, get_images_from_file, select_athena, create_athena, partition_athena, execute_query_athena,
    get_query_athena, delete_athena
)

# Mock environment variables
@pytest.fixture(autouse=True)
def set_env_vars():
    os.environ['REDIS_DB_STATUS'] = 'active'
    os.environ['REDIS_DB_TIMEOUT'] = '30'
    os.environ['REDIS_DB_SESSION'] = 'session_id'
    os.environ['REDIS_DB_TEMPLATES'] = 'template_id'
    os.environ['PROVIDER'] = 'aws'
    os.environ['STORAGE_DATA'] = 's3://data'
    os.environ['STORAGE_BACKEND'] = 's3://backend'
    os.environ['BYTES_MODE'] = 'False'

@pytest.fixture
def mock_controllers():
    with patch('genai_controllers.FilesController') as fc, \
         patch('genai_controllers.StorageController') as sc, \
         patch('genai_controllers.QueueController') as qc, \
         patch('genai_controllers.DBController') as dbc, \
         patch('genai_controllers.DataBunchController') as data_bunch_c:
        
        fc_instance = fc.return_value
        sc_instance = sc.return_value
        qc_instance = qc.return_value
        dbc_instance = dbc.return_value
        data_bunch_c_instance = data_bunch_c.return_value

        yield {
            'fc': fc_instance,
            'sc': sc_instance,
            'qc': qc_instance,
            'dbc': dbc_instance,
            'data_bunch_c': data_bunch_c_instance
        }

def test_set_db():
    mock_credentials = {"redis": "dummy_credentials"}
    with patch('genai_sdk_services.db.DBController.set_credentials') as mock_set:
        set_db(mock_credentials)
        mock_set.assert_called_with("dummy_credentials")

def test_set_queue():
    queue = ("aws", "QUEUE_URL")
    with patch.dict(os.environ, {"QUEUE_URL": "https://dummy-queue-url"}):
        with patch('genai_sdk_services.queue_controller.QueueController.set_credentials') as mock_set:
            set_queue(queue)
            mock_set.assert_called_with(queue, "https://dummy-queue-url")

def test_write_to_queue():
    message = {"key": "value"}
    origin_qc = ("aws", "QUEUE_URL")
    with patch.dict(os.environ, {"QUEUE_URL": "https://dummy-queue-url"}):
        with patch('genai_sdk_services.queue_controller.QueueController.write'):
            write_to_queue(origin_qc, message)

def test_read_from_queue(mock_controllers):
    origin_qc = ('aws', 'QUEUE_URL')
    with patch.dict(os.environ, {"QUEUE_URL": "https://dummy-queue-url"}):
        with patch('genai_sdk_services.queue_controller.QueueController.read') as mock_read:
            mock_read.return_value = (None, None)
            read_from_queue(origin_qc, 10, delete=True)

def test_delete_from_queue(mock_controllers):
    origin_qc = ('aws', 'QUEUE_URL')
    entries = ['entry1', 'entry2']
    with patch.dict(os.environ, {"QUEUE_URL": "https://dummy-queue-url"}):
        with patch('genai_sdk_services.queue_controller.QueueController.delete_messages'):
            delete_from_queue(origin_qc, entries)


def test_check_file(mock_controllers):
    with patch('genai_sdk_services.storage.StorageController.check_file'):
        check_file('s3://origin', 'prefix')

def test_list_files(mock_controllers):
    with patch('genai_sdk_services.storage.StorageController.list_files'):
        list_files('s3://origin', 'prefix')

def test_get_sizes(mock_controllers):
    with patch('genai_sdk_services.storage.StorageController.get_size_of_files'):
        get_sizes('s3://origin', 'prefix')

def test_download_filesss(mock_controllers):
    with patch('genai_sdk_services.storage.StorageController.download_file'):
        files = [('remote1', 'local1')]
        download_files('s3://origin', files)

def test_download_file(mock_controllers):
    with patch('genai_sdk_services.storage.StorageController.download_file'):
        files = ['remote1', 'local1']
        download_file('s3://origin', files)

def test_download_directory(mock_controllers):
    with patch('genai_sdk_services.storage.StorageController.download_directory'):
        download_directory('s3://origin', 'test_path')

def test_load_file(mock_controllers):
    with patch('genai_sdk_services.storage.StorageController.load_file'):
        load_file('s3://origin', 'test_path')

def test_upload_object(mock_controllers):
    with patch('genai_sdk_services.storage.StorageController.upload_object'):
        upload_object('s3://origin', 'test', 'test_path')

def test_upload_files():
    origin = "aws_storage"
    files = [("remote_path/file1.txt", "local_path/file1.txt")]

    with patch('genai_sdk_services.storage.StorageController.upload_file') as mock_upload:
        upload_files(origin, files)
        mock_upload.assert_called_with(origin, "local_path/file1.txt", "remote_path/file1.txt")


def test_delete_file(mock_controllers):
    with patch('genai_sdk_services.storage.StorageController.delete_files'):
        files = 'file'
        delete_file(('aws', 'bucket'), files)

def test_delete_filesss(mock_controllers):
    with patch('genai_sdk_services.storage.StorageController.delete_files'):
        files = ['file1', 'file2']
        delete_files(('aws', 'bucket'), files)

def test_delete_folder(mock_controllers):
    with (patch('genai_sdk_services.storage.StorageController.delete_files'),
          patch('genai_sdk_services.storage.StorageController.list_files') as mock_list_files):
        files = ['file1', 'file2']
        mock_list_files.return_value = files
        delete_folder(('aws', 'bucket'), 'test_folder')

def test_delete_folder_except(mock_controllers):
    with (patch('genai_sdk_services.storage.StorageController.delete_files') as mock_delete_files,
          patch('genai_sdk_services.storage.StorageController.list_files') as mock_list_files):
        files = ['file1', 'file2']
        mock_list_files.return_value = files
        mock_delete_files.side_effect = Exception
        with pytest.raises(Exception):
            delete_folder(('aws', 'bucket'), 'test_folder')

def test_get_mimetype():
    filename = "file.pdf"
    with patch('genai_sdk_services.files.FilesController.get_type', return_value="application/pdf") as mock_get_type:
        result = get_mimetype(filename)
        assert result == "application/pdf"
        mock_get_type.assert_called_with(filename)

def test_get_number_pages(mock_controllers):
    with patch('genai_sdk_services.files.FilesController.get_number_pages', return_value="application/pdf"):
        get_number_pages('file.pdf')

def test_get_text_from_file(mock_controllers):
    with patch('genai_sdk_services.files.FilesController.get_text', return_value=[None, None, None, None]):
        get_texts_from_file('file.pdf', "", 0, 1, False, False)

def test_get_images_from_file(mock_controllers):
    with patch('genai_sdk_services.files.FilesController.extract_images', return_value=[None, None, None, None]):
        get_images_from_file("file.pdf", 0,0)

def test_extract_ocr_files():
    files = ["file1"]
    origin = "ocr_origin"
    with patch('genai_sdk_services.files.FilesController.extract_multiple_text', return_value=([], [], [], [], [], [])) as mock_extract:
        result = extract_ocr_files(files, origin, False, False, False)
        assert result == ([], [], [], [], [], [])
        mock_extract.assert_called_with(files, ocr_origin=origin, do_cells=False, extract_tables=False, do_lines=False, bytes_mode=False)


def test_get_dataset():
    origin = "aws"
    dataset_type = "csv"
    path_name = "dummy_path"

    with patch('genai_sdk_services.data_bunch.DataBunchController.get_dataset', return_value=pandas.DataFrame()) as mock_get:
        result = get_dataset(origin, dataset_type, path_name)
        assert isinstance(result, pandas.DataFrame)
        mock_get.assert_called_with(dataset_type, origin, path_name=path_name)


def test_select_athena():
    origin = "aws"

    with patch('genai_sdk_services.db.DBController.select'):
        select_athena(origin, "athena_txt", "name")

def test_create_athena():
    origin = "aws"

    with patch('genai_sdk_services.db.DBController.create'):
        create_athena(origin, "athena_txt", "name", n_classes=1, n_entities=1, n_metadata=1, table_type="tabletype")

def test_partition_athena():
    origin = "aws"

    with patch('genai_sdk_services.db.DBController.partition'):
        partition_athena(origin, "athena_txt", "name")

def test_execute_athena():
    origin = "aws"

    with patch('genai_sdk_services.db.DBController.execute_query'):
        execute_query_athena(origin, "athena_txt", "name")

def test_get_query_athena():
    origin = "aws"

    with patch('genai_sdk_services.db.DBController.get_query_select'):
        get_query_athena(origin, ["athena_txt"], [], [])

def test_delete_athena():
    origin = "aws"

    with patch('genai_sdk_services.db.DBController.delete'):
        delete_athena(origin, "athena_txt", "name")

def test_set_storage():
    with (patch('genai_sdk_services.storage.StorageController.set_credentials'),
          patch('genai_sdk_services.data_bunch.DataBunchController.set_credentials')):
        set_storage({"test": "origin_test"})
