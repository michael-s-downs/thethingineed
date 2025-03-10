import pytest
import unittest
from unittest.mock import MagicMock, patch, mock_open

import main
from main import PreprocessOCRDeployment
from unittest import mock
import os


@pytest.fixture
def preprocess_deployment():
    with patch("main.set_queue"):
        return PreprocessOCRDeployment()

@pytest.fixture(autouse=True)
def mock_aws_credentials(monkeypatch):
    monkeypatch.setenv('AWS_ACCESS_KEY', 'mock_access_key')
    monkeypatch.setenv('AWS_SECRET_KEY', 'mock_secret_key')
    monkeypatch.setenv('AWS_REGION_NAME', 'mock_region_name')

def test_max_num_queue(preprocess_deployment):
    assert preprocess_deployment.max_num_queue == 1

def test_process_success(mocker, preprocess_deployment):
    mocker.patch('main.get_generic', return_value={
        'project_conf': {
            'department': 'DEPARTMENT_NAME',
            'extract_tables': False,
        }
    })
    mocker.patch('main.get_specific', return_value={
          'paths': {
            'images': [{'filename': 'image1.jpeg', 'number': 0}, {'filename': 'image2.jpeg', 'number': 1}],
            'text': 'path_to_text',
            'cells': 'path_to_cells',
            'tables': 'path_to_tables',
            'txt': 'path_to_txt'
        },
        'path_img': 'path_to_image',
        'path_cells': 'path_to_cells',
        'path_text': 'path_to_text',
        'path_txt': 'path_to_txt',
        'path_tables': 'path_to_tables'
    })
    mocker.patch('main.get_project_config', return_value={
        'process_id': '123',
        'process_type': 'test',
        'report_url': 'http://test.url',
        'department': 'test',
        'tenant': 'test'
    })
    mocker.patch('main.get_document', return_value={'n_pags': 5, 'filename': 'test_file.pdf', 'language': 'en'})
    mocker.patch('main.get_metadata_conf', return_value={})
    mocker.patch('main.get_ocr_config', return_value={
        'files_size': 10,
        'batch_length': 5,
        'calls_per_minute': 10,
        'ocr': 'test-ocr',
        'extract_tables': False
    })
    mocker.patch('main.get_do_cells_ocr')
    mocker.patch('main.get_do_lines_ocr')
    mocker.patch('main.resize_image', return_value=(None, True))
    mocker.patch('main.upload_files')
    mocker.patch('main.get_image_size', return_value=10.0)
    mocker.patch('main.get_ocr_files', return_value={'text': ['example text']})
    mocker.patch('main.chunk', return_value=[
        [{'filename': 'image1.jpeg', 'size': 5}, {'filename': 'image2.jpeg', 'size': 10}]
    ])
    mocker.patch('main.insert_at_rate', return_value=['rate'])
    mocker.patch.object(preprocess_deployment, 'merge_files_text', return_value=None)
    mocker.patch('os.makedirs', return_value=None)
    m = mocker.patch('builtins.open', mock_open())
    mocker.patch('main.format_indexing_metadata', side_effect=lambda text, filename, num_pags, metadata: text)
    mocker.patch('main.upload_files')
    mocker.patch('main.get_language', return_value='en')
    mocker.patch('main.remove_local_files')
    mocker.patch('main.update_status')
    mocker.patch('main.download_files')

    preprocess_deployment = PreprocessOCRDeployment()

    json_input = {'input': 'data'}  # Datos de entrada de prueba

    must_continue, message, next_service = preprocess_deployment.process(json_input)


    assert must_continue is True
    assert message == json_input
    assert next_service == 'preprocess_end'


def test_process_llm_ocr_error_extracting(mocker, preprocess_deployment):
    mocker.patch('main.get_generic', return_value={
        'project_conf': {
            'department': 'DEPARTMENT_NAME',
            'extract_tables': False,
        }
    })
    mocker.patch('main.get_specific', return_value={
          'paths': {
            'images': [{'filename': 'image1.jpeg', 'number': 0}, {'filename': 'image2.jpeg', 'number': 1}],
            'text': 'path_to_text',
            'cells': 'path_to_cells',
            'tables': 'path_to_tables',
            'txt': 'path_to_txt'
        },
        'path_img': 'path_to_image',
        'path_cells': 'path_to_cells',
        'path_text': 'path_to_text',
        'path_txt': 'path_to_txt',
        'path_tables': 'path_to_tables'
    })
    mocker.patch('main.get_project_config', return_value={
        'process_id': '123',
        'process_type': 'test',
        'report_url': 'http://test.url',
        'department': 'test',
        'tenant': 'test'
    })
    mocker.patch('main.get_document', return_value={'n_pags': 5, 'filename': 'test_file.pdf', 'language': 'en'})
    mocker.patch('main.get_metadata_conf', return_value={})
    mocker.patch('main.get_ocr_config', return_value={
        'files_size': 10,
        'batch_length': 5,
        'calls_per_minute': 10,
        'ocr': 'llm-ocr',
        'extract_tables': False,
        'llm_ocr_conf': {
            "model": "test",
            "platform": "test",  
            "query": "test:",
            "system": "test",
            "max_tokens": 1000
        }
    })
    mocker.patch('main.get_do_cells_ocr')
    mocker.patch('main.get_do_lines_ocr')
    mocker.patch('main.resize_image', return_value=(None, True))
    mocker.patch('main.upload_files')
    mocker.patch('main.get_image_size', return_value=10.0)
    mocker.patch('main.get_ocr_files', side_effect=Exception)
    mocker.patch('main.chunk', return_value=[
        [{'filename': 'image1.jpeg', 'size': 5}, {'filename': 'image2.jpeg', 'size': 10}]
    ])
    mocker.patch('main.remove_local_files', side_effect=Exception)
    mocker.patch('main.update_status')

    preprocess_deployment = PreprocessOCRDeployment()

    json_input = {'input': 'data'}  # Datos de entrada de prueba


    must_continue, message, next_service = preprocess_deployment.process(json_input)


    assert must_continue is True
    assert message == json_input
    assert next_service == 'preprocess_end'


def test_merge_files_text(mocker, preprocess_deployment):
    upload_docs = {
        'text': [
            ('dummy_page1.txt', 'dummy_page1.txt'),
            ('dummy_page2.txt', 'dummy_page2.txt')
        ]
    }
    path_file_txt = 'output.txt'
    path_file_text = 'output_text.txt'
    filename = 'dummy_file'
    num_pags = 2
    metadata = {'key': 'value'}


    mocker.patch('main.format_indexing_metadata', return_value='formatted_metadata')

    m = mock_open(read_data='Contenido de la página')
    mocker.patch('builtins.open', m)

    mocker.patch('os.makedirs',return_value=None)

    preprocess_deployment.merge_files_text(upload_docs, path_file_txt, path_file_text, filename, num_pags, metadata)


    m.assert_any_call('dummy_page1.txt', 'r', encoding='utf-8')
    m.assert_any_call('dummy_page2.txt', 'r', encoding='utf-8')


    m.assert_any_call(path_file_text, 'a', encoding='utf-8')
    m.assert_any_call(path_file_txt, 'a', encoding='utf-8')

    handle = m()
    handle.write.assert_any_call('formatted_metadata')

def test_generic_error(mocker, preprocess_deployment):
    mocker.patch('main.get_generic', side_effect=KeyError)
    mocker.patch('main.get_dataset_status_key', return_value={'mock_dataset_status_key': 'id_089'})
    mocker.patch('main.update_status')

    json_input_invalid = {'key': 'value'}

    must_continue, message, next_service = preprocess_deployment.process(json_input_invalid)
    assert must_continue is True
    assert message == json_input_invalid
    assert next_service == 'preprocess_end'

def test_get_project_config_error(mocker, preprocess_deployment):
    mocker.patch('main.get_generic', return_value={
        # No incluido 'process_id'
        'process_type': 'test',
        'report_url': 'http://test.url'
    })
    mocker.patch('main.get_specific')
    mock.patch('main.get_project_config', side_effect=KeyError)

    mocker.patch('main.get_dataset_status_key', return_value={'mock_dataset_status_key': 'id_089'})
    mocker.patch('main.update_status')

    json_input_invalid = {'key': 'value'}

    must_continue, message, next_service = preprocess_deployment.process(json_input_invalid)
    assert must_continue is True
    assert message == json_input_invalid
    assert next_service == 'preprocess_end'


def test_get_documents_error(mocker, preprocess_deployment):
    mocker.patch('main.get_generic', return_value={
        'project_conf': {
            'department': 'DEPARTMENT_NAME',
            'extract_tables': False,
        }
    })
    mocker.patch('main.get_specific', return_value={
        'paths': {
            'images': [{'filename': 'image1.jpeg', 'number': 0}, {'filename': 'image2.jpeg', 'number': 1}],
            'text': 'path_to_text',
            'cells': 'path_to_cells',
            'tables': 'path_to_tables',
            'txt': 'path_to_txt'
        },
        'path_img': 'path_to_image',
        'path_cells': 'path_to_cells',
        'path_text': 'path_to_text',
        'path_txt': 'path_to_txt',
        'path_tables': 'path_to_tables'
    })
    mocker.patch('main.get_project_config', return_value={
        'process_id': '123',
        'process_type': 'test',
        'report_url': 'http://test.url',
        'department': 'test',
        'tenant': 'test'
    })
    mocker.patch('main.get_dataset_status_key', return_value={'mock_dataset_status_key': 'id_089'})
    mocker.patch('main.update_status')

    json_input_invalid = {'key': 'value'}

    must_continue, message, next_service = preprocess_deployment.process(json_input_invalid)
    assert must_continue is True
    assert message == json_input_invalid
    assert next_service == 'preprocess_end'

def test_storage_containers_error(mocker, preprocess_deployment):
    mocker.patch('main.get_generic', return_value={
        'project_conf': {
            'department': 'DEPARTMENT_NAME',
            'extract_tables': False,
        }
    })
    mocker.patch('main.get_specific', return_value={
        'paths': {
            'images': [{'filename': 'image1.jpeg', 'number': 0}, {'filename': 'image2.jpeg', 'number': 1}],
            'text': 'path_to_text',
            'cells': 'path_to_cells',
            'tables': 'path_to_tables',
            'txt': 'path_to_txt'
        },
        'path_img': 'path_to_image',
        'path_cells': 'path_to_cells',
        'path_text': 'path_to_text',
        'path_txt': 'path_to_txt',
        'path_tables': 'path_to_tables'
    })
    mocker.patch('main.get_project_config', return_value={
        'process_id': '123',
        'process_type': 'test',
        'report_url': 'http://test.url',
        'department': 'test',
        'tenant': 'test'
    })
    mocker.patch('main.storage_containers', {'wrong_key': 'wrong_value'})
    mocker.patch('main.get_dataset_status_key', return_value={'mock_dataset_status_key': 'id_089'})
    mocker.patch('main.update_status')

    json_input_invalid = {'key': 'value'}

    must_continue, message, next_service = preprocess_deployment.process(json_input_invalid)
    assert must_continue is True
    assert message == json_input_invalid
    assert next_service == 'preprocess_end'

def test_ocr_conf_error(mocker, preprocess_deployment):
    mocker.patch('main.get_generic', return_value={
        'project_conf': {
            'department': 'DEPARTMENT_NAME',
            'extract_tables': False,
        }
    })
    mocker.patch('main.get_specific', return_value={
          'paths': {
            'images': [{'filename': 'image1.jpeg', 'number': 0}, {'filename': 'image2.jpeg', 'number': 1}],
            'text': 'path_to_text',
            'cells': 'path_to_cells',
            'tables': 'path_to_tables',
            'txt': 'path_to_txt'
        },
        'path_img': 'path_to_image',
        'path_cells': 'path_to_cells',
        'path_text': 'path_to_text',
        'path_txt': 'path_to_txt',
        'path_tables': 'path_to_tables'
    })
    mocker.patch('main.get_project_config', return_value={
        'process_id': '123',
        'process_type': 'test',
        'report_url': 'http://test.url',
        'department': 'test',
        'tenant': 'test'
    })
    mocker.patch('main.get_document', return_value={'n_pags': 5, 'filename': 'test_file.pdf', 'language': 'en'})
    mocker.patch('main.get_metadata_conf', return_value={})
    mocker.patch('main.get_ocr_config', return_value={'another_key': 'value'})

    mocker.patch('main.get_dataset_status_key', return_value={'mock_dataset_status_key': 'id_089'})
    mocker.patch('main.update_status')

    json_input_invalid = {'key': 'value'}

    must_continue, message, next_service = preprocess_deployment.process(json_input_invalid)
    assert must_continue is True
    assert message == json_input_invalid
    assert next_service == 'preprocess_end'

def test_path_images_error(mocker, preprocess_deployment):
    mocker.patch('main.get_generic', return_value={
        'project_conf': {
            'department': 'DEPARTMENT_NAME',
            'extract_tables': False,
        }
    })
    mocker.patch('main.get_specific', return_value={
        #No paths parameter given
        'path_img': 'path_to_image',
        'path_cells': 'path_to_cells',
        'path_text': 'path_to_text',
        'path_txt': 'path_to_txt',
        'path_tables': 'path_to_tables'
    })
    mocker.patch('main.get_project_config', return_value={
        'process_id': '123',
        'process_type': 'test',
        'report_url': 'http://test.url',
        'department': 'test',
        'tenant': 'test'
    })
    mocker.patch('main.get_document', return_value={'n_pags': 5, 'filename': 'test_file.pdf', 'language': 'en'})
    mocker.patch('main.get_metadata_conf', return_value={})
    mocker.patch('main.get_ocr_config', return_value={
        'files_size': 10,
        'batch_length': 5,
        'calls_per_minute': 10,
        'ocr': 'test-ocr',
        'extract_tables': False
    })

    mocker.patch('main.get_dataset_status_key', return_value={'mock_dataset_status_key': 'id_089'})
    mocker.patch('main.update_status')

    json_input_invalid = {'key': 'value'}

    must_continue, message, next_service = preprocess_deployment.process(json_input_invalid)
    assert must_continue is True
    assert message == json_input_invalid
    assert next_service == 'preprocess_end'

def test_getting_lines_and_cells_error(mocker, preprocess_deployment):
    mocker.patch('main.get_generic', return_value={
        'wrong_key': 'wrong_value'
    })
    mocker.patch('main.get_specific', return_value={
        'paths': {
            'images': [{'filename': 'image1.jpeg', 'number': 0}, {'filename': 'image2.jpeg', 'number': 1}],
            'text': 'path_to_text',
            'cells': 'path_to_cells',
            'tables': 'path_to_tables',
            'txt': 'path_to_txt'
        },
        'path_img': 'path_to_image',
        'path_cells': 'path_to_cells',
        'path_text': 'path_to_text',
        'path_txt': 'path_to_txt',
        'path_tables': 'path_to_tables'
    })
    mocker.patch('main.get_project_config', return_value={
        'process_id': '123',
        'process_type': 'test',
        'report_url': 'http://test.url',
        'department': 'test',
        'tenant': 'test'
    })
    mocker.patch('main.get_document', return_value={'n_pags': 5, 'filename': 'test_file.pdf', 'language': 'en'})
    mocker.patch('main.get_metadata_conf', return_value={})
    mocker.patch('main.get_ocr_config', return_value={
        'files_size': 10,
        'batch_length': 5,
        'calls_per_minute': 10,
        'ocr': 'test-ocr',
        'extract_tables': False
    })
    mocker.patch('main.get_dataset_status_key', return_value={'mock_dataset_status_key': 'id_089'})
    mocker.patch('main.update_status')

    json_input_invalid = {'key': 'value'}

    must_continue, message, next_service = preprocess_deployment.process(json_input_invalid)
    assert must_continue is True
    assert message == json_input_invalid
    assert next_service == 'preprocess_end'

def test_resize_image_error(mocker, preprocess_deployment):

    mocker.patch('main.get_generic', return_value={
        'project_conf': {
            'department': 'DEPARTMENT_NAME',
            'extract_tables': False,
        }
    })
    mocker.patch('main.get_specific', return_value={
          'paths': {
            'images': [{'filename': 'image1.jpeg', 'number': 0}, {'filename': 'image2.jpeg', 'number': 1}],
            'text': 'path_to_text',
            'cells': 'path_to_cells',
            'tables': 'path_to_tables',
            'txt': 'path_to_txt'
        },
        'path_img': 'path_to_image',
        'path_cells': 'path_to_cells',
        'path_text': 'path_to_text',
        'path_txt': 'path_to_txt',
        'path_tables': 'path_to_tables'
    })
    mocker.patch('main.get_project_config', return_value={
        'process_id': '123',
        'process_type': 'test',
        'report_url': 'http://test.url',
        'department': 'test',
        'tenant': 'test'
    })
    mocker.patch('main.get_document', return_value={'n_pags': 5, 'filename': 'test_file.pdf', 'language': 'en'})
    mocker.patch('main.get_metadata_conf', return_value={})
    mocker.patch('main.get_ocr_config', return_value={
        'files_size': 10,
        'batch_length': 5,
        'calls_per_minute': 10,
        'ocr': 'test-ocr',
        'extract_tables': False
    })
    mocker.patch('main.get_do_cells_ocr')
    mocker.patch('main.get_do_lines_ocr')
    mocker.patch('main.resize_image', side_effect=Exception)

    mocker.patch('main.get_dataset_status_key', return_value={'mock_dataset_status_key': 'id_089'})
    mocker.patch('main.update_status')

    json_input_invalid = {'key': 'value'}

    must_continue, message, next_service = preprocess_deployment.process(json_input_invalid)
    assert must_continue is True
    assert message == json_input_invalid
    assert next_service == 'preprocess_end'

def test_get_image_size_error(mocker, preprocess_deployment):
    mocker.patch('main.get_generic', return_value={
        'project_conf': {
            'department': 'DEPARTMENT_NAME',
            'extract_tables': False,
        }
    })
    mocker.patch('main.get_specific', return_value={
        'paths': {
            'images': [{'filename': 'image1.jpeg', 'number': 0}, {'filename': 'image2.jpeg', 'number': 1}],
            'text': 'path_to_text',
            'cells': 'path_to_cells',
            'tables': 'path_to_tables',
            'txt': 'path_to_txt'
        },
        'path_img': 'path_to_image',
        'path_cells': 'path_to_cells',
        'path_text': 'path_to_text',
        'path_txt': 'path_to_txt',
        'path_tables': 'path_to_tables'
    })
    mocker.patch('main.get_project_config', return_value={
        'process_id': '123',
        'process_type': 'test',
        'report_url': 'http://test.url',
        'department': 'test',
        'tenant': 'test'
    })
    mocker.patch('main.get_document', return_value={'n_pags': 5, 'filename': 'test_file.pdf', 'language': 'en'})
    mocker.patch('main.get_metadata_conf', return_value={})
    mocker.patch('main.get_ocr_config', return_value={
        'files_size': 10,
        'batch_length': 5,
        'calls_per_minute': 10,
        'ocr': 'test-ocr',
        'extract_tables': False
    })
    mocker.patch('main.get_do_cells_ocr')
    mocker.patch('main.get_do_lines_ocr')
    mocker.patch('main.resize_image', return_value=(None, False))
    mocker.patch('main.get_image_size',side_effect=Exception)
    mocker.patch('main.get_dataset_status_key', return_value={'mock_dataset_status_key': 'id_089'})
    mocker.patch('main.update_status')

    json_input_invalid = {'key': 'value'}

    must_continue, message, next_service = preprocess_deployment.process(json_input_invalid)
    assert must_continue is True
    assert message == json_input_invalid
    assert next_service == 'preprocess_end'

def test_extracting_text_ocr_error(mocker, preprocess_deployment):
    mocker.patch('main.get_generic', return_value={
        'project_conf': {
            'department': 'DEPARTMENT_NAME',
            'extract_tables': False,
        }
    })
    mocker.patch('main.get_specific', return_value={
          'paths': {
            'images': [{'filename': 'image1.jpeg', 'number': 0}, {'filename': 'image2.jpeg', 'number': 1}],
            'text': 'path_to_text',
            'cells': 'path_to_cells',
            'tables': 'path_to_tables',
            'txt': 'path_to_txt'
        },
        'path_img': 'path_to_image',
        'path_cells': 'path_to_cells',
        'path_text': 'path_to_text',
        'path_txt': 'path_to_txt',
        'path_tables': 'path_to_tables'
    })
    mocker.patch('main.get_project_config', return_value={
        'process_id': '123',
        'process_type': 'test',
        'report_url': 'http://test.url',
        'department': 'test',
        'tenant': 'test'
    })
    mocker.patch('main.get_document', return_value={'n_pags': 5, 'filename': 'test_file.pdf', 'language': 'en'})
    mocker.patch('main.get_metadata_conf', return_value={})
    mocker.patch('main.get_ocr_config', return_value={
        'files_size': 10,
        'batch_length': 5,
        'calls_per_minute': 10,
        'ocr': 'test-ocr',
        'extract_tables': False
    })
    mocker.patch('main.get_do_cells_ocr')
    mocker.patch('main.get_do_lines_ocr')
    mocker.patch('main.resize_image', return_value=(None, False))
    mocker.patch('main.get_image_size', return_value=10.0)
    mocker.patch('main.get_ocr_files', return_value={'text': ['example text']})
    mocker.patch('main.chunk', side_effect=Exception)

    mocker.patch('main.get_dataset_status_key', return_value={'mock_dataset_status_key': 'id_089'})
    mocker.patch('main.update_status')

    json_input_invalid = {'key': 'value'}

    must_continue, message, next_service = preprocess_deployment.process(json_input_invalid)
    assert must_continue is True
    assert message == json_input_invalid
    assert next_service == 'preprocess_end'

def test_repor_ocr_pages(mocker, preprocess_deployment):
    mocker.patch('main.get_generic', return_value={
        'project_conf': {
            'department': 'DEPARTMENT_NAME',
            'extract_tables': False,
        }
    })
    mocker.patch('main.get_specific', return_value={
          'paths': {
            'images': [{'filename': 'image1.jpeg', 'number': 0}, {'filename': 'image2.jpeg', 'number': 1}],
            'text': 'path_to_text',
            'cells': 'path_to_cells',
            'tables': 'path_to_tables',
            'txt': 'path_to_txt'
        },
        'path_img': 'path_to_image',
        'path_cells': 'path_to_cells',
        'path_text': 'path_to_text',
        'path_txt': 'path_to_txt',
        'path_tables': 'path_to_tables'
    })
    mocker.patch('main.get_project_config', return_value={
        'process_id': '123',
        'process_type': 'test',
        'report_url': 'http://test.url',
        'department': 'test',
        'tenant': 'test'
    })
    mocker.patch('main.get_document', return_value={'n_pags': 5, 'filename': 'test_file.pdf', 'language': 'en'})
    mocker.patch('main.get_metadata_conf', return_value={})
    mocker.patch('main.get_ocr_config', return_value={
        'files_size': 10,
        'batch_length': 5,
        'calls_per_minute': 10,
        'ocr': 'test-ocr',
        'extract_tables': False
    })
    mocker.patch('main.get_do_cells_ocr')
    mocker.patch('main.get_do_lines_ocr')
    mocker.patch('main.resize_image', return_value=(None, False))
    mocker.patch('main.get_image_size', return_value=10.0)
    mocker.patch('main.get_ocr_files', return_value={'text': ['example text']})
    mocker.patch('main.chunk', return_value=[
        [{'filename': 'image1.jpeg', 'size': 5}, {'filename': 'image2.jpeg', 'size': 10}]
    ])
    mocker.patch('main.insert_at_rate', return_value=['rate'])
    mocker.patch.object(preprocess_deployment, 'merge_files_text', return_value=None)
    mocker.patch('os.makedirs', return_value=None)
    m = mocker.patch('builtins.open', mock_open())
    mocker.patch.object(preprocess_deployment, 'report_api', side_effect=Exception)

    mocker.patch('main.get_dataset_status_key', return_value={'mock_dataset_status_key': 'id_089'})
    mocker.patch('main.update_status')

    json_input_invalid = {'key': 'value'}

    must_continue, message, next_service = preprocess_deployment.process(json_input_invalid)
    assert must_continue is True
    assert message == json_input_invalid
    assert next_service == 'preprocess_end'

def test_merge_files_exception(mocker, preprocess_deployment):
    mocker.patch('main.get_generic', return_value={
        'project_conf': {
            'department': 'DEPARTMENT_NAME',
            'extract_tables': False,
        }
    })
    mocker.patch('main.get_specific', return_value={
          'paths': {
            'images': [{'filename': 'image1.jpeg', 'number': 0}, {'filename': 'image2.jpeg', 'number': 1}],
            'text': 'path_to_text',
            'cells': 'path_to_cells',
            'tables': 'path_to_tables',
            'txt': 'path_to_txt'
        },
        'path_img': 'path_to_image',
        'path_cells': 'path_to_cells',
        'path_text': 'path_to_text',
        'path_txt': 'path_to_txt',
        'path_tables': 'path_to_tables'
    })
    mocker.patch('main.get_project_config', return_value={
        'process_id': '123',
        'process_type': 'test',
        'report_url': 'http://test.url',
        'department': 'test',
        'tenant': 'test'
    })
    mocker.patch('main.get_document', return_value={'n_pags': 5, 'filename': 'test_file.pdf', 'language': 'en'})
    mocker.patch('main.get_metadata_conf', return_value={})
    mocker.patch('main.get_ocr_config', return_value={
        'files_size': 10,
        'batch_length': 5,
        'calls_per_minute': 10,
        'ocr': 'test-ocr',
        'extract_tables': False
    })
    mocker.patch('main.get_do_cells_ocr')
    mocker.patch('main.get_do_lines_ocr')
    mocker.patch('main.resize_image', return_value=(None, False))
    mocker.patch('main.get_image_size', return_value=10.0)
    mocker.patch('main.get_ocr_files', return_value={'text': ['example text']})
    mocker.patch('main.chunk', return_value=[
        [{'filename': 'image1.jpeg', 'size': 5}, {'filename': 'image2.jpeg', 'size': 10}]
    ])
    mocker.patch('main.insert_at_rate', return_value=['rate'])
    mocker.patch.object(preprocess_deployment, 'merge_files_text', side_effect=Exception)

    mocker.patch('main.get_dataset_status_key', return_value={'mock_dataset_status_key': 'id_089'})
    mocker.patch('main.update_status')

    json_input_invalid = {'key': 'value'}

    must_continue, message, next_service = preprocess_deployment.process(json_input_invalid)
    assert must_continue is True
    assert message == json_input_invalid
    assert next_service == 'preprocess_end'

def test_upoading_files_error(mocker, preprocess_deployment):
    mocker.patch('main.get_generic', return_value={
        'project_conf': {
            'department': 'DEPARTMENT_NAME',
            'extract_tables': False,
        }
    })
    mocker.patch('main.get_specific', return_value={
        'paths': {
            'images': [{'filename': 'image1.jpeg', 'number': 0}, {'filename': 'image2.jpeg', 'number': 1}],
            'text': 'path_to_text',
            'cells': 'path_to_cells',
            'tables': 'path_to_tables',
            'txt': 'path_to_txt'
        },
        'path_img': 'path_to_image',
        'path_cells': 'path_to_cells',
        'path_text': 'path_to_text',
        'path_txt': 'path_to_txt',
        'path_tables': 'path_to_tables'
    })
    mocker.patch('main.get_project_config', return_value={
        'process_id': '123',
        'process_type': 'test',
        'report_url': 'http://test.url',
        'department': 'test',
        'tenant': 'test'
    })
    mocker.patch('main.get_document', return_value={'n_pags': 5, 'filename': 'test_file.pdf', 'language': 'en'})
    mocker.patch('main.get_metadata_conf', return_value={})
    mocker.patch('main.get_ocr_config', return_value={
        'files_size': 10,
        'batch_length': 5,
        'calls_per_minute': 10,
        'ocr': 'test-ocr',
        'extract_tables': False
    })
    mocker.patch('main.get_do_cells_ocr')
    mocker.patch('main.get_do_lines_ocr')
    mocker.patch('main.resize_image', return_value=(None, False))
    mocker.patch('main.get_image_size', return_value=10.0)
    mocker.patch('main.get_ocr_files', return_value={'text': ['example text']})
    mocker.patch('main.chunk', return_value=[
        [{'filename': 'image1.jpeg', 'size': 5}, {'filename': 'image2.jpeg', 'size': 10}]
    ])
    mocker.patch('main.insert_at_rate', return_value=['rate'])
    mocker.patch.object(preprocess_deployment, 'merge_files_text', return_value=None)
    mocker.patch('os.makedirs',return_value=None)
    m = mocker.patch('builtins.open', mock_open())
    mocker.patch('main.format_indexing_metadata', side_effect=lambda text, filename, num_pags, metadata: text)
    mocker.patch('main.upload_files', side_effect=Exception)

    mocker.patch('main.get_dataset_status_key', return_value={'mock_dataset_status_key': 'id_089'})
    mocker.patch('main.update_status')

    json_input_invalid = {'key': 'value'}

    must_continue, message, next_service = preprocess_deployment.process(json_input_invalid)
    assert must_continue is True
    assert message == json_input_invalid
    assert next_service == 'preprocess_end'

def test_getting_languages_error(mocker, preprocess_deployment):
    mocker.patch('main.get_generic', return_value={
        'project_conf': {
            'department': 'DEPARTMENT_NAME',
            'extract_tables': False,
        }
    })
    mocker.patch('main.get_specific', return_value={
          'paths': {
            'images': [{'filename': 'image1.jpeg', 'number': 0}, {'filename': 'image2.jpeg', 'number': 1}],
            'text': 'path_to_text',
            'cells': 'path_to_cells',
            'tables': 'path_to_tables',
            'txt': 'path_to_txt'
        },
        'path_img': 'path_to_image',
        'path_cells': 'path_to_cells',
        'path_text': 'path_to_text',
        'path_txt': 'path_to_txt',
        'path_tables': 'path_to_tables'
    })
    mocker.patch('main.get_project_config', return_value={
        'process_id': '123',
        'process_type': 'test',
        'report_url': 'http://test.url',
        'department': 'test',
        'tenant': 'test'
    })
    mocker.patch('main.get_document', return_value={'n_pags': 5, 'filename': 'test_file.pdf', 'language': 'en'})
    mocker.patch('main.get_metadata_conf', return_value={})
    mocker.patch('main.get_ocr_config', return_value={
        'files_size': 10,
        'batch_length': 5,
        'calls_per_minute': 10,
        'ocr': 'test-ocr',
        'extract_tables': False
    })
    mocker.patch('main.get_do_cells_ocr')
    mocker.patch('main.get_do_lines_ocr')
    mocker.patch('main.resize_image', return_value=(None, False))
    mocker.patch('main.get_image_size', return_value=10.0)
    mocker.patch('main.get_ocr_files', return_value={'text': ['example text']})
    mocker.patch('main.chunk', return_value=[
        [{'filename': 'image1.jpeg', 'size': 5}, {'filename': 'image2.jpeg', 'size': 10}]
    ])
    mocker.patch('main.insert_at_rate', return_value=['rate'])
    mocker.patch.object(preprocess_deployment, 'merge_files_text', return_value=None)
    mocker.patch('os.makedirs',return_value=None)
    m = mocker.patch('builtins.open', mock_open())
    mocker.patch('main.format_indexing_metadata', side_effect=lambda text, filename, num_pags, metadata: text)
    mocker.patch('main.upload_files')
    mocker.patch('main.get_language', side_effect=Exception)

    mocker.patch('main.get_dataset_status_key', return_value={'mock_dataset_status_key': 'id_089'})
    mocker.patch('main.update_status')

    json_input_invalid = {'key': 'value'}

    must_continue, message, next_service = preprocess_deployment.process(json_input_invalid)
    assert must_continue is True
    assert message == json_input_invalid
    assert next_service == 'preprocess_end'


def test_remove_local_files_error(mocker, preprocess_deployment):

    mocker.patch('main.get_generic', return_value={
        'project_conf': {
            'department': 'DEPARTMENT_NAME',
            'extract_tables': False,
        }
    })
    mocker.patch('main.get_specific', return_value={
          'paths': {
            'images': [{'filename': 'image1.jpeg', 'number': 0}, {'filename': 'image2.jpeg', 'number': 1}],
            'text': 'path_to_text',
            'cells': 'path_to_cells',
            'tables': 'path_to_tables',
            'txt': 'path_to_txt'
        },
        'path_img': 'path_to_image',
        'path_cells': 'path_to_cells',
        'path_text': 'path_to_text',
        'path_txt': 'path_to_txt',
        'path_tables': 'path_to_tables'
    })
    mocker.patch('main.get_project_config', return_value={
        'process_id': '123',
        'process_type': 'test',
        'report_url': 'http://test.url',
        'department': 'test',
        'tenant': 'test'
    })
    mocker.patch('main.get_document', return_value={'n_pags': 5, 'filename': 'test_file.pdf', 'language': 'en'})
    mocker.patch('main.get_metadata_conf', return_value={})
    mocker.patch('main.get_ocr_config', return_value={
        'files_size': 10,
        'batch_length': 5,
        'calls_per_minute': 10,
        'ocr': 'test-ocr',
        'extract_tables': False
    })
    mocker.patch('main.get_do_cells_ocr')
    mocker.patch('main.get_do_lines_ocr')
    mocker.patch('main.resize_image', return_value=(None, False))
    mocker.patch('main.get_image_size', return_value=10.0)
    mocker.patch('main.get_ocr_files', return_value={'text': ['example text']})
    mocker.patch('main.chunk', return_value=[
        [{'filename': 'image1.jpeg', 'size': 5}, {'filename': 'image2.jpeg', 'size': 10}]
    ])
    mocker.patch('main.insert_at_rate', return_value=['rate'])
    mocker.patch.object(preprocess_deployment, 'merge_files_text', return_value=None)
    mocker.patch('os.makedirs',return_value=None)
    m = mocker.patch('builtins.open', mock_open())
    mocker.patch('main.format_indexing_metadata', side_effect=lambda text, filename, num_pags, metadata: text)
    mocker.patch('main.upload_files')
    mocker.patch('main.get_language', return_value='en')
    mocker.patch('main.remove_local_files', side_effect=Exception)

    mocker.patch('main.get_dataset_status_key', return_value={'mock_dataset_status_key': 'id_089'})
    mocker.patch('main.update_status')

    json_input_invalid = {'key': 'value'}

    must_continue, message, next_service = preprocess_deployment.process(json_input_invalid)
    assert must_continue is True
    assert message == json_input_invalid
    assert next_service == 'preprocess_end'

def test_process_image_resizing_branches(mocker, preprocess_deployment):
    common_mocks = {
        'get_generic': mocker.patch('main.get_generic', return_value={
            'project_conf': {
                'department': 'DEPARTMENT_NAME',
                'extract_tables': False,
            }
        }),
        'get_specific': mocker.patch('main.get_specific', return_value={
            'paths': {
                'images': [{'filename': 'image1.jpeg', 'number': 0}, {'filename': 'image2.jpeg', 'number': 1}],
                'text': 'path_to_text',
                'cells': 'path_to_cells',
                'tables': 'path_to_tables',
                'txt': 'path_to_txt'
            },
            'path_img': 'path_to_image',
            'path_cells': 'path_to_cells',
            'path_text': 'path_to_text',
            'path_txt': 'path_to_txt',
            'path_tables': 'path_to_tables'
        }),
        'get_project_config': mocker.patch('main.get_project_config', return_value={
            'process_id': '123',
            'process_type': 'test',
            'report_url': 'http://test.url',
            'department': 'test',
            'tenant': 'test'
        }),
        'get_document': mocker.patch('main.get_document', return_value={
            'n_pags': 5, 
            'filename': 'test_file.pdf', 
            'language': 'en'
        }),
        'get_metadata_conf': mocker.patch('main.get_metadata_conf', return_value={}),
        'get_do_cells_ocr': mocker.patch('main.get_do_cells_ocr'),
        'get_do_lines_ocr': mocker.patch('main.get_do_lines_ocr'),
        'upload_files': mocker.patch('main.upload_files'),
        'get_image_size': mocker.patch('main.get_image_size', return_value=10.0),
        'download_batch_files_async': mocker.patch('main.download_batch_files_async'),
        'upload_batch_files_async': mocker.patch('main.upload_batch_files_async')
    }
    
    for name, mock_obj in common_mocks.items():
        globals()[name] = mock_obj

    mocker.patch('main.get_ocr_files', return_value={'text': [('remote_text.txt', 'local_text.txt')]})
    mocker.patch.object(preprocess_deployment, 'merge_files_text')
    mocker.patch('os.makedirs')
    mocker.patch('builtins.open', mock_open())
    mocker.patch('main.format_indexing_metadata', side_effect=lambda text, filename, num_pags, metadata: text)
    mocker.patch('main.get_language', return_value='en')
    mocker.patch('main.remove_local_files')
    mocker.patch('main.update_status')
    mocker.patch('main.chunk', return_value=[
        [{'filename': 'image1.jpeg', 'size': 5}, {'filename': 'image2.jpeg', 'size': 10}]
    ])
    
    mock_logger = mocker.patch('main.logger')
    
    get_ocr_config_llm = mocker.patch('main.get_ocr_config', return_value={
        'files_size': 10,
        'batch_length': 5,
        'calls_per_minute': 10,
        'ocr': 'llm-ocr',
        'extract_tables': False,
        'llm_ocr_conf': {
            "model": "test",
            "platform": "test",  
            "query": "test:",
            "system": "test",
            "max_tokens": 1000
        }
    })

    mock_logger.reset_mock()
    
    json_input = {'input': 'data'}
    must_continue, message, next_service = preprocess_deployment.process(json_input)
    
    mock_logger.info.assert_any_call("Images will be resized by LLM as llm-ocr model has been selected")
    
    get_ocr_config_standard = mocker.patch('main.get_ocr_config', return_value={
        'files_size': 10,
        'batch_length': 5,
        'calls_per_minute': 10,
        'ocr': 'standard-ocr',
        'extract_tables': False
    })
    
    resize_image_mock = mocker.patch('main.resize_image', side_effect=[
        (None, True),   
        (None, False)  
    ])
    
    mock_logger.reset_mock()
    
    must_continue, message, next_service = preprocess_deployment.process(json_input)

    mock_logger.info.assert_any_call("Resizing images if is necesary.")
    
    assert resize_image_mock.call_count == 2
    
    upload_batch_files_async = common_mocks['upload_batch_files_async']
    upload_batch_files_async.assert_called()

def test_process_pag_in_local_path(mocker, preprocess_deployment):
    mocker.patch('main.get_generic', return_value={
        'project_conf': {
            'department': 'DEPARTMENT_NAME',
            'extract_tables': False,
        }
    })
    mocker.patch('main.get_specific', return_value={
        'paths': {
            'images': [{'filename': 'image1.jpeg', 'number': 0}, {'filename': 'image2.jpeg', 'number': 1}],
            'text': 'path_to_text',
            'cells': 'path_to_cells',
            'tables': 'path_to_tables',
            'txt': 'path_to_txt'
        },
        'path_img': 'path_to_image',
        'path_cells': 'path_to_cells',
        'path_text': 'path_to_text',
        'path_txt': 'path_to_txt',
        'path_tables': 'path_to_tables'
    })
    mocker.patch('main.get_project_config', return_value={
        'process_id': '123',
        'process_type': 'test',
        'report_url': 'http://test.url',
        'department': 'test',
        'tenant': 'test'
    })
    mocker.patch('main.get_document', return_value={'n_pags': 5, 'filename': 'test_file.pdf', 'language': 'en'})
    mocker.patch('main.get_metadata_conf', return_value={})
    mocker.patch('main.get_ocr_config', return_value={
        'files_size': 10,
        'batch_length': 5,
        'calls_per_minute': 10,
        'ocr': 'test-ocr',
        'extract_tables': False
    })
    mocker.patch('main.get_do_cells_ocr')
    mocker.patch('main.get_do_lines_ocr')
    mocker.patch('main.resize_image', return_value=(None, True))
    mocker.patch('main.get_image_size', return_value=10.0)
    mocker.patch('main.download_batch_files_async')

    mocker.patch('main.chunk', return_value=[
        [{'filename': 'image1.jpeg', 'size': 5}, {'filename': 'image2.jpeg', 'size': 10}]
    ])
    
    mock_get_ocr_files = mocker.patch('main.get_ocr_files', return_value={
        'text': [
            ('/remote/path/file.txt', '/local/path/file.txt'),  
            ('/remote/path/file_pag_1.txt', '/local/path/file_pag_1.txt')  
        ],
        'cells': [],
        'paragraphs': [],
        'words': [],
        'tables': [],
        'lines': [],
        'txt': []
    })
    
    upload_batch_mock = mocker.patch('main.upload_batch_files_async')
    
    mocker.patch.object(preprocess_deployment, 'merge_files_text')
    mocker.patch('os.makedirs')
    mocker.patch('builtins.open', mock_open(read_data='sample text'))
    mocker.patch('main.format_indexing_metadata', side_effect=lambda text, filename, num_pags, metadata: text)
    mocker.patch('main.get_language', return_value='en')
    mocker.patch('main.remove_local_files')
    mocker.patch('main.update_status')
    
    json_input = {'input': 'data'}
    must_continue, message, next_service = preprocess_deployment.process(json_input)
    
    upload_calls = upload_batch_mock.call_args_list

    pags_call_found = False
    for call in upload_calls:
        args, kwargs = call
        remote_dir = args[2]  
        if "pags" in remote_dir:
            pags_call_found = True
            break
    
    assert pags_call_found, "No upload call found with 'pags' in the remote directory"
    
    assert must_continue is True
    assert message == json_input
    assert next_service == 'preprocess_end'

def test_process_llm_ocr_queue_mode(mocker, preprocess_deployment):
    mocker.patch('main.get_generic', return_value={
        'project_conf': {
            'department': 'DEPARTMENT_NAME',
            'extract_tables': False,
        }
    })
    mocker.patch('main.get_specific', return_value={
        'paths': {
            'images': [{'filename': 'image1.jpeg', 'number': 0}, {'filename': 'image2.jpeg', 'number': 1}],
            'text': 'path_to_text',
            'cells': 'path_to_cells',
            'tables': 'path_to_tables',
            'txt': 'path_to_txt'
        },
        'path_img': 'path_to_image',
        'path_cells': 'path_to_cells',
        'path_text': 'path_to_text',
        'path_txt': 'path_to_txt',
        'path_tables': 'path_to_tables'
    })
    mocker.patch('main.get_project_config', return_value={
        'process_id': '123',
        'process_type': 'test',
        'report_url': 'http://test.url',
        'department': 'test',
        'tenant': 'test'
    })
    mocker.patch('main.get_document', return_value={'n_pags': 5, 'filename': 'test_file.pdf', 'language': 'en'})
    mocker.patch('main.get_metadata_conf', return_value={})
    
    mocker.patch('main.get_ocr_config', return_value={
        'files_size': 10,
        'batch_length': 5,
        'calls_per_minute': 10,
        'ocr': 'llm-ocr',
        'extract_tables': False,
        'llm_ocr_conf': {
            "model": "test",
            "platform": "test",  
            "query": "test:",
            "system": "test",
            "max_tokens": 1000
        }
    })
    
    mocker.patch('main.get_do_cells_ocr', return_value=True)
    mocker.patch('main.get_do_lines_ocr', return_value=True)
    mocker.patch('main.resize_image', return_value=(None, True))
    mocker.patch('main.get_image_size', return_value=10.0)
    mocker.patch('main.download_batch_files_async')
    mocker.patch('main.upload_batch_files_async')
    
    mocker.patch('main.LLMOCR.get_queue_mode', return_value=True)
    
    get_ocr_files_mock = mocker.patch('main.get_ocr_files')
    
    mock_extract_docs = {
        'text': [('/remote/path/file.txt', '/local/path/file.txt')],
        'cells': [('/remote/path/cell.txt', '/local/path/cell.txt')],
        'words': [],
        'tables': [],
        'lines': [],
        'paragraphs': [],
        'txt': []
    }
    get_ocr_files_mock.return_value = mock_extract_docs
    
    mocker.patch.object(preprocess_deployment, 'merge_files_text')
    mocker.patch('os.makedirs')
    mocker.patch('builtins.open', mock_open(read_data='sample text'))
    mocker.patch('main.format_indexing_metadata', side_effect=lambda text, filename, num_pags, metadata: text)
    mocker.patch('main.get_language', return_value='en')
    mocker.patch('main.remove_local_files')
    mocker.patch('main.update_status')
    
    json_input = {'input': 'data'}
    must_continue, message, next_service = preprocess_deployment.process(json_input)
    
    assert must_continue is True
    assert message == json_input
    assert next_service == 'preprocess_end'
    
    get_ocr_files_mock.assert_called_once()
    args, kwargs = get_ocr_files_mock.call_args
    
    assert len(args) >= 6  
    assert 'llm_ocr_conf' in kwargs
    assert kwargs['llm_ocr_conf']['headers']['x-department'] == 'test'
    assert kwargs['llm_ocr_conf']['language'] == 'en'
    
    preprocess_deployment.merge_files_text.assert_called_once()
    
    upload_batch_files_async = main.upload_batch_files_async
    upload_batch_files_async.assert_called()

if __name__ == '__main__':
    pytest.main()