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
    mocker.patch('main.get_sizes', return_value=[{'filename': 'image1.jpeg', 'size': 5},{'filename': 'image2.jpeg', 'size': 10}])
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

def test_get_origns_error(mocker, preprocess_deployment):
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
        'report_url': 'http://test.url'
    })

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
        'report_url': 'http://test.url'
    })
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
        'report_url': 'http://test.url'
    })
    mocker.patch('main.get_document', return_value={'n_pags': 5, 'filename': 'test_file.pdf'})
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
        'report_url': 'http://test.url'
    })
    mocker.patch('main.get_document', return_value={'n_pags': 5, 'filename': 'test_file.pdf'})
    mocker.patch('main.get_metadata_conf', return_value={})
    mocker.patch('main.get_ocr_config', return_value={
        'files_size': 10,
        'batch_length': 5,
        'calls_per_minute': 10
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
        'report_url': 'http://test.url'
    })
    mocker.patch('main.get_document', return_value={'n_pags': 5, 'filename': 'test_file.pdf'})
    mocker.patch('main.get_metadata_conf', return_value={})
    mocker.patch('main.get_ocr_config', return_value={
        'files_size': 10,
        'batch_length': 5,
        'calls_per_minute': 10
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
        'report_url': 'http://test.url'
    })
    mocker.patch('main.get_document', return_value={'n_pags': 5, 'filename': 'test_file.pdf'})
    mocker.patch('main.get_metadata_conf', return_value={})
    mocker.patch('main.get_ocr_config', return_value={
        'files_size': 10,
        'batch_length': 5,
        'calls_per_minute': 10
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

def test_get_sizes_error(mocker, preprocess_deployment):
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
        'report_url': 'http://test.url'
    })
    mocker.patch('main.get_document', return_value={'n_pags': 5, 'filename': 'test_file.pdf'})
    mocker.patch('main.get_metadata_conf', return_value={})
    mocker.patch('main.get_ocr_config', return_value={
        'files_size': 10,
        'batch_length': 5,
        'calls_per_minute': 10
    })
    mocker.patch('main.get_do_cells_ocr')
    mocker.patch('main.get_do_lines_ocr')
    mocker.patch('main.resize_image', return_value=(None, False))
    mocker.patch('main.get_sizes',side_effect=Exception)
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
        'report_url': 'http://test.url'
    })
    mocker.patch('main.get_document', return_value={'n_pags': 5, 'filename': 'test_file.pdf'})
    mocker.patch('main.get_metadata_conf', return_value={})
    mocker.patch('main.get_ocr_config', return_value={
        'files_size': 10,
        'batch_length': 5,
        'calls_per_minute': 10
    })
    mocker.patch('main.get_do_cells_ocr')
    mocker.patch('main.get_do_lines_ocr')
    mocker.patch('main.resize_image', return_value=(None, False))
    mocker.patch('main.get_sizes', return_value=[{'filename': 'image1.jpeg', 'size': 5},{'filename': 'image2.jpeg', 'size': 10}])
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
        'report_url': 'http://test.url'
    })
    mocker.patch('main.get_document', return_value={'n_pags': 5, 'filename': 'test_file.pdf'})
    mocker.patch('main.get_metadata_conf', return_value={})
    mocker.patch('main.get_ocr_config', return_value={
        'files_size': 10,
        'batch_length': 5,
        'calls_per_minute': 10
    })
    mocker.patch('main.get_do_cells_ocr')
    mocker.patch('main.get_do_lines_ocr')
    mocker.patch('main.resize_image', return_value=(None, False))
    mocker.patch('main.get_sizes', return_value=[{'filename': 'image1.jpeg', 'size': 5},{'filename': 'image2.jpeg', 'size': 10}])
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
        'report_url': 'http://test.url'
    })
    mocker.patch('main.get_document', return_value={'n_pags': 5, 'filename': 'test_file.pdf'})
    mocker.patch('main.get_metadata_conf', return_value={})
    mocker.patch('main.get_ocr_config', return_value={
        'files_size': 10,
        'batch_length': 5,
        'calls_per_minute': 10
    })
    mocker.patch('main.get_do_cells_ocr')
    mocker.patch('main.get_do_lines_ocr')
    mocker.patch('main.resize_image', return_value=(None, False))
    mocker.patch('main.get_sizes', return_value=[{'filename': 'image1.jpeg', 'size': 5},{'filename': 'image2.jpeg', 'size': 10}])
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
        'report_url': 'http://test.url'
    })
    mocker.patch('main.get_document', return_value={'n_pags': 5, 'filename': 'test_file.pdf'})
    mocker.patch('main.get_metadata_conf', return_value={})
    mocker.patch('main.get_ocr_config', return_value={
        'files_size': 10,
        'batch_length': 5,
        'calls_per_minute': 10
    })
    mocker.patch('main.get_do_cells_ocr')
    mocker.patch('main.get_do_lines_ocr')
    mocker.patch('main.resize_image', return_value=(None, False))
    mocker.patch('main.get_sizes',
                 return_value=[{'filename': 'image1.jpeg', 'size': 5}, {'filename': 'image2.jpeg', 'size': 10}])
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
        'report_url': 'http://test.url'
    })
    mocker.patch('main.get_document', return_value={'n_pags': 5, 'filename': 'test_file.pdf'})
    mocker.patch('main.get_metadata_conf', return_value={})
    mocker.patch('main.get_ocr_config', return_value={
        'files_size': 10,
        'batch_length': 5,
        'calls_per_minute': 10
    })
    mocker.patch('main.get_do_cells_ocr')
    mocker.patch('main.get_do_lines_ocr')
    mocker.patch('main.resize_image', return_value=(None, False))
    mocker.patch('main.get_sizes', return_value=[{'filename': 'image1.jpeg', 'size': 5},{'filename': 'image2.jpeg', 'size': 10}])
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
        'report_url': 'http://test.url'
    })
    mocker.patch('main.get_document', return_value={'n_pags': 5, 'filename': 'test_file.pdf'})
    mocker.patch('main.get_metadata_conf', return_value={})
    mocker.patch('main.get_ocr_config', return_value={
        'files_size': 10,
        'batch_length': 5,
        'calls_per_minute': 10
    })
    mocker.patch('main.get_do_cells_ocr')
    mocker.patch('main.get_do_lines_ocr')
    mocker.patch('main.resize_image', return_value=(None, False))
    mocker.patch('main.get_sizes', return_value=[{'filename': 'image1.jpeg', 'size': 5},{'filename': 'image2.jpeg', 'size': 10}])
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



if __name__ == '__main__':
    pytest.main()