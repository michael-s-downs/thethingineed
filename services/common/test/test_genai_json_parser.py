### This code is property of the GGAO ###

import unittest
from unittest.mock import patch
from datetime import datetime
import os

from typing import Dict, Union, List, Optional

from genai_json_parser import (
    get_credentials, get_generic, get_specific, get_department, get_document,
    get_dataset_keys, get_exc_info, get_dataset_status_key,
    generate_dataset_status_key, get_headers, get_dataset_counter_key,
    get_dataset_config, get_dataset_id, get_ocr_config, get_project_type, get_force_ocr, get_languages, get_train_conf,
    get_models_config, select_model, get_model_parameters, get_index_conf,get_metadata_conf, get_compose_conf, get_elastic_params, get_layout_conf,
    get_do_cells_text, get_do_lines_text, get_do_cells_ocr, get_do_lines_ocr, get_do_tables, get_do_titles, get_prediction_multilabel,
    get_do_lines_conf, get_tables_conf, get_segmentation_conf, get_do_segments, get_segmenters
)

GenaiInput = Dict[str, Union[Dict, str, List]]

class TestGenaiConfig(unittest.TestCase):

    def setUp(self):
        """Setup test input data"""
        self.json_input: GenaiInput = {
            'credentials': {'user': {'name': 'test_user', 'password': 'test_pass'}},
            'generic': {
                'generic': {
                    'dataset_conf': {'generic':"", 'dataset_id': '1234', 'other_param': 'value'},
                    'generic':"", 'dataset_id': '1234', 'other_param': 'value'},
                'dataset_conf': {'generic':"", 'dataset_id': '1234', 'other_param': 'value'},
                'process_type': 'test_process',
                'project_conf': {
                    'project_type': 'text',
                    'languages': ['en', 'fr']
                },
                'train_conf': 
                    {
                        'model': {'type': 'genai', 'hyperparams': {'lr': 0.01}},
                        'models': [
                            {
                                'model_id': 'model1',
                                'model_type': 'genai',
                                'hyperparams': {'lr': 0.01}
                            }
                        ]
                    }
                ,
                'indexation_conf': {'index_param': 'index_value'},
                'compose_conf': {'compose_key': 'compose_value'},
                'elastic_params': {'elastic_key': 'elastic_value'},
                'predict_conf': {'param1': 'value1'},
                'preprocess_conf': {
                    'parameters_pretext': "", 
                    'layout_conf': {
                        'param1': 'layout_value'
                    },
                    'do_cells_text': True,
                    'do_lines_text': False,
                    'do_cells_ocr': True,
                    'do_lines_ocr': False,
                    'do_tables': True,
                    'do_titles': True,
                    'segmentation_conf': {
                        'do_segments': True,
                        'segmenters': ['seg1', 'seg2']
                    },
                    'ocr_conf': {
                        'force_ocr': True,
                        'param1': 10
                    },
                    'predict_conf': {'param1': 'value1'}
                }
            },
            'specific': {
                'document': {'name': 'doc1', 'version': 1},
                'dataset': {'dataset_key': 'key1', 'dataset_counter_key': 'counter_key1'},
                'model': {'model_id': 'model1', 'model_language': 'en'}
            },
            'headers': {'Authorization': 'Bearer token123'},
            'request_json': {'dataset_status_key': 'test:status'}
        }

    def test_get_credentials(self):
        result = get_credentials(self.json_input)
        self.assertEqual(result, {'user': {'name': 'test_user', 'password': 'test_pass'}})

    def test_get_generic(self):
        result = get_generic(self.json_input)
        self.assertEqual(result['dataset_conf']['dataset_id'], '1234')
        self.assertEqual(result['process_type'], 'test_process')

    def test_get_specific(self):
        result = get_specific(self.json_input)
        self.assertEqual(result['document']['name'], 'doc1')
        self.assertEqual(result['dataset']['dataset_key'], 'key1')

    def test_get_department(self):
        with self.assertRaises(KeyError):  # Missing 'department' in generic config
            get_department(json_input=self.json_input)

    def test_get_document(self):
        result = get_document(json_input=self.json_input)
        self.assertEqual(result['name'], 'doc1')
        self.assertEqual(result['version'], 1)

    def test_get_dataset_keys(self):
        result = get_dataset_keys(json_input=self.json_input)
        self.assertEqual(result['dataset_key'], 'key1')

    def test_get_exc_info(self):
        with patch.dict(os.environ, {'LOG_LEVEL': 'DEBUG'}):
            self.assertTrue(get_exc_info(10))
        with patch.dict(os.environ, {'LOG_LEVEL': 'INFO'}):
            self.assertFalse(get_exc_info(10))

    def test_get_dataset_status_key_with_request_json(self):
        result = get_dataset_status_key(json_input=self.json_input)
        self.assertEqual(result, 'test:status')

    def test_get_dataset_status_key_with_specific(self):
        json_input = {'specific': {'dataset': {'dataset_key': 'specific_key'}}}
        result = get_dataset_status_key(json_input=json_input)
        self.assertEqual(result, 'specific_key')

    def test_generate_dataset_status_key_with_dataset_id(self):
        result = generate_dataset_status_key(self.json_input)
        self.assertEqual(result, '1234:1234')

    @patch("genai_json_parser.datetime")
    def test_generate_dataset_status_key_without_dataset_id(self, mock_datetime):
        mock_datetime.now.return_value = datetime(2024, 6, 6, 12, 0, 0)
        with patch("random.choice", return_value='a'):
            del self.json_input['generic']['dataset_conf']
            result = generate_dataset_status_key(self.json_input)
            self.assertTrue(result.startswith("test_process_20240606_120000_"))

    def test_get_headers(self):
        result = get_headers(self.json_input)
        self.assertEqual(result, {'Authorization': 'Bearer token123'})

    def test_get_dataset_counter_key(self):
        result = get_dataset_counter_key(json_input=self.json_input)
        self.assertEqual(result, 'counter_key1')

    def test_get_dataset_config(self):
        result = get_dataset_config(json_input=self.json_input)
        self.assertEqual(result['dataset_id'], '1234')
        self.assertEqual(result['other_param'], 'value')

    def test_get_dataset_id(self):
        result = get_dataset_id(json_input=self.json_input)
        self.assertEqual(result, '1234')

    def test_get_ocr_config(self):
        result = get_ocr_config(json_input=self.json_input)
        self.assertEqual(result['param1'], 10)

    def test_get_project_type(self):
        result = get_project_type(json_input=self.json_input)
        self.assertEqual(result, 'text')

    def test_get_force_ocr(self):
        result = get_force_ocr(json_input=self.json_input)
        self.assertTrue(result)

    def test_get_languages(self):
        result = get_languages(json_input=self.json_input)
        self.assertEqual(result, ['en', 'fr'])

    def test_get_train_conf(self):
        result = get_train_conf(json_input=self.json_input)
        self.assertEqual(result['model']['type'], 'genai')
        self.assertEqual(result['model']['hyperparams']['lr'], 0.01)
    
    def test_get_models_config(self):
        result = get_models_config(json_input=self.json_input)
        self.assertEqual(result[0]['model_id'], 'model1')
        self.assertEqual(result[0]['model_type'], 'genai')

    def test_select_model(self):
        result, language = select_model(self.json_input)
        self.assertEqual(result['model_id'], 'model1')
        self.assertEqual(language, 'en')

    def test_get_model_parameters(self):
        result = get_model_parameters(self.json_input)
        model_type, model_language, model_params = result
        self.assertEqual(model_type, 'genai')
        self.assertEqual(model_language, 'en')
        self.assertEqual(model_params['model_conf']['lr'], 0.01)

    def test_get_indexation_conf(self):
        result = get_index_conf(json_input=self.json_input)
        self.assertEqual(result['index_param'], 'index_value')
    
    def test_get_metadata_conf(self):
        get_metadata_conf(json_input=self.json_input)

    def test_get_compose_conf(self):
        result = get_compose_conf(json_input=self.json_input)
        self.assertEqual(result['compose_key'], 'compose_value')

    def test_get_elastic_params(self):
        result = get_elastic_params(json_input=self.json_input)
        self.assertEqual(result['elastic_key'], 'elastic_value')

    def test_get_layout_conf(self):
        result = get_layout_conf(json_input=self.json_input)
        self.assertEqual(result['param1'], 'layout_value')
    
    def test_get_do_cells_text(self):
        result = get_do_cells_text(self.json_input)
        self.assertTrue(result)

    def test_get_do_lines_text(self):
        result = get_do_lines_text(self.json_input)
        self.assertFalse(result)

    def test_get_do_cells_ocr(self):
        result = get_do_cells_ocr(self.json_input)
        self.assertTrue(result)

    def test_get_do_lines_ocr(self):
        result = get_do_lines_ocr(self.json_input)
        self.assertFalse(result)

    def test_get_do_tables(self):
        result = get_do_tables(self.json_input)
        self.assertFalse(result)

    def test_get_do_titles(self):
        result = get_do_titles(self.json_input)
        self.assertFalse(result)

    def test_get_prediction_multilabel(self):
        result = get_prediction_multilabel(self.json_input)
        self.assertEqual(result['param1'], 'value1')

    def test_get_do_lines_conf(self):
        result = get_do_lines_conf(self.json_input)
        self.assertEqual(result, {})

    def test_get_tables_conf(self):
        result = get_tables_conf(self.json_input)
        self.assertEqual(result, {})

    def test_get_segmentation_conf(self):
        result = get_segmentation_conf(self.json_input)
        self.assertTrue(result['do_segments'])
        self.assertEqual(result['segmenters'], ['seg1', 'seg2'])

    def test_get_do_segments(self):
        result = get_do_segments(self.json_input)
        self.assertTrue(result)

    def test_get_segmenters(self):
        result = get_segmenters(self.json_input)
        self.assertEqual(result, ['seg1', 'seg2'])