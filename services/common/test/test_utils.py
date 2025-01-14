### This code is property of the GGAO ###

import pytest
import os
import json
from unittest.mock import patch, mock_open, MagicMock
from shutil import rmtree
from utils import (
    convert_service_to_queue,
    convert_service_to_endpoint,
    remove_local_files,
    convert_to_queue_extractor,
    load_secrets,
    get_error_word_from_exception,
    get_models,
    ELASTICSEARCH_INDEX,
    resize_image
)

# Mock constants
SECRETS_ROOT_PATH = '/secrets'

@pytest.fixture
def mock_env_vars():
    return {
        "SECRETS_PATH": "/mock_secrets",
        "AWS_ACCESS_KEY": "mock_access_key",
        "AWS_SECRET_KEY": "mock_secret_key",
    }

@patch.dict(os.environ, {"SECRETS_PATH": "/mock_secrets"})
def test_convert_service_to_queue():
    assert convert_service_to_queue("test-service") == "Q_TEST_SERVICE"
    assert convert_service_to_queue("test-service", provider="gcp") == "test-service"

@patch.dict(os.environ, {"SECRETS_PATH": "/mock_secrets"})
def test_convert_service_to_endpoint():
    assert convert_service_to_endpoint("test-service") == "/test_service"

@patch("utils.rmtree")
def test_remove_local_files(mock_rmtree):
    remove_local_files("/some/path/to/file")
    mock_rmtree.assert_called_once()

@patch.dict(os.environ, {"SECRETS_PATH": "/mock_secrets"})
def test_convert_to_queue_extractor():
    assert convert_to_queue_extractor("test-extractor") == "Q_TEST_EXTRACTOR_EXTRACTOR"

@patch("os.path.exists")
@patch("builtins.open", new_callable=mock_open, read_data='{"models": {}}')
def test_load_secrets(mock_open, mock_exists, mock_env_vars):
    mock_exists.side_effect = lambda path: True

    with patch.dict(os.environ, mock_env_vars):
        models, vector_storages, aws_credentials = load_secrets()

    assert models == {'models':{}}
    # assert vector_storages is not None
    assert aws_credentials == {'models': {}}

def test_load_secrets_file_not_found(mock_env_vars):

    with patch.dict(os.environ, mock_env_vars):
        with pytest.raises(FileNotFoundError):
            load_secrets()

@patch("os.path.exists")
def test_load_secrets_no_vector_storage(mock_exists, mock_env_vars):
    mock_exists.side_effect = lambda path: False

    with patch.dict(os.environ, mock_env_vars):
        with pytest.raises(FileNotFoundError):
            load_secrets(vector_storage_needed=False)

def test_get_error_word_from_exception():
    json_string = '{"key": "value", "broken": $error_word }'
    ex = ValueError("Expecting value: line 1 column 28 (char 27)")
    error_word = get_error_word_from_exception(ex, json_string)
    assert error_word == "$error_word"

def test_get_models_with_pool():
    available_models = {}
    available_pools = {
        "pool1": [{"model": "model1"}, {"model": "model2"}]
    }
    models, _ = get_models(available_models, available_pools, "pool", "pool1")
    assert models == ["model1", "model2"]

def test_get_models_with_key():
    available_models = [{"platform": "aws", "model": "model1", "zone": "us"}]
    available_pools = {}
    models, pools = get_models(available_models, available_pools, "platform", "aws")
    assert models == ["model1"]
    assert pools == []

def test_get_models_with_key_dict():
    available_models = {"azure": [
		{
			"model": "techhubdev-AustraliaEast-dall-e-3",
			"model_type": "dalle3",
			"max_input_tokens": 4000,
			"zone": "techhubdev-AustraliaEast",
			"message": "dalle",
			"api_version": "2023-12-01-preview",
			"model_pool": ["techhubdev-pool-world-dalle3","techhub-pool-world-dalle3"]
		}
    ]}
    available_pools = {}
    models, pools = get_models(available_models, available_pools, "platform", "aws")
    assert models == []
    assert pools == []

def test_get_models_with_key_dict_error():
    available_models = {"azure": [
		{
			"model_error": "techhubdev-AustraliaEast-dall-e-3",
			"model_type": "dalle3",
			"max_input_tokens": 4000,
			"zone_error": "techhubdev-AustraliaEast",
			"message": "dalle",
			"api_version": "2023-12-01-preview",
			"model_pool": ["techhubdev-pool-world-dalle3","techhub-pool-world-dalle3"]
		}
    ]}
    available_pools = {}
    with pytest.raises(ValueError):
        get_models(available_models, available_pools, "platform_error", "aws")

def test_elasticsearch_index():
    assert ELASTICSEARCH_INDEX("index", "model") == "index_model"
    assert ELASTICSEARCH_INDEX("index:name", "model") == "index_name_model"

@patch("PIL.Image.open")
@patch("os.stat")
def test_resize_image(mock_stat, mock_image_open):
    mock_stat.return_value = MagicMock(st_size=20000.00)
    mock_image = MagicMock(format="PNG", size=(27893, 73829))
    mock_image.resize.return_value = mock_image
    mock_image.save.return_value = None
    mock_image_open.return_value = mock_image

    current_size, resized = resize_image("test")
    assert current_size == 0.019073486328125
    assert not resized

    mock_stat.return_value = MagicMock(st_size=20000000.00)

    with pytest.raises(RuntimeError):
        resize_image("test")
