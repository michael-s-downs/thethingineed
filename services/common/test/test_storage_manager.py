### This code is property of the GGAO ###

import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
import os
import json
from common.errors.genaierrors import PrintableGenaiError
from storage_manager import LLMStorageManager, BaseStorageManager, IRStorageManager, ManagerStorage

# Constants
WORKSPACE = "test_workspace"
ORIGIN = "test_origin"
MODELS_FILE_PATH = "src/LLM/conf/models_config.json"
PROMPTS_PATH = "src/LLM/prompts/"
IR_MODELS_FILE_PATH = "src/ir/conf/models_config.json"

@pytest.fixture
def storage_manager():
    return LLMStorageManager(WORKSPACE, ORIGIN)

@pytest.fixture
def ir_storage_manager():
    return IRStorageManager(WORKSPACE, ORIGIN)

@patch("common.genai_controllers.load_file")
def test_get_available_models(mock_load_file, storage_manager):
    # Success case
    mock_load_file.return_value = '{"LLMs": {"model1": [{}], "model2": [{}]}}'
    storage_manager.load_file = mock_load_file
    models = storage_manager.get_available_models()
    assert models == {"model1": [{}], "model2": [{}]}

    # File not found case
    mock_load_file.return_value = None
    with pytest.raises(PrintableGenaiError):
        storage_manager.get_available_models()

    # Malformed JSON case
    mock_load_file.return_value = '{"Invalid": "Data"}'
    with pytest.raises(PrintableGenaiError):
        storage_manager.get_available_models()

@patch("common.genai_controllers.load_file")
def test_get_available_pools(mock_load_file, storage_manager):
    # Success case
    mock_load_file.return_value = '{"LLMs": {"model1": [{"model_pool": ["pool1", "pool2"]}], "model2": [{"model_pool": ["pool2"]}]}}'
    storage_manager.load_file = mock_load_file
    pools = storage_manager.get_available_pools()
    assert pools == {"pool1": [{"model_pool": ["pool1", "pool2"]}], "pool2": [{"model_pool": ["pool1", "pool2"]}, {"model_pool": ["pool2"]}]}

    # File not found case
    mock_load_file.return_value = None
    with pytest.raises(PrintableGenaiError):
        storage_manager.get_available_pools()

    # Malformed JSON case
    mock_load_file.return_value = '{"LLMs": {}}'
    with pytest.raises(PrintableGenaiError):
        storage_manager.get_available_pools()

@patch.dict(os.environ, {"LANGFUSE": "true"})
def test_get_template_langfuse_success(storage_manager):
    storage_manager.langfuse_m = MagicMock()
    prompt_mock = MagicMock()
    prompt_mock.prompt = json.dumps({"key": "value"})
    storage_manager.langfuse_m.load_template.return_value = prompt_mock

    template = storage_manager.get_template("template1")
    assert template == {"key": "value"}


@patch.dict(os.environ, {"LANGFUSE": "true"})
def test_get_template_langfuse_empty_string(storage_manager):
    storage_manager.langfuse_m = MagicMock()
    prompt_mock = MagicMock()
    prompt_mock.prompt = ""
    storage_manager.langfuse_m.load_template.return_value = prompt_mock

    with pytest.raises(PrintableGenaiError) as exc:
        storage_manager.get_template("template1")
    assert "doesn't exist" in str(exc.value)


@patch.dict(os.environ, {"LANGFUSE": "true"})
def test_get_template_langfuse_empty_json(storage_manager):
    storage_manager.langfuse_m = MagicMock()
    prompt_mock = MagicMock()
    prompt_mock.prompt = "{}"
    storage_manager.langfuse_m.load_template.return_value = prompt_mock

    with pytest.raises(PrintableGenaiError) as exc:
        storage_manager.get_template("template1")
    assert "doesn't exist" in str(exc.value)


@patch.dict(os.environ, {"LANGFUSE": "true"})
@patch("storage_manager.get_error_word_from_exception")
def test_get_template_langfuse_invalid_json(mock_get_error, storage_manager):
    storage_manager.langfuse_m = MagicMock()
    prompt_mock = MagicMock()
    prompt_mock.prompt = '{"invalid": "json"'  # Missing closing brace
    storage_manager.langfuse_m.load_template.return_value = prompt_mock
    mock_get_error.return_value = "invalid"

    with pytest.raises(PrintableGenaiError) as exc:
        storage_manager.get_template("template1")
    assert "not json serializable" in str(exc.value)
    assert "<invalid>" in str(exc.value)


@patch.dict(os.environ, {"LANGFUSE": "true"})
def test_get_template_langfuse_generic_exception(storage_manager):
    storage_manager.langfuse_m = MagicMock()
    storage_manager.langfuse_m.load_template.side_effect = Exception("any error")

    with pytest.raises(PrintableGenaiError) as exc:
        storage_manager.get_template("template1")
    assert "doesn't exist" in str(exc.value)


@patch.dict(os.environ, {}, clear=True)
@patch("storage_manager.load_file")
def test_get_template_file_success(mock_load_file, storage_manager):
    mock_load_file.return_value = json.dumps({"key": "value"})
    template = storage_manager.get_template("template1")
    assert template == {"key": "value"}


@patch.dict(os.environ, {}, clear=True)
@patch("storage_manager.load_file")
def test_get_template_file_empty_string(mock_load_file, storage_manager):
    mock_load_file.return_value = ""
    with pytest.raises(PrintableGenaiError) as exc:
        storage_manager.get_template("template1")
    assert "not found or is empty" in str(exc.value)


@patch.dict(os.environ, {}, clear=True)
@patch("storage_manager.load_file")
def test_get_template_file_empty_json(mock_load_file, storage_manager):
    mock_load_file.return_value = "{}"
    with pytest.raises(PrintableGenaiError) as exc:
        storage_manager.get_template("template1")
    assert "is empty" in str(exc.value)


@patch.dict(os.environ, {}, clear=True)
@patch("storage_manager.load_file")
@patch("storage_manager.get_error_word_from_exception")
def test_get_template_file_invalid_json(mock_get_error, mock_load_file, storage_manager):
    mock_load_file.return_value = '{"invalid": "json"'
    mock_get_error.return_value = "invalid"
    with pytest.raises(PrintableGenaiError) as exc:
        storage_manager.get_template("template1")
    assert "<invalid>" in str(exc.value)


@patch.dict(os.environ, {}, clear=True)
@patch("storage_manager.load_file", side_effect=ValueError("not found"))
def test_get_template_file_value_error(mock_load_file, storage_manager):
    with pytest.raises(PrintableGenaiError) as exc:
        storage_manager.get_template("template1")
    assert "doesn't exist" in str(exc.value)

@patch.dict(os.environ, {}, clear=True)
@patch("storage_manager.load_file")
def test_get_template_file_success(mock_load_file, storage_manager):
    mock_load_file.return_value = json.dumps({"key": "value"})
    template = storage_manager.get_template("template1")
    assert template == {"key": "value"}


@patch.dict(os.environ, {}, clear=True)
@patch("storage_manager.load_file")
def test_get_template_file_empty_string(mock_load_file, storage_manager):
    mock_load_file.return_value = ""
    with pytest.raises(PrintableGenaiError) as exc:
        storage_manager.get_template("template1")
    assert "not found or is empty" in str(exc.value)


@patch.dict(os.environ, {}, clear=True)
@patch("storage_manager.load_file")
def test_get_template_file_empty_json(mock_load_file, storage_manager):
    mock_load_file.return_value = "{}"
    with pytest.raises(PrintableGenaiError) as exc:
        storage_manager.get_template("template1")
    assert "is empty" in str(exc.value)


@patch.dict(os.environ, {}, clear=True)
@patch("storage_manager.load_file")
@patch("storage_manager.get_error_word_from_exception")
def test_get_template_file_invalid_json(mock_get_error, mock_load_file, storage_manager):
    mock_load_file.return_value = '{"invalid": "json"'
    mock_get_error.return_value = "invalid"
    with pytest.raises(PrintableGenaiError) as exc:
        storage_manager.get_template("template1")
    assert "<invalid>" in str(exc.value)


@patch.dict(os.environ, {}, clear=True)
@patch("storage_manager.load_file", side_effect=ValueError("not found"))
def test_get_template_file_value_error(mock_load_file, storage_manager):
    with pytest.raises(PrintableGenaiError) as exc:
        storage_manager.get_template("template1")
    assert "doesn't exist" in str(exc.value)

@patch("storage_manager.list_files")
@patch("common.genai_controllers.load_file")
def test_get_templates(mock_load_file, mock_list_files, storage_manager):
    # Mock file listing and loading
    mock_list_files.return_value = ["template1.json", "template2.json"]
    mock_load_file.side_effect = [
        '{"key1": "value1"}',
        '{"key2": "value2"}'
    ]
    storage_manager.load_file = mock_load_file

    templates, keys = storage_manager.get_templates()
    assert templates == {"key1": "value1", "key2": "value2"}
    assert keys == ["key1", "key2"]

    # Malformed JSON case
    mock_load_file.side_effect = ['{"key1": "value1"}', 'INVALID_JSON']
    with patch.object(storage_manager.logger, "warning") as mock_warning:
        templates, keys = storage_manager.get_templates()
        mock_warning.assert_called_once()

@patch.dict(os.environ, {"LANGFUSE": "true"})
def test_get_templates_with_langfuse(storage_manager):
    mock_langfuse = MagicMock()
    mock_langfuse.get_list_templates.return_value = ["template1", "template2"]

    valid_prompt = MagicMock()
    valid_prompt.prompt = json.dumps({"key1": "value1"})

    invalid_prompt = MagicMock()
    invalid_prompt.prompt = "INVALID_JSON"

    mock_langfuse.load_template.side_effect = [valid_prompt, invalid_prompt]
    storage_manager.langfuse_m = mock_langfuse

    with patch.object(storage_manager.logger, "warning") as mock_warning:
        templates, keys = storage_manager.get_templates()
        assert templates == {"template1": {"key1": "value1"}}
        assert keys == ["template1"]
        mock_warning.assert_called_once_with("Malformed json file not loaded: template2")

    mock_langfuse.load_template.side_effect = [valid_prompt, invalid_prompt]
    storage_manager.langfuse_m = mock_langfuse

    with patch.object(storage_manager.logger, "warning"):
        templates, keys, templates_with_file = storage_manager.get_templates(return_files=True)
        assert templates == {"template1": {"key1": "value1"}}
        assert keys == ["template1"]
        assert templates_with_file == {"template1": ["key1"]}


@patch("storage_manager.upload_object")
def test_upload_template(mock_upload_object, storage_manager):
    data = {"name": "test_template", "content": "{}"}

    response = storage_manager.upload_template(data)
    assert response == {"status": "finished", "result": "Request finished", "status_code": 200}

    # KeyError case
    with patch.object(storage_manager.logger, "error") as mock_error:
        response = storage_manager.upload_template({"content": "{}"})
        assert response["status"] == "error"
        mock_error.assert_called_once()

    #Exception case
    mock_upload_object.side_effect = Exception
    with patch.object(storage_manager.logger, "error") as mock_error:
        response = storage_manager.upload_template(data)
        assert response["status"] == "error"
        mock_error.assert_called_once()


@patch("storage_manager.upload_object")
def test_upload_template_with_langfuse_enabled(mock_upload_object, storage_manager):
    data = {"name": "test_template", "content": "{}"}

    storage_manager.langfuse_m = MagicMock()

    with patch.dict(os.environ, {"LANGFUSE": "true"}):
        response = storage_manager.upload_template(data)

    storage_manager.langfuse_m.upload_template.assert_called_once_with(
        template_name="test_template",
        template_content="{}",
        label="llm_template"
    )

    assert response == {"status": "finished", "result": "Request finished", "status_code": 200}

@patch("storage_manager.delete_file")
def test_delete_template(mock_delete_file, storage_manager):
    data = {"name": "test_template"}

    response = storage_manager.delete_template(data)
    assert response == {"status": "finished", "result": "Request finished", "status_code": 200}

    # KeyError case
    with patch.object(storage_manager.logger, "error") as mock_error:
        response = storage_manager.delete_template({"content": "{}"})
        assert response["status"] == "error"
        mock_error.assert_called_once()

    #Exception case
    mock_delete_file.side_effect = Exception
    with patch.object(storage_manager.logger, "error") as mock_error:
        response = storage_manager.delete_template(data)
        assert response["status"] == "error"
        mock_error.assert_called_once()

@patch("common.genai_controllers.load_file")
def test_init_load_file_fallback(mock_load_file):
    # Test fallback mechanism
    mock_load_file.side_effect = [None, "Fallback content"]
    storage_manager = LLMStorageManager(WORKSPACE, ORIGIN)
    assert storage_manager.models_file_path == "src/compose/conf/models_config.json"

    # Test no fallback available
    mock_load_file.side_effect = [None, None]
    storage_manager = LLMStorageManager(WORKSPACE, ORIGIN)
    assert storage_manager.models_file_path == "src/compose/conf/models_config.json"

def test_base_storage():
    basemanager = BaseStorageManager("test_work", "test_origin")
    basemanager.delete_template({})
    basemanager.get_available_embedding_models()
    basemanager.get_available_models()
    basemanager.get_available_pools()
    basemanager.get_embedding_equivalences()
    basemanager.get_pools_per_embedding_model()
    basemanager.get_specific_files(None)
    basemanager.get_templates()
    basemanager.get_unique_embedding_models()
    basemanager.is_file_storage_type(None)
    basemanager.load_file(None, None)
    basemanager.upload_template({})


@patch("storage_manager.load_file")
def test_get_pools_per_embedding_model(mock_load_file, ir_storage_manager):
    # Success case
    mock_load_file.return_value = '{"embeddings": {"openai": [{"embedding_model": "text-embedding-ada-002", "embedding_model_name": "ada-002-genai-westeurope", "model_pool": ["pool1", "pool2"]}]}}'
    ir_storage_manager.load_file = mock_load_file
    pools = ir_storage_manager.get_pools_per_embedding_model()
    assert "openai" in pools
    assert "text-embedding-ada-002" in pools["openai"]
    assert "pool1" in pools["openai"]["text-embedding-ada-002"]

    # File not found case
    mock_load_file.return_value = None
    with pytest.raises(PrintableGenaiError):
        ir_storage_manager.get_pools_per_embedding_model()

@patch("storage_manager.load_file")
def test_get_unique_embedding_models(mock_load_file, ir_storage_manager):
    # Success case
    mock_load_file.return_value = '{"embeddings": {"openai": [{"embedding_model": "text-embedding-ada-002"}]}}'
    ir_storage_manager.load_file = mock_load_file
    unique_models = ir_storage_manager.get_unique_embedding_models()
    assert "text-embedding-ada-002" in unique_models

    # File not found case
    mock_load_file.return_value = None
    with pytest.raises(PrintableGenaiError):
        ir_storage_manager.get_unique_embedding_models()

@patch("storage_manager.load_file")
def test_get_available_embedding_models(mock_load_file, ir_storage_manager):
    # Success case with inforetrieval_mode
    mock_load_file.return_value = '{"embeddings": {"openai": [{"embedding_model": "text-embedding-ada-002", "embedding_model_name": "ada-002-genai-westeurope"}]}}'
    ir_storage_manager.load_file = mock_load_file
    models = ir_storage_manager.get_available_embedding_models(inforetrieval_mode=True)
    assert len(models) == 1
    assert models[0]["platform"] == "openai"

    # File not found case
    mock_load_file.return_value = None
    with pytest.raises(PrintableGenaiError):
        ir_storage_manager.get_available_embedding_models()

@patch("storage_manager.load_file")
def test_get_embedding_equivalences(mock_load_file, ir_storage_manager):
    # Success case
    mock_load_file.return_value = '{"default_embedding": "value"}'
    ir_storage_manager.load_file = mock_load_file
    equivalences = ir_storage_manager.get_embedding_equivalences()
    assert equivalences["default_embedding"] == "value"

@patch("common.genai_controllers.load_file")
def test_get_available_pools_ir(mock_load_file, ir_storage_manager):
    # Success case
    mock_load_file.return_value = '{"embeddings": {"openai": [{"embedding_model_name": "text-embedding-ada-002", "model_pool": ["pool1", "pool2"]}]}}'
    ir_storage_manager.load_file = mock_load_file
    pools = ir_storage_manager.get_available_pools()
    assert "pool1" in pools
    assert "text-embedding-ada-002" in pools["pool1"]

    # File not found case
    mock_load_file.return_value = None
    with pytest.raises(PrintableGenaiError):
        ir_storage_manager.get_available_pools()

    # File not found case
    mock_load_file.return_value = '{"embeddings": {}}'
    ir_storage_manager.load_file = mock_load_file
    with pytest.raises(PrintableGenaiError):
        ir_storage_manager.get_available_pools()

@patch('storage_manager.IRStorageManager.load_file')
def test_get_dataframe_file_csv(mock_load_file, ir_storage_manager):
    mock_load_file.return_value = None  # No preprocessing needed for CSV

    with patch('storage_manager.get_dataset') as mock_get_dataset:
        mock_get_dataset.return_value = pd.DataFrame({
            'text': ["Sample text", None],
            'other_column': ["data1", "data2"]
        })

        df = ir_storage_manager._get_dataframe_file(
            csv=True,
            dataset_csv_path="dummy_path",
            txt_path="",
            do_titles=False,
            do_tables=False
        )

        assert not df.empty
        assert len(df) == 1  # Drop rows with NaN in 'text'
        assert 'text' in df.columns

@patch('storage_manager.IRStorageManager.load_file')
def test_get_dataframe_file_txt(mock_load_file, ir_storage_manager):
    mock_load_file.side_effect = [
        b"url\tSample text\ten\t10\tmetadata1:value1\tmetadata2:value2",
        b"Processed markdown text"
    ]

    df = ir_storage_manager._get_dataframe_file(
        csv=False,
        dataset_csv_path="",
        txt_path="dummy_txt_path",
        do_titles=True,
        do_tables=False
    )

    assert not df.empty
    assert 'text' in df.columns
    assert df['text'][0] == "Processed markdown text"
    assert 'metadata1' in df.columns
    assert df['metadata1'][0] == "value1"

@patch('storage_manager.IRStorageManager.load_file')
def test_get_markdown_files(mock_load_file, ir_storage_manager):
    mock_load_file.side_effect = [b"markdown text1", b"markdown text2"]

    df = pd.DataFrame({'Url': ["url1", "url2"]})

    markdown_files = ir_storage_manager._get_markdown_files(
        txt_path="dummy_txt_path",
        df=df,
        department="dummy_department",
        process_id="dummy_process"
    )

    assert len(markdown_files) == 2
    assert markdown_files[0] == b"markdown text1"
    assert markdown_files[1] == b"markdown text2"

@patch('storage_manager.IRStorageManager.load_file')
@patch('storage_manager.list_files')
def test_load_doc_per_pages(mock_list_files, mock_load_file, ir_storage_manager):
    mock_list_files.return_value = ["page1.txt", "page2.txt"]
    mock_load_file.side_effect = [b"Page 1 content", b"Page 2 content"]

    ir_storage_manager.doc_by_pages = []  # Ensure it's clean

    ir_storage_manager._load_doc_per_pages("dummy_txt_path")

    assert len(ir_storage_manager.doc_by_pages) == 2
    assert ir_storage_manager.doc_by_pages[0] == "Page 1 content"
    assert ir_storage_manager.doc_by_pages[1] == "Page 2 content"

@patch('storage_manager.IRStorageManager.load_file')
@patch('storage_manager.list_files')
def test_load_doc_per_pages_except(mock_list_files, mock_load_file, ir_storage_manager):
    mock_list_files.side_effect = Exception
    mock_load_file.side_effect = [b"Page 1 content", b"Page 2 content"]

    ir_storage_manager.doc_by_pages = []  # Ensure it's clean

    ir_storage_manager._load_doc_per_pages("dummy_txt_path")


def test_parse_metadata():
    metadata_str = '{"key": "value", "number": 123}'
    parsed_metadata = IRStorageManager._parse_metadata(metadata_str)
    assert parsed_metadata["key"] == "value"
    assert parsed_metadata["number"] == 123

    invalid_metadata_str = "Invalid JSON string"
    parsed_metadata = IRStorageManager._parse_metadata(invalid_metadata_str)
    assert parsed_metadata == "Invalid JSON string"


def test_get_file_storage_manager(ir_storage_manager):
    manager = ManagerStorage()
    manager.get_file_storage({"type": "IRStorage", "workspace": None, "origin": None})