### This code is property of the GGAO ###

import os
os.environ['URL_LLM'] = "test_url"
os.environ['URL_RETRIEVE'] = "test_retrieve"
import pytest
from unittest.mock import patch, MagicMock
from langfusemanager import LangFuseManager

@pytest.fixture
def langfuse_manager():
    """Fixture to initialize LangFuseManager instance."""
    return LangFuseManager()

@patch('langfusemanager.Langfuse')
def test_parse_with_langfuse_enabled(mock_langfuse, langfuse_manager):
    """Test parse method when LangFuse is enabled."""
    # Mock environment variables and compose_config
    compose_config = {"langfuse": True}
    with patch.dict('os.environ', {"LANGFUSE_SECRET_KEY": "secret", "LANGFUSE_PUBLIC_KEY": "public", "LANGFUSE_HOST": "host"}):
        session_id = "test_session"
        langfuse_manager.parse(compose_config, session_id)
    
    mock_langfuse.assert_called_once_with(secret_key="secret", public_key="public", host="host")
    assert langfuse_manager.langfuse is not None
    langfuse_manager.langfuse.trace.assert_called_once_with(session_id=session_id)

@patch('langfusemanager.Langfuse')
def test_parse_with_langfuse_disabled(mock_langfuse, langfuse_manager):
    """Test parse method when LangFuse is disabled."""
    # Compose config without LangFuse enabled
    compose_config = {}
    session_id = "test_session"
    langfuse_manager.parse(compose_config, session_id)
    
    mock_langfuse.assert_not_called()
    assert langfuse_manager.langfuse is None

@patch('langfusemanager.Langfuse')
def test_update_metadata(mock_langfuse, langfuse_manager):
    """Test update_metadata when langfuse is initialized."""
    langfuse_manager.langfuse = MagicMock()
    langfuse_manager.trace = langfuse_manager.langfuse.trace

    metadata = {"key": "value"}
    langfuse_manager.update_metadata(metadata)
    langfuse_manager.trace.update.assert_called_once_with(metadata=metadata)

def test_update_metadata_no_langfuse(langfuse_manager):
    """Test update_metadata when langfuse is not initialized."""
    metadata = {"key": "value"}
    langfuse_manager.update_metadata(metadata)
    assert langfuse_manager.langfuse is None

@patch('langfusemanager.Langfuse')
def test_update_input(mock_langfuse, langfuse_manager):
    """Test update_input when langfuse is initialized."""
    langfuse_manager.langfuse = MagicMock()
    langfuse_manager.trace = langfuse_manager.langfuse.trace

    input_data = {"input": "value"}
    langfuse_manager.update_input(input_data)
    langfuse_manager.trace.update.assert_called_once_with(input=input_data)

def test_update_input_no_langfuse(langfuse_manager):
    """Test update_input when langfuse is not initialized."""
    input_data = {"input": "value"}
    langfuse_manager.update_input(input_data)
    assert langfuse_manager.langfuse is None

@patch('langfusemanager.Langfuse')
def test_update_output(mock_langfuse, langfuse_manager):
    """Test update_output when langfuse is initialized."""
    langfuse_manager.langfuse = MagicMock()
    langfuse_manager.trace = langfuse_manager.langfuse.trace

    output_data = {"output": "value"}
    langfuse_manager.update_output(output_data)
    langfuse_manager.trace.update.assert_called_once_with(output=output_data)

def test_update_output_no_langfuse(langfuse_manager):
    """Test update_output when langfuse is not initialized."""
    output_data = {"output": "value"}
    langfuse_manager.update_output(output_data)
    assert langfuse_manager.langfuse is None

@patch('langfusemanager.Langfuse')
def test_add_span(mock_langfuse, langfuse_manager):
    """Test add_span when langfuse is initialized."""
    langfuse_manager.langfuse = MagicMock()
    langfuse_manager.trace = langfuse_manager.langfuse.trace

    span_name = "test_span"
    metadata = {"key": "value"}
    input_data = {"input": "data"}
    langfuse_manager.add_span(span_name, metadata, input_data)
    langfuse_manager.trace.span.assert_called_once_with(name=span_name, metadata=metadata, input=input_data)

def test_add_span_no_langfuse(langfuse_manager):
    """Test add_span when langfuse is not initialized."""
    span_name = "test_span"
    metadata = {"key": "value"}
    input_data = {"input": "data"}
    langfuse_manager.add_span(span_name, metadata, input_data)
    assert langfuse_manager.langfuse is None

@patch('langfusemanager.Langfuse')
def test_add_span_output(mock_langfuse, langfuse_manager):
    """Test add_span_output when langfuse is initialized."""
    langfuse_manager.langfuse = MagicMock()
    langfuse_manager.trace = langfuse_manager.langfuse.trace
    span = MagicMock()

    output_data = {"output": "data"}
    langfuse_manager.add_span_output(span, output_data)
    span.end.assert_called_once_with(output=output_data)

def test_add_span_output_no_langfuse(langfuse_manager):
    """Test add_span_output when langfuse is not initialized."""
    span = MagicMock()
    output_data = {"output": "data"}
    langfuse_manager.add_span_output(span, output_data)
    assert langfuse_manager.langfuse is None

@patch('langfusemanager.Langfuse')
def test_add_generation(mock_langfuse, langfuse_manager):
    """Test add_generation when langfuse is initialized."""
    langfuse_manager.langfuse = MagicMock()
    langfuse_manager.trace = langfuse_manager.langfuse.trace

    name = "generation_name"
    metadata = {"key": "value"}
    input_data = {"input": "data"}
    model = "test_model"
    model_params = {"param": "value"}
    langfuse_manager.add_generation(name, metadata, input_data, model, model_params)
    langfuse_manager.trace.generation.assert_called_once_with(
        name=name,
        metadata=metadata,
        input=input_data,
        model=model,
        model_parameters=model_params
    )

def test_add_generation_no_langfuse(langfuse_manager):
    """Test add_generation when langfuse is not initialized."""
    name = "generation_name"
    metadata = {"key": "value"}
    input_data = {"input": "data"}
    model = "test_model"
    model_params = {"param": "value"}
    langfuse_manager.add_generation(name, metadata, input_data, model, model_params)
    assert langfuse_manager.langfuse is None

@patch('langfusemanager.Langfuse')
def test_add_generation_output(mock_langfuse, langfuse_manager):
    """Test add_generation_output when langfuse is initialized."""
    langfuse_manager.langfuse = MagicMock()
    langfuse_manager.trace = langfuse_manager.langfuse.trace
    generation = MagicMock()

    output_data = {"output": "data"}
    langfuse_manager.add_generation_output(generation, output_data)
    generation.end.assert_called_once_with(output=output_data)

def test_add_generation_output_no_langfuse(langfuse_manager):
    """Test add_generation_output when langfuse is not initialized."""
    generation = MagicMock()
    output_data = {"output": "data"}
    langfuse_manager.add_generation_output(generation, output_data)
    assert langfuse_manager.langfuse is None

@patch('langfusemanager.Langfuse')
def test_flush(mock_langfuse, langfuse_manager):
    """Test flush when langfuse is initialized."""
    langfuse_manager.langfuse = MagicMock()
    langfuse_manager.flush()
    langfuse_manager.langfuse.flush.assert_called_once()

def test_flush_no_langfuse(langfuse_manager):
    """Test flush when langfuse is not initialized."""
    langfuse_manager.flush()
    assert langfuse_manager.langfuse is None

