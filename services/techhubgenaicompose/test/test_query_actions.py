### This code is property of the GGAO ###


import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from compose.query_actions.expansion import ExpansionFactory, LangExpansion, StepSplitExpansion
from compose.query import expansion
from common.errors.genaierrors import PrintableGenaiError, GenaiError
from common.errors.LLM import LLMParser

@pytest.fixture
def params():
    return {
        "headers": {"Authorization": "Bearer dummy_token"},
        "langs": ["en", "espanol"]
    }

@pytest.fixture
def params_lang_empty():
    return {
        "headers": {"Authorization": "Bearer dummy_token"}
    }

@pytest.fixture
def actions_confs():
    return [
        {
            "action": "retrieve",
            "action_params": {
                "params": {
                    "generic": {
                        "index_conf": {
                            "query": ""
                        }
                    }
                }
            }
        }
    ]

@pytest.fixture
def query():
    return "Hello, how are you?"

@pytest.fixture
def llm_parser():
    return LLMParser()

@pytest.mark.asyncio
@patch("aiohttp.ClientSession.post")
async def test_async_call_llm_200(mock_post, query):
    """Test async_call_llm handles a 200 OK response correctly"""
    # Mock a successful response
    mock_response = MagicMock()
    mock_response.status = 200  # Set the status to 200
    mock_response.json = AsyncMock(return_value={"result": {"answer": "Hola, ¿cómo estás?"}})  # Set the json method
    
    # Mock the post method to return our mock response
    mock_post.return_value.__aenter__.return_value = mock_response

    lang_expansion = LangExpansion(query)
    template = {"query_metadata": {"query": "Translate this"}}
    headers = {"Authorization": "Bearer dummy_token"}

    # Call the async function
    mock_session = MagicMock()
    mock_session.post = mock_post
    result = await lang_expansion.async_call_llm(template, headers, mock_session)

    # Validate the result
    assert result["answer"] == "Hola, ¿cómo estás?"


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.post")
async def test_control_errors_403(mock_post, query):
    """Test control_errors handles 403 Forbidden"""
    # Mock a successful response
    mock_response = MagicMock()
    mock_response.status = 403  
    
    # Mock the post method to return our mock response
    mock_post.return_value.__aenter__.return_value = mock_response

    lang_expansion = LangExpansion(query)
    template = {"query_metadata": {"query": "Translate this"}}
    headers = {"Authorization": "Bearer dummy_token"}

    # Call the async function
    mock_session = MagicMock()
    mock_session.post = mock_post

    # Validate the result
    with pytest.raises(GenaiError) as exc_info:
        await lang_expansion.async_call_llm(template, headers, mock_session)
    assert "Service is non reachable" in str(exc_info.value)

@pytest.mark.asyncio
@patch("aiohttp.ClientSession.post")
async def test_control_errors_400(mock_post, query):
    """Test control_errors handles 400 Bad Request"""
    mock_response = MagicMock()
    mock_response.status = 400
    
    # Mock the post method to return our mock response
    mock_post.return_value.__aenter__.return_value = mock_response

    lang_expansion = LangExpansion(query)
    template = {"query_metadata": {"query": "Translate this"}}
    headers = {"Authorization": "Bearer dummy_token"}

    # Call the async function
    mock_session = MagicMock()
    mock_session.post = mock_post

    # Validate the result
    with pytest.raises(PrintableGenaiError) as exc_info:
        await lang_expansion.async_call_llm(template, headers, mock_session)
    assert "filtered out due to sensitive content" in str(exc_info.value)

@pytest.mark.asyncio
@patch("aiohttp.ClientSession.post")
async def test_control_errors_401(mock_post, query):
    """Test control_errors handles 401 Unauthorized"""
    mock_response = MagicMock()
    mock_response.status = 401
    
    # Mock the post method to return our mock response
    mock_post.return_value.__aenter__.return_value = mock_response

    lang_expansion = LangExpansion(query)
    template = {"query_metadata": {"query": "Translate this"}}
    headers = {"Authorization": "Bearer dummy_token"}

    # Call the async function
    mock_session = MagicMock()
    mock_session.post = mock_post

    # Validate the result
    with pytest.raises(PrintableGenaiError) as exc_info:
        await lang_expansion.async_call_llm(template, headers, mock_session)
    assert "Credentials failed" in str(exc_info.value)

@pytest.mark.asyncio
@patch("aiohttp.ClientSession.post")
async def test_control_errors_408(mock_post, query):
    """Test control_errors handles 408 Request Timeout"""
    mock_response = MagicMock()
    mock_response.status = 408
    
    # Mock the post method to return our mock response
    mock_post.return_value.__aenter__.return_value = mock_response

    lang_expansion = LangExpansion(query)
    template = {"query_metadata": {"query": "Translate this"}}
    headers = {"Authorization": "Bearer dummy_token"}

    # Call the async function
    mock_session = MagicMock()
    mock_session.post = mock_post

    # Validate the result
    with pytest.raises(PrintableGenaiError) as exc_info:
        await lang_expansion.async_call_llm(template, headers, mock_session)
    assert "Request timed out" in str(exc_info.value)

@pytest.mark.asyncio
@patch("aiohttp.ClientSession.post")
async def test_control_errors_503(mock_post, query):
    """Test control_errors handles 503 Service Unavailable"""
    mock_response = MagicMock()
    mock_response.status = 503
    
    # Mock the post method to return our mock response
    mock_post.return_value.__aenter__.return_value = mock_response

    lang_expansion = LangExpansion(query)
    template = {"query_metadata": {"query": "Translate this"}}
    headers = {"Authorization": "Bearer dummy_token"}

    # Call the async function
    mock_session = MagicMock()
    mock_session.post = mock_post

    # Validate the result
    with pytest.raises(PrintableGenaiError) as exc_info:
        await lang_expansion.async_call_llm(template, headers, mock_session)
    assert "Model is overloaded" in str(exc_info.value)

@pytest.mark.asyncio
@patch("aiohttp.ClientSession.post")
async def test_control_errors_404(mock_post, query):
    """Test control_errors handles 404 Not Found"""
    mock_response = MagicMock()
    mock_response.status = 404
    
    # Mock the post method to return our mock response
    mock_post.return_value.__aenter__.return_value = mock_response

    lang_expansion = LangExpansion(query)
    template = {"query_metadata": {"query": "Translate this"}}
    headers = {"Authorization": "Bearer dummy_token"}

    # Call the async function
    mock_session = MagicMock()
    mock_session.post = mock_post

    # Validate the result
    with pytest.raises(PrintableGenaiError) as exc_info:
        await lang_expansion.async_call_llm(template, headers, mock_session)
    assert "Deployed model is not available" in str(exc_info.value)

@pytest.mark.asyncio
@patch("aiohttp.ClientSession.post")
async def test_control_errors_500(mock_post, query):
    mock_response = MagicMock()
    mock_response.status = 500
    
    # Mock the post method to return our mock response
    mock_post.return_value.__aenter__.return_value = mock_response

    lang_expansion = LangExpansion(query)
    template = {"query_metadata": {"query": "Translate this"}}
    headers = {"Authorization": "Bearer dummy_token"}

    # Call the async function
    mock_session = MagicMock()
    mock_session.post = mock_post

    # Validate the result
    with pytest.raises(GenaiError) as exc_info:
        await lang_expansion.async_call_llm(template, headers, mock_session)
    assert "Internal server error" in str(exc_info.value)
    
@pytest.mark.asyncio
@patch("aiohttp.ClientSession.post")
async def test_control_errors_500(mock_post, query):
    mock_response = MagicMock()
    mock_response.status = 666
    
    # Mock the post method to return our mock response
    mock_post.return_value.__aenter__.return_value = mock_response

    lang_expansion = LangExpansion(query)
    template = {"query_metadata": {"query": "Translate this"}}
    headers = {"Authorization": "Bearer dummy_token"}

    # Call the async function
    mock_session = MagicMock()
    mock_session.post = mock_post

    # Validate the result
    with pytest.raises(GenaiError) as exc_info:
        await lang_expansion.async_call_llm(template, headers, mock_session)
    assert "not implemented" in str(exc_info.value)

# Test the parallel_calls method
@pytest.mark.asyncio
@patch("compose.query_actions.expansion.LangExpansion.async_call_llm", new_callable=AsyncMock)
async def test_parallel_calls(mock_async_call_llm, query):
    # Mock the async_call_llm to return different translations
    mock_async_call_llm.side_effect = [
        {"answer": "Hola, ¿cómo estás?"},
        {"answer": "Hello, how are you?"}
    ]

    lang_expansion = LangExpansion(query)

    templates = [
        {"query_metadata": {"query": "Translate: Hello to Spanish"}},
        {"query_metadata": {"query": "Translate: Hello to English"}}
    ]
    headers = {"Authorization": "Bearer dummy_token"}

    # Run the parallel_calls method
    result = await lang_expansion.parallel_calls(templates, headers)

    # Ensure we got the two translated results
    assert len(result) == 2
    assert result[0]["answer"] == "Hola, ¿cómo estás?"
    assert result[1]["answer"] == "Hello, how are you?"



@patch("compose.query_actions.expansion.LangExpansion.async_call_llm")
@patch("compose.query_actions.expansion.LangExpansion.parallel_calls")
def test_lang_expansion_process(mock_parallel_calls, mock_async_call_llm, params, actions_confs, query):
    # Mock response for async LLM call
    mock_async_call_llm.return_value = {"answer": "Hola, ¿cómo estás?"}
    mock_parallel_calls.return_value = [{"answer": "Hola, ¿cómo estás?"}, {"answer": "Hello, how are you?"}]
    
    # Create an instance of LangExpansion and run the process
    lang_expansion = LangExpansion(query)
    result = lang_expansion.process(params, actions_confs)

    # Check if result has the translated queries
    assert len(result) == 2
    assert result[0] == "Hola, ¿cómo estás?"
    assert result[1] == "Hello, how are you?"

    # Ensure the actions_confs has been updated
    assert actions_confs[0]["action_params"]["params"]["generic"]["index_conf"]["query"] == query

@patch("compose.query_actions.expansion.LangExpansion.async_call_llm")
@patch("compose.query_actions.expansion.LangExpansion.parallel_calls")
def test_lang_expansion_process_lang_empty(mock_parallel_calls, mock_async_call_llm, params_lang_empty, actions_confs, query):
    # Mock response for async LLM call
    mock_async_call_llm.return_value = {"answer": "Hola, ¿cómo estás?"}
    mock_parallel_calls.return_value = [{"answer": "Hola, ¿cómo estás?"}, {"answer": "Hello, how are you?"}]
    
    # Create an instance of LangExpansion and run the process
    lang_expansion = LangExpansion(query)
    with pytest.raises(PrintableGenaiError, match="Langs to expand not provided"):
        lang_expansion.process(params_lang_empty, actions_confs)


def test_expansion_factory_valid_expansion_type(params, actions_confs, query):
    factory = ExpansionFactory("lang")
    assert isinstance(factory.expansionmethod(query), LangExpansion)

def test_expansion_factory_invalid_expansion_type(params, actions_confs, query):
    with pytest.raises(PrintableGenaiError):
        ExpansionFactory("invalid_type")

@patch("compose.query_actions.expansion.LangExpansion.process")
def test_expansion_function(mock_process, params, query, actions_confs):
    # Mock the result from the process method
    mock_process.return_value = ["Hola, ¿cómo estás?", "Hello, how are you?"]

    result = expansion("lang", params, query, actions_confs)

    # Check if expansion function correctly processes the query
    assert result == ["Hola, ¿cómo estás?", "Hello, how are you?"]

def test_lang_expansion_invalid_lang_list(params, actions_confs, query):
    params["langs"] = "not a list"
    
    with pytest.raises(PrintableGenaiError, match="Param <langs> is not a list"):
        LangExpansion(query).process(params, actions_confs)

def test_lang_expansion_no_retrieve_action(params, actions_confs, query):
    actions_confs.clear()

    with pytest.raises(PrintableGenaiError):
        LangExpansion(query).process(params, actions_confs)


@patch("compose.query_actions.expansion.requests.post")
def test_call_llm(mock_post, query):
    mock_response = MagicMock()
    mock_response.status_code = 200  # Set the status to 200
    mock_response.json = MagicMock(return_value={"result": {"answer": "Hola, ¿cómo estás?"}})  # Set the json method
    
    # Mock the post method to return our mock response
    mock_post.return_value = mock_response

    step_expansion = StepSplitExpansion(query)
    template = {"query_metadata": {"query": "Translate this"}}
    headers = {"Authorization": "Bearer dummy_token"}

    # Call the async function
    mock_session = MagicMock()
    mock_session.post = mock_post
    result = step_expansion.call_llm(template, headers)

    # Validate the result
    assert result["answer"] == "Hola, ¿cómo estás?"

@patch("compose.query_actions.expansion.requests.post")
def test_call_llm_400(mock_post, query):
    mock_response = MagicMock()
    mock_response.status_code = 400  # Set the status to 200
    mock_response.json = MagicMock(return_value={"result": {"answer": "Hola, ¿cómo estás?"}})  # Set the json method
    
    # Mock the post method to return our mock response
    mock_post.return_value = mock_response

    step_expansion = StepSplitExpansion(query)
    template = {"query_metadata": {"query": "Translate this"}}
    headers = {"Authorization": "Bearer dummy_token"}

    # Call the async function
    mock_session = MagicMock()
    mock_session.post = mock_post
    with pytest.raises(PrintableGenaiError):
        step_expansion.call_llm(template, headers)


@patch("compose.query_actions.expansion.requests.post")
def test_call_llm_error(mock_post, query):
    
    # Mock the post method to return our mock response
    mock_post.side_effect = Exception

    step_expansion = StepSplitExpansion(query)
    template = {"query_metadata": {"query": "Translate this"}}
    headers = {"Authorization": "Bearer dummy_token"}

    # Call the async function
    mock_session = MagicMock()
    mock_session.post = mock_post
    with pytest.raises(PrintableGenaiError):
        step_expansion.call_llm(template, headers)


@patch("compose.query_actions.expansion.StepSplitExpansion.call_llm")
def test_lang_expansion_step_process(mock_async_call_llm, params, actions_confs, query):
    mock_async_call_llm.return_value = {"answer": "Hola, ¿cómo estás?"}
    
    expansion = StepSplitExpansion(query)
    params["k_steps"] = 4
    params["context"] = "hello"
    params["model"] = "example_model"
    result = expansion.process(params, actions_confs)

    assert len(result) == 1
    assert result[0] == "Hola, ¿cómo estás?"

    assert actions_confs[0]["action_params"]["params"]["generic"]["index_conf"]["query"] == "Hola, ¿cómo estás?"