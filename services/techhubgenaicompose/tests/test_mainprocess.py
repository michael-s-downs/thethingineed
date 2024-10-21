### This code is property of the GGAO ###

import pytest
import json
from unittest.mock import patch
from main import app
from common.errors.genaierrors import PrintableGenaiError

# Setup: Create a test client for the Flask app
@pytest.fixture
def client():
    app.config['TESTING'] = True
    client = app.test_client()
    yield client

# Test for success: Valid input
def test_process_success(mocker, client):
    # Mock the necessary method calls
    mock_director_run = mocker.patch('director.Director.run', return_value={"result": "processed successfully"})
    mock_report_api = mocker.patch('main.ComposeDeployment.report_api')

    # Sample valid input
    payload = {
        "generic": {
            "compose_conf": {
                "lang": "en",
                "template": {
                    "name": "retrieval",
                    "params": {
                        "query": "What is a pool of models in LLM service?",
                        "index": "testcomposellminforetrieval",
                        "platform": "azure",
                        "template_name": "system_query",
                        "template_llm": "rag_with_references",
                        "model": "gpt-4-pool-europe"
                    }
                },
                "persist": {"type": "chat", "params": {"max_persistence": 20}}
            }
        }
    }

    headers = {
        'x-tenant': 'test-tenant',
        'x-department': 'test-department',
        'x-reporting': 'test-reporting',
        'user-token': 'test-token'
    }

    response = client.post('/process', data=json.dumps(payload), headers=headers, content_type='application/json')

    assert response.status_code == 200
    assert json.loads(response.data) == {
        'status': 'finished',
        'result': {"result": "processed successfully"},
        'status_code': 200
    }

    mock_director_run.assert_called_once()
    mock_report_api.assert_called_once()

def test_process_key_error(mocker, client):
    mock_director_run = mocker.patch('director.Director.run')

    payload = {
        "generic": {
            "compose_conf": {
                "lang": "en",
                "persist": {"type": "chat", "params": {"max_persistence": 20}}
            }
        }
    }

    headers = {
        'x-tenant': 'test-tenant',
        'x-department': 'test-department',
        'x-reporting': '',
        'user-token': 'test-token'
    }

    with patch('main.get_generic') as mock_report_api:
        mock_report_api.side_effect = KeyError('Key: <template> not found')
        response = client.post('/process', data=json.dumps(payload), headers=headers, content_type='application/json')

        assert response.status_code == 404
        response_data = json.loads(response.data)
        assert response_data['status'] == 'error'

def test_process_exception(mocker, client):
    mock_director_run = mocker.patch('director.Director.run')

    payload = {
        "generic": {
            "compose_conf": {
                "lang": "en",
                "persist": {"type": "chat", "params": {"max_persistence": 20}}
            }
        }
    }

    headers = {
        'x-tenant': 'test-tenant',
        'x-department': 'test-department',
        'x-reporting': '',
        'user-token': 'test-token'
    }

    with patch('main.get_generic') as mock_report_api:
        mock_report_api.side_effect = PrintableGenaiError(500, "Error")
        response = client.post('/process', data=json.dumps(payload), headers=headers, content_type='application/json')

        assert response.status_code == 500
        response_data = json.loads(response.data)
        assert response_data['status'] == 'error'
