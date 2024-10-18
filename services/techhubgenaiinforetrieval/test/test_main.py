### This code is property of the GGAO ###


# Native imports
import re, copy, json

# Installed imports
import pytest
from unittest.mock import patch, MagicMock
from unittest import mock

# Local imports
from common.errors.genaierrors import PrintableGenaiError
from common.utils import load_secrets
from main import app

models_credentials, aws_credentials = load_secrets(vector_storage_needed=False)
#models_urls = models_credentials.get('URLs')


class TestMain:
    headers = {
        'x-tenant': 'develop',
        'x-department': 'main',
        'x-reporting': ''
    }




@pytest.fixture
def client():
    with app.test_client() as client:
        yield client

def test_retrieve_documents(client):
    body = {
        "index": "test",
        "filters": "{\"test_system_query_v\": {\"system\": \"$system\", \"user\": [{\"type\": \"text\", \"text\": \"Answer the question as youngster: \"},{\"type\": \"image_url\",\"image\": {\"url\": \"https://static-00.iconduck.com/assets.00/file-type-favicon-icon-256x256-6l0w7xol.png\",\"detail\": \"high\"}},\"$query\"]}}"
    }
    with patch('main.manage_actions_get_elasticsearch') as mock_get_es, patch('common.ir.get_connector') as mock_get_connector:
        mock_get_es.return_value = {'status': 'finished', 'result': {'docs': [{'_id': '1', '_source': {'text': 'test'}}]}}, 200
        mock_get_connector.return_value = MagicMock(close=MagicMock())
        response = client.post("/retrieve_documents", json=body)
        result = json.loads(response.text).get('result')
        assert response.status_code == 200
        assert len(result.get('docs')) > 0

"""
def test_list_templates(client):
    response = client.get("/list_templates")
    result = json.loads(response.text).get('result')
    assert response.status_code == 200
    assert len(result.get('templates')) > 0


def test_predict(client):
    response = client.post("/predict", json=vision_query_template_call, headers=copy.deepcopy(TestMain.headers))
    result = json.loads(response.text).get('result')
    assert response.status_code == 200
    assert result.get('answer') != ""

def test_delete_prompt_template(client):
    response = client.post("/delete_prompt_template", json={"name": "test_system_query_v"})
    assert response.status_code == 200

    # Now call get_template to check if the template was deleted
    response = client.get("/get_template", query_string={"template_name": "test"})
    result = json.loads(response.text)
    assert response.status_code == 404
    assert result.get('error_message') == "Template 'test' not found"

def test_get_template_name(client):
    response = client.get("/get_template")
    result = json.loads(response.text)
    assert response.status_code == 400
    assert result.get('error_message') == "You must provide a 'template_name' param"

def test_get_models(client):
    response = client.get("/get_models", query_string={"platform": "azure", "model": "gpt-4o"})
    result = json.loads(response.text)
    assert response.status_code == 400
    assert result.get('error_message') == "You must provide only one parameter between 'platform', 'pool', 'zone' and 'model_type' param"

    response = client.get("/get_models", query_string={"platform": "azure"})
    result = json.loads(response.text).get('result')
    assert response.status_code == 200
    assert len(result.get('models')) > 0

def test_get_exception(client):
    with patch('main.LLMDeployment.parse_input') as mock_func:
        mock_func.side_effect = KeyError("Error")
        response = client.post("/predict", json=vision_query_template_call,
                               headers=copy.deepcopy(TestMain.headers))
        result = json.loads(response.text)
        assert response.status_code == 400
        assert result['error_message'] == "Error parsing input JSON."

"""