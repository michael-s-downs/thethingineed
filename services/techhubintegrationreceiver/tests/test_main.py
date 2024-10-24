### This code is property of the GGAO ###


# Native imports
import json
from unittest import mock
from unittest.mock import patch, MagicMock

# Installed imports
import pytest

# Custom imports
from main import app


@pytest.fixture
def client():
    with app.test_client() as client:
        yield client


def test_healthcheck(client):
    """Test the healthcheck endpoint."""
    response = client.get('/healthcheck')
    assert response.status_code == 200
    assert json.loads(response.data) == {'status': "ok"}
