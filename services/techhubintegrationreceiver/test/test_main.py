### This code is property of the GGAO ###

# Native imports
import os
import sys
import json
from unittest import mock
from functools import wraps

# Installed imports
import pytest
from dotenv import load_dotenv

test_env_vars = {
    'INTEGRATION_NAME': 'test_integration',
    'LOCAL_COMMON_PATH': '/mock/path',
    'QUEUE_URL': 'test_queue_url',
    'STORAGE_URL': 'test_storage_url',
    'DEBUG': 'false',
    'ENVIRONMENT': 'test',
}

local_common_path = os.getenv('LOCAL_COMMON_PATH', '/mock/path')
if local_common_path not in sys.path:
    sys.path.append(local_common_path)

def setup_comprehensive_mocks():
    """Setup all necessary mocks before importing main module."""
    
    # Create mock objects
    mock_integration_base = mock.MagicMock()
    mock_logger = mock.MagicMock()
    mock_provider_resources = mock.MagicMock()
    mock_requests_manager = mock.MagicMock()
    
    # Setup provider_resources mock methods
    mock_provider_resources.queue_write_message = mock.MagicMock(return_value=True)
    mock_provider_resources.storage_download_folder = mock.MagicMock(return_value=True)
    mock_provider_resources.storage_upload_file = mock.MagicMock(return_value=True)
    mock_provider_resources.storage_put_file = mock.MagicMock(return_value=True)
    mock_provider_resources.storage_list_folder = mock.MagicMock(return_value=[])
    mock_provider_resources.queue_url = "test_queue_url"
    
    # Setup requests_manager mock
    mock_requests_manager.storage_delete_request = True
    
    # Setup functions from integration_base
    mock_load_custom_files = mock.MagicMock()
    mock_receive_request = mock.MagicMock()
    mock_process_request = mock.MagicMock()
    mock_check_shutdown = mock.MagicMock()
    
    # Set default return values
    mock_receive_request.return_value = (
        {'integration_id': 'test_123', 'status': 'processing'}, 
        {'status': 'processing'}
    )
    
    mock_process_request.return_value = (
        {'integration_id': 'test_123', 'status': 'completed'}, 
        {'status': 'completed'}
    )
    
    # Setup integration_base mock attributes
    mock_integration_base.logger = mock_logger
    mock_integration_base.provider_resources = mock_provider_resources
    mock_integration_base.requests_manager = mock_requests_manager
    mock_integration_base.load_custom_files = mock_load_custom_files
    mock_integration_base.receive_request = mock_receive_request
    mock_integration_base.process_request = mock_process_request
    mock_integration_base.check_shutdown = mock_check_shutdown
    
    # Mock the modules in sys.modules
    sys.modules['integration_base'] = mock_integration_base
    sys.modules['provider_resources'] = mock_provider_resources
    sys.modules['requests_manager'] = mock_requests_manager
    
    return {
        'integration_base': mock_integration_base,
        'logger': mock_logger,
        'provider_resources': mock_provider_resources,
        'requests_manager': mock_requests_manager,
        'load_custom_files': mock_load_custom_files,
        'receive_request': mock_receive_request,
        'process_request': mock_process_request,
        'check_shutdown': mock_check_shutdown
    }

# Setup mocks
global_mocks = setup_comprehensive_mocks()

# Import main module with environment variables mocked
with mock.patch.dict(os.environ, test_env_vars):
    from main import app


@pytest.fixture
def client():
    """Create a test client for the Flask app."""
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client


@pytest.fixture(autouse=True)
def mock_env_vars():
    """Mock environment variables for all tests."""
    with mock.patch.dict(os.environ, test_env_vars):
        yield


# Decorator to apply mocks to individual tests
def mock_resources(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        with mock.patch('main.logger', global_mocks['logger']) as mock_logger, \
             mock.patch('main.provider_resources', global_mocks['provider_resources']) as mock_provider_resources, \
             mock.patch('main.receive_request', global_mocks['receive_request']) as mock_receive_request, \
             mock.patch('main.process_request', global_mocks['process_request']) as mock_process_request, \
             mock.patch('main.check_shutdown', global_mocks['check_shutdown']) as mock_check_shutdown, \
             mock.patch('main.load_custom_files', global_mocks['load_custom_files']) as mock_load_custom_files:
            
            # Reset mock call counts for this test
            mock_logger.reset_mock()
            mock_provider_resources.reset_mock()
            mock_receive_request.reset_mock()
            mock_process_request.reset_mock()
            mock_check_shutdown.reset_mock()
            mock_load_custom_files.reset_mock()
            
            # Reset any side_effect that might have been set by previous tests
            mock_receive_request.side_effect = None
            mock_process_request.side_effect = None
            mock_check_shutdown.side_effect = None
            mock_load_custom_files.side_effect = None
            
            # Set up default return values
            mock_provider_resources.queue_write_message.return_value = True
            mock_receive_request.return_value = (
                {'integration_id': 'test_123', 'status': 'processing'}, 
                {'status': 'processing'}
            )
            mock_process_request.return_value = (
                {'integration_id': 'test_123', 'status': 'completed'}, 
                {'status': 'completed'}
            )
            
            # Call function with all mocks
            return func(*args, **kwargs,
                       mock_logger=mock_logger,
                       mock_provider_resources=mock_provider_resources,
                       mock_receive_request=mock_receive_request,
                       mock_process_request=mock_process_request,
                       mock_check_shutdown=mock_check_shutdown,
                       mock_load_custom_files=mock_load_custom_files)
    
    return wrapper


@mock_resources
def test_healthcheck(client, **test_mocks):
    """Test the healthcheck endpoint."""
    response = client.get("/healthcheck")

    assert response.status_code == 200
    data = json.loads(response.data)
    assert data.get('status') == "ok"


@mock_resources
def test_killcheck(client, **test_mocks):
    """Test the killcheck endpoint."""
    response = client.get("/killcheck")

    assert response.status_code == 200
    data = json.loads(response.data)
    assert data.get('status') == "ok"
    
    # Verify check_shutdown was called
    test_mocks['mock_check_shutdown'].assert_called_once()


@mock_resources
def test_reloadconfig(client, **test_mocks):
    """Test the reloadconfig endpoint."""
    response = client.get("/reloadconfig")

    assert response.status_code == 200
    data = json.loads(response.data)
    assert data.get('status') == "ok"
    
    # Verify load_custom_files was called
    test_mocks['mock_load_custom_files'].assert_called_once()


class TestProcessEndpoints:
    headers = {
        'x-tenant': "tenant", 
        'x-department': "department", 
        'x-reporting': "report",
        'Content-Type': 'application/json'
    }
    message = {
        'operation': "indexing", 
        'index': "test_index", 
        'documents_metadata': {
            'doc1': {"content_binary": "dGVzdA=="}
        }
    }

    @mock_resources
    def test_process_async_post_ok(self, client, **test_mocks):
        """Test the process-async endpoint with POST method."""
        response = client.post('/process-async', headers=self.headers, json=self.message)

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data.get('status') == "processing"
        
        # Verify functions were called
        test_mocks['mock_receive_request'].assert_called_once()
        test_mocks['mock_check_shutdown'].assert_called_once()

    @mock_resources
    def test_process_async_get_ok(self, client, **test_mocks):
        """Test the process-async endpoint with GET method."""
        response = client.get('/process-async', headers=self.headers)

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data.get('status') == "processing"

    @mock_resources
    def test_process_post_ok(self, client, **test_mocks):
        """Test the process endpoint with POST method."""
        response = client.post('/process', headers=self.headers, json=self.message)

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data.get('status') == "processing"

    @mock_resources
    def test_process_get_ok(self, client, **test_mocks):
        """Test the process endpoint with GET method."""
        response = client.get('/process', headers=self.headers)

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data.get('status') == "processing"

    @mock_resources
    def test_process_sync_post_ok(self, client, **test_mocks):
        """Test the process-sync endpoint with POST method."""
        response = client.post('/process-sync', headers=self.headers, json=self.message)

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data.get('status') == "completed"
        
        # Verify both receive_request and process_request were called
        test_mocks['mock_receive_request'].assert_called_once()
        test_mocks['mock_process_request'].assert_called_once()

    @mock_resources
    def test_process_sync_get_ok(self, client, **test_mocks):
        """Test the process-sync endpoint with GET method."""
        response = client.get('/process-sync', headers=self.headers)

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data.get('status') == "completed"

    @mock_resources
    def test_process_queue_error(self, client, **test_mocks):
        """Test the process endpoint with queue write error."""
        # Override the mock for this specific test
        test_mocks['mock_provider_resources'].queue_write_message.return_value = False

        response = client.post('/process', headers=self.headers, json=self.message)

        assert response.status_code == 400
        data = json.loads(response.data)
        assert data.get('status') == "error"
        assert data.get('error') == "Internal error"

    @mock_resources
    def test_process_receive_request_error(self, client, **test_mocks):
        """Test the process endpoint when receive_request returns error status."""
        # Override the mock for this specific test
        test_mocks['mock_receive_request'].return_value = (
            {'integration_id': 'test_123', 'status': 'error'}, 
            {'status': 'error', 'error': 'Bad input: Invalid format'}
        )

        response = client.post('/process', headers=self.headers, json=self.message)

        assert response.status_code == 400
        data = json.loads(response.data)
        assert data.get('status') == "error"
        assert data.get('error') == "Bad input: Invalid format"

    @mock_resources
    def test_process_sync_error(self, client, **test_mocks):
        """Test the process-sync endpoint with processing error."""
        # Override the mock for this specific test
        test_mocks['mock_process_request'].return_value = (
            {'integration_id': 'test_123', 'status': 'error'}, 
            {'status': 'error', 'error': 'Processing failed'}
        )

        response = client.post('/process-sync', headers=self.headers, json=self.message)

        assert response.status_code == 400
        data = json.loads(response.data)
        assert data.get('status') == "error"
        assert data.get('error') == "Processing failed"

    @mock_resources
    def test_process_exception_handling(self, client, **test_mocks):
        """Test the process endpoint with unexpected exception."""
        with mock.patch('main.receive_request') as isolated_mock_receive_request:
            isolated_mock_receive_request.side_effect = Exception("Unexpected error")
            
            try:
                response = client.post('/process', headers=self.headers, json=self.message)
                assert response.status_code in [400, 500]
            except Exception as e:
                assert "Unexpected error" in str(e)


class TestEndpointRouting:
    """Test that all endpoints are properly routed."""
    
    @mock_resources
    def test_all_endpoints_exist(self, client, **test_mocks):
        """Test that all expected endpoints exist and return valid responses."""
        endpoints = [
            ('/healthcheck', 'GET'),
            ('/killcheck', 'GET'),
            ('/reloadconfig', 'GET'),
            ('/process', 'GET'),
            ('/process', 'POST'),
            ('/process-async', 'GET'),
            ('/process-async', 'POST'),
            ('/process-sync', 'GET'),
            ('/process-sync', 'POST'),
        ]
        
        headers = {'Content-Type': 'application/json'}
        
        for endpoint, method in endpoints:
            try:
                if method == 'GET':
                    response = client.get(endpoint, headers=headers)
                else:
                    response = client.post(endpoint, json={}, headers=headers)
                
                assert response.status_code != 404, f"Endpoint {method} {endpoint} not found"
                assert response.status_code in [200, 400, 401, 403, 500], f"Unexpected status code for {method} {endpoint}"
                
            except Exception as e:
                pytest.fail(f"Exception occurred when testing {method} {endpoint}: {str(e)}")


class TestMockVerification:
    """Test that mocks are properly called."""
    
    @mock_resources
    def test_receive_request_called(self, client, **test_mocks):
        """Test that receive_request is called for process endpoints."""
        headers = {'Content-Type': 'application/json'}
        
        try:
            response = client.post('/process', json={}, headers=headers)
            # Verify the mock was called regardless of response
            test_mocks['mock_receive_request'].assert_called_once()
        except Exception as e:
            if test_mocks['mock_receive_request'].called:
                pass
            else:
                pytest.fail(f"receive_request was not called due to exception: {str(e)}")

    @mock_resources
    def test_process_request_called_sync(self, client, **test_mocks):
        """Test that process_request is called for process-sync endpoint."""
        headers = {'Content-Type': 'application/json'}
        
        try:
            response = client.post('/process-sync', json={}, headers=headers)
            # Verify the mock was called
            test_mocks['mock_process_request'].assert_called_once()
        except Exception as e:
            if test_mocks['mock_process_request'].called:
                pass
            else:
                pytest.fail(f"process_request was not called due to exception: {str(e)}")

    @mock_resources
    def test_check_shutdown_called(self, client, **test_mocks):
        """Test that check_shutdown is called for process endpoints."""
        headers = {'Content-Type': 'application/json'}
        
        try:
            response = client.post('/process', json={}, headers=headers)
            # Verify the mock was called
            test_mocks['mock_check_shutdown'].assert_called_once()
        except Exception as e:
            if test_mocks['mock_check_shutdown'].called:
                pass
            else:
                pytest.fail(f"check_shutdown was not called due to exception: {str(e)}")

    @mock_resources
    def test_load_custom_files_called(self, client, **test_mocks):
        """Test that load_custom_files is called for reloadconfig endpoint."""
        try:
            response = client.get('/reloadconfig')
            # Verify the mock was called
            test_mocks['mock_load_custom_files'].assert_called_once()
        except Exception as e:
            if test_mocks['mock_load_custom_files'].called:
                pass
            else:
                pytest.fail(f"load_custom_files was not called due to exception: {str(e)}")


def test_environment_variables_are_mocked():
    """Test that environment variables are properly mocked."""
    assert os.getenv('INTEGRATION_NAME') == 'test_integration'
    assert os.getenv('LOCAL_COMMON_PATH') == '/mock/path'


def test_mocks_are_setup():
    """Test that all mocks are properly set up."""
    assert 'integration_base' in sys.modules
    assert 'provider_resources' in sys.modules
    assert 'requests_manager' in sys.modules
    
    assert hasattr(sys.modules['integration_base'], 'logger')
    assert hasattr(sys.modules['integration_base'], 'provider_resources')
    assert hasattr(sys.modules['provider_resources'], 'queue_write_message')


def test_app_is_imported():
    """Test that the Flask app was imported successfully."""
    assert app is not None
    assert hasattr(app, 'test_client')
    assert hasattr(app, 'config')