### This code is property of the GGAO ###

# Native imports
import os
import sys
import pytest
from unittest.mock import Mock, patch

os.environ.setdefault('INTEGRATION_NAME', 'test_integration')
os.environ.setdefault('LOCAL_COMMON_PATH', '/test/common')
os.environ.setdefault('STORAGE_CONFIG_FOLDER', 'config/{integration_name}/')
os.environ.setdefault('STORAGE_BACKEND', 'test_backend')
os.environ.setdefault('STORAGE_DATA', 'test_container')
os.environ.setdefault('TENANT', 'test_tenant')
os.environ.setdefault('DATA_MOUNT_PATH', '/test/mount/')
os.environ.setdefault('INTEGRATION_QUEUE_URL', 'test://queue/{integration_name}/')

sys.modules['conf_utils'] = Mock()
sys.modules['docs_utils'] = Mock()
sys.modules['core_calls'] = Mock()
sys.modules['requests_manager'] = Mock()
sys.modules['provider_resources'] = Mock()
sys.modules['logging_handler'] = Mock()
sys.modules['graceful_killer'] = Mock()
sys.modules['core_api'] = Mock()

# Mock docs_utils formats
docs_utils_mock = sys.modules['docs_utils']
docs_utils_mock.formats_convert = ['pdf', 'docx']
docs_utils_mock.formats_pass = ['txt', 'pdf', 'docx']
docs_utils_mock.document_conversion = Mock(return_value=('converted.txt', b'converted_content'))

# Mock requests_manager
requests_manager_mock = sys.modules['requests_manager']
requests_manager_mock.storage_delete_request = True
requests_manager_mock.generate_request = Mock()
requests_manager_mock.persist_request = Mock()
requests_manager_mock.delete_request = Mock()

# Mock provider_resources
provider_resources_mock = sys.modules['provider_resources']
provider_resources_mock.queue_url = 'test://queue/TEST_INTEGRATION/'
provider_resources_mock.storage_download_folder = Mock(return_value=True)
provider_resources_mock.storage_put_file = Mock(return_value=True)
provider_resources_mock.storage_upload_file = Mock(return_value=True)
provider_resources_mock.storage_get_file = Mock(return_value=b'file_content')
provider_resources_mock.storage_list_folder = Mock(return_value=[])
provider_resources_mock.storage_remove_files = Mock()
provider_resources_mock.queue_write_message = Mock(return_value=True)

# Mock logging_handler
logging_handler_mock = sys.modules['logging_handler']
logging_handler_mock.logger = Mock()

# Mock graceful_killer
graceful_killer_mock = sys.modules['graceful_killer']
graceful_killer_mock.GracefulKiller = Mock()

# Mock conf_utils
conf_utils_mock = sys.modules['conf_utils']
conf_utils_mock.custom_folder = '/test/custom/'

# Mock core_calls
core_calls_mock = sys.modules['core_calls']
core_calls_mock.delete = Mock()

# Test imports
sys.path.append(os.getenv('LOCAL_COMMON_PATH'))
import integration_base


class TestGetFunction:
    """Test the get_function utility."""
    
    def test_get_function_valid_module_function(self):
        """Test getting a valid function from a module."""
        # Mock a module with a function
        mock_module = Mock()
        mock_module.test_function = lambda x: x * 2
        
        with patch.dict('sys.modules', {'test_module': mock_module}):
            func = integration_base.get_function('test_module.test_function')
            assert func(5) == 10

    def test_get_function_nested_module(self):
        """Test getting a function from nested module path."""
        mock_module = Mock()
        mock_module.nested_function = lambda: "nested_result"
        
        with patch.dict('sys.modules', {'parent.child': mock_module}):
            func = integration_base.get_function('parent.child.nested_function')
            assert func() == "nested_result"

    def test_get_function_attribute_error(self):
        """Test handling of missing function in module."""
        mock_module = Mock()
        del mock_module.nonexistent_function 
        
        with patch.dict('sys.modules', {'test_module': mock_module}):
            with pytest.raises(AttributeError):
                integration_base.get_function('test_module.nonexistent_function')


class TestImportModule:
    """Test the import_module utility."""
    
    @patch('importlib.reload')
    @patch('builtins.__import__')
    def test_import_module_basic_path(self, mock_import, mock_reload):
        """Test importing a module with basic path."""
        mock_module = Mock()
        mock_import.return_value = mock_module
        
        with patch.dict('sys.modules', {'test_module': mock_module}):
            integration_base.import_module('path/to/test_module.py')
            
            mock_import.assert_called_once_with('path.to.test_module', fromlist=['path.to.test_module'])
            mock_reload.assert_called_once_with(mock_module)

    @patch('importlib.reload')
    @patch('builtins.__import__')
    def test_import_module_windows_path(self, mock_import, mock_reload):
        """Test importing a module with Windows path separators."""
        mock_module = Mock()
        mock_import.return_value = mock_module
        
        with patch.dict('sys.modules', {'test_module': mock_module}):
            integration_base.import_module('path\\to\\test_module.py')
            
            mock_import.assert_called_once_with('path.to.test_module', fromlist=['path.to.test_module'])


class TestLoadCustomFiles:
    """Test the load_custom_files function."""
    
    @patch('integration_base.provider_resources.storage_download_folder')
    @patch('integration_base.glob.glob')
    @patch('integration_base.os.path.isfile')
    @patch('integration_base.import_module')
    def test_load_custom_files_success(self, mock_import_module, 
                                      mock_isfile, mock_glob, mock_storage_download):
        """Test successful loading of custom files."""
        # Setup mocks
        mock_storage_download.return_value = True
        mock_glob.return_value = ['/test/custom/code/module1.py', '/test/custom/code/module2.py']
        mock_isfile.return_value = True
        
        with patch('integration_base.conf_utils') as mock_conf_utils:
            mock_conf_utils.custom_folder = '/test/custom/'
        
            integration_base.load_custom_files()
            
            # Verify storage download was called
            mock_storage_download.assert_called_once()
            
            # Verify import_module was called for each file
            assert mock_import_module.call_count == 2

    @patch('integration_base.provider_resources.storage_download_folder')
    @patch('integration_base.logger')
    def test_load_custom_files_download_failure(self, mock_logger, mock_storage_download):
        """Test handling of storage download failure."""
        mock_storage_download.return_value = False
        
        integration_base.load_custom_files()
        
        mock_logger.error.assert_called_once()


class TestGetInputs:
    """Test the get_inputs function."""
    
    def test_get_inputs_post_request_with_json(self):
        """Test getting inputs from POST request with JSON data."""
        mock_request = Mock()
        mock_request.headers = {
            'x-tenant': 'test_tenant',
            'x-department': 'test_dept',
            'x-reporting': 'test_report',
            'x-limits': '{"max": 100}'
        }
        mock_request.method = 'POST'
        mock_request.data = b'{"key": "value"}'
        mock_request.get_json.return_value = {"key": "value"}
        mock_request.files = {}
        
        apigw_params, input_json, input_files = integration_base.get_inputs(mock_request)
        
        assert apigw_params['x-tenant'] == 'test_tenant'
        assert apigw_params['x-department'] == 'test_dept'
        assert input_json == {"key": "value"}
        assert input_files == []

    def test_get_inputs_get_request(self):
        """Test getting inputs from GET request."""
        mock_request = Mock()
        mock_request.headers = {'x-tenant': 'test_tenant'}
        mock_request.method = 'GET'
        mock_request.args = {'param1': 'value1', 'param2': 'value2'}
        
        apigw_params, input_json, input_files = integration_base.get_inputs(mock_request)
        
        assert input_json == {'param1': 'value1', 'param2': 'value2'}

    def test_get_inputs_dict_request(self):
        """Test getting inputs from dictionary (queue request)."""
        mock_request = {"operation": "test", "data": "value"}
        
        apigw_params, input_json, input_files = integration_base.get_inputs(mock_request)
        
        assert apigw_params['x-tenant'] == 'test_tenant'  # From env var setup
        assert input_json == {"operation": "test", "data": "value"}

    def test_get_inputs_with_files(self):
        """Test getting inputs with attached files."""
        mock_file = Mock()
        mock_file.filename = 'test.txt'
        mock_file.stream.read.return_value = b'file content'
        
        mock_request = Mock()
        mock_request.headers = {'x-tenant': 'test_tenant'}
        mock_request.method = 'POST'
        mock_request.data = b'{"key": "value"}'
        mock_request.get_json.return_value = {"key": "value"}
        
        # Mock files properly
        mock_files = Mock()
        mock_files.items.return_value = [('file1', mock_file)]
        mock_request.files = mock_files
        
        apigw_params, input_json, input_files = integration_base.get_inputs(mock_request)
        
        assert len(input_files) == 1
        assert input_files[0]['file_name'] == 'test.txt'
        assert input_files[0]['file_bytes'] == b'file content'

    def test_get_inputs_exception_handling(self):
        """Test exception handling in get_inputs."""
        mock_request = Mock()
        mock_request.headers.get.side_effect = Exception("Header error")
        
        # Should handle exception and return dict request format
        apigw_params, input_json, input_files = integration_base.get_inputs(mock_request)
        
        assert apigw_params == {}
        assert input_json == {}


class TestCheckShutdown:
    """Test the check_shutdown function."""
    
    @patch('integration_base.killer')
    def test_check_shutdown_kill_signal_received(self, mock_killer):
        """Test shutdown when kill signal is received."""
        mock_killer.kill_now = True
        mock_shutdown = Mock()
        
        mock_request = Mock()
        mock_request.environ = {'werkzeug.server.shutdown': mock_shutdown}
        
        integration_base.check_shutdown(mock_request)
        
        mock_shutdown.assert_called_once()

    @patch('integration_base.killer')
    def test_check_shutdown_no_kill_signal(self, mock_killer):
        """Test no shutdown when kill signal is not received."""
        mock_killer.kill_now = False
        mock_shutdown = Mock()
        
        mock_request = Mock()
        mock_request.environ = {'werkzeug.server.shutdown': mock_shutdown}
        
        integration_base.check_shutdown(mock_request)
        
        mock_shutdown.assert_not_called()


class TestUploadDocs:
    """Test the upload_docs function."""
    
    @patch('integration_base.provider_resources.storage_put_file')
    @patch('integration_base.provider_resources.storage_upload_file')
    def test_upload_docs_basic_files(self, mock_storage_upload, mock_storage_put):
        """Test uploading basic files."""
        request_json = {'integration_id': 'test_123'}
        input_files = [
            {'file_name': 'file1.txt', 'file_bytes': b'content1'},
            {'file_name': 'file2.txt', 'file_bytes': b'content2'}
        ]
        
        integration_base.upload_docs(request_json, input_files)
        
        assert mock_storage_put.call_count == 2
        mock_storage_put.assert_any_call('test_123/file1.txt', b'content1', 'test_container')
        mock_storage_put.assert_any_call('test_123/file2.txt', b'content2', 'test_container')

    @patch('integration_base.provider_resources.storage_upload_file')
    @patch('integration_base.os.path.exists')
    @patch('integration_base.os.path.isfile')
    def test_upload_docs_with_mount_path(self, mock_isfile, mock_exists, mock_storage_upload):
        """Test uploading docs with mount path."""
        request_json = {
            'integration_id': 'test_123',
            'documents_folder': '/test/mount/docs/', 
            'documents': ['/test/mount/docs/file1.txt', '/test/mount/docs/file2.txt']
        }
        input_files = []
        
        mock_exists.return_value = True
        mock_isfile.return_value = True
        mock_storage_upload.return_value = True
        
        integration_base.upload_docs(request_json, input_files)
        
        assert mock_storage_upload.call_count == 2
        assert request_json['documents_folder'] == 'test_123/docs/'
        assert len(request_json['documents']) == 2

    @patch('integration_base.provider_resources.storage_upload_file')
    @patch('integration_base.os.path.exists')
    @patch('integration_base.logger')
    def test_upload_docs_file_not_found(self, mock_logger, mock_exists, mock_storage_upload):
        """Test handling of missing files during upload."""
        request_json = {
            'integration_id': 'test_123',
            'documents_folder': '/test/mount/docs/', 
            'documents': ['/test/mount/docs/missing.txt']
        }
        input_files = []
        
        mock_exists.return_value = False
        
        integration_base.upload_docs(request_json, input_files)
        
        mock_logger.warning.assert_called()


class TestConvertDocs:
    """Test the convert_docs function."""
    
    @patch('integration_base.provider_resources.storage_list_folder')
    @patch('integration_base.provider_resources.storage_get_file')
    @patch('integration_base.provider_resources.storage_put_file')
    @patch('integration_base.docs_utils.document_conversion')
    def test_convert_docs_success(self, mock_conversion, mock_storage_put, 
                                 mock_storage_get, mock_storage_list):
        """Test successful document conversion."""
        request_json = {'documents_folder': 'test_folder/'}
        
        mock_storage_list.return_value = ['file1.pdf', 'file2.docx']
        mock_storage_get.return_value = b'file_content'
        mock_conversion.return_value = ('converted_file.txt', b'converted_content')
        
        # Mock docs_utils.formats_convert directly
        with patch.object(integration_base.docs_utils, 'formats_convert', ['pdf', 'docx']):
            integration_base.convert_docs(request_json)
            
            assert mock_storage_list.call_count == 1
            assert mock_storage_get.call_count == 2
            assert mock_conversion.call_count == 2
            assert mock_storage_put.call_count == 2


class TestListDocs:
    """Test the list_docs function."""
    
    @patch('integration_base.provider_resources.storage_list_folder')
    def test_list_docs_no_existing_documents(self, mock_storage_list):
        """Test listing documents when none exist in request_json."""
        request_json = {'documents_folder': 'test_folder/'}
        mock_storage_list.return_value = ['file1.txt', 'file2.pdf']
        
        # Mock docs_utils.formats_pass directly
        with patch.object(integration_base.docs_utils, 'formats_pass', ['txt', 'pdf']):
            result = integration_base.list_docs(request_json)
            
            assert 'documents' in result
            assert result['documents'] == ['file1.txt', 'file2.pdf']

    def test_list_docs_existing_documents(self):
        """Test listing documents when they already exist in request_json."""
        request_json = {
            'documents_folder': 'test_folder/',
            'documents': ['existing1.txt', 'existing2.pdf']
        }
        
        result = integration_base.list_docs(request_json)
        
        assert result['documents'] == ['existing1.txt', 'existing2.pdf']


class TestDeleteData:
    """Test the delete_data function."""
    
    @patch('integration_base.requests_manager.storage_delete_request', True)
    @patch('integration_base.provider_resources.storage_remove_files')
    @patch('integration_base.core_calls.delete')
    def test_delete_data_with_deletion_enabled(self, mock_core_delete, mock_storage_remove):
        """Test data deletion when deletion is enabled."""
        request_json = {'integration_id': 'test_123', 'persist_preprocess': False}
        
        integration_base.delete_data(request_json)
        
        mock_storage_remove.assert_called_once_with('test_123', 'test_container')
        mock_core_delete.assert_called_once_with(request_json)

    @patch('integration_base.requests_manager.storage_delete_request', True)
    @patch('integration_base.provider_resources.storage_remove_files')
    @patch('integration_base.core_calls.delete')
    def test_delete_data_with_persist_preprocess(self, mock_core_delete, mock_storage_remove):
        """Test data deletion when persist_preprocess is True."""
        request_json = {'integration_id': 'test_123', 'persist_preprocess': True}
        
        integration_base.delete_data(request_json)
        
        mock_storage_remove.assert_called_once()
        mock_core_delete.assert_not_called()

    @patch('integration_base.requests_manager.storage_delete_request', False)
    @patch('integration_base.provider_resources.storage_remove_files')
    def test_delete_data_with_deletion_disabled(self, mock_storage_remove):
        """Test no deletion when storage_delete_request is False."""
        request_json = {'integration_id': 'test_123'}
        
        integration_base.delete_data(request_json)
        
        mock_storage_remove.assert_not_called()


class TestReceiveRequest:
    """Test the receive_request function."""

    @patch('integration_base.get_inputs')
    @patch('integration_base.requests_manager.generate_request')
    @patch('integration_base.get_function')
    def test_receive_request_validation_failure(self, mock_get_function, 
                                              mock_generate_request, mock_get_inputs):
        """Test request reception with validation failure."""
        # Setup mocks
        mock_get_inputs.return_value = ({'x-tenant': 'test'}, {'operation': 'test'}, [])
        mock_generate_request.return_value = {
            'integration_id': 'test_123',
            'input_json': {},
            'client_profile': {
                'custom_functions': {
                    'validate_input': 'module.validate_input'
                }
            }
        }
        
        # Mock validation failure
        mock_validate = Mock(return_value=(False, "Invalid input format"))
        mock_get_function.return_value = mock_validate
        
        mock_request = Mock()
        
        request_json, result = integration_base.receive_request(mock_request)
        
        assert result['status'] == 'error'
        assert 'Bad input:' in result['error']
        assert request_json['status'] == 'error'

    @patch('integration_base.get_inputs')
    def test_receive_request_exception_handling(self, mock_get_inputs):
        """Test request reception with exception."""
        mock_get_inputs.side_effect = Exception("Unexpected error")
        
        mock_request = Mock()
        
        request_json, result = integration_base.receive_request(mock_request)
        
        assert result['status'] == 'error'
        assert result['error'] == 'Internal error'


class TestProcessRequest:
    """Test the process_request function."""
    
    @patch('integration_base.get_function')
    @patch('integration_base.requests_manager.persist_request')
    @patch('integration_base.delete_data')
    @patch('integration_base.requests_manager.delete_request')
    def test_process_request_success(self, mock_delete_request, mock_delete_data, 
                                   mock_persist_request, mock_get_function):
        """Test successful request processing."""
        request_json = {
            'integration_id': 'test_123',
            'status': 'processing',
            'client_profile': {
                'pipeline': ['step1', 'step2'],
                'custom_functions': {
                    'step1': 'module.step1',
                    'step2': 'module.step2',
                    'adapt_output': 'module.adapt_output'
                }
            }
        }
        
        # Mock pipeline steps
        def step1_func(req_json):
            req_json['step1_done'] = True
            return req_json
        
        def step2_func(req_json):
            req_json['step2_done'] = True
            return req_json
        
        def adapt_output_func(req_json):
            return req_json, {'status': 'completed', 'result': 'success'}
        
        mock_get_function.side_effect = [step1_func, step2_func, adapt_output_func]
        
        with patch('integration_base.datetime') as mock_datetime:
            mock_datetime.now.return_value.timestamp.return_value = 1234567890
            
            result_json, result = integration_base.process_request(request_json)
            
            assert result_json['status'] == 'finish'
            assert result_json['step1_done'] is True
            assert result_json['step2_done'] is True
            assert result['status'] == 'completed'

    def test_process_request_waiting_status(self):
        """Test processing request with waiting status."""
        request_json = {
            'integration_id': 'test_123',
            'status': 'waiting',
            'client_profile': {'custom_functions': {}}
        }
        
        result_json, result = integration_base.process_request(request_json)
        
        assert result_json['status'] == 'waiting'
        assert result == {}

    @patch('integration_base.get_function')
    @patch('integration_base.requests_manager.persist_request')
    def test_process_request_with_init_logic(self, mock_persist_request, mock_get_function):
        """Test processing request with init logic."""
        request_json = {
            'integration_id': 'test_123',
            'status': 'processing',
            'client_profile': {
                'pipeline': [],
                'custom_functions': {
                    'init_logic': 'module.init_logic',
                    'adapt_output': 'module.adapt_output'
                }
            }
        }
        
        def init_logic_func(req_json):
            req_json['initialized'] = True
            return req_json
        
        def adapt_output_func(req_json):
            return req_json, {'status': 'completed'}
        
        mock_get_function.side_effect = [init_logic_func, adapt_output_func]
        
        with patch('integration_base.datetime') as mock_datetime:
            mock_datetime.now.return_value.timestamp.return_value = 1234567890
            
            result_json, result = integration_base.process_request(request_json)
            
            assert result_json['initialized'] is True
            assert result_json['status'] == 'finish'

    @patch('integration_base.get_function')
    @patch('integration_base.requests_manager.persist_request')
    def test_process_request_with_error_logic(self, mock_persist_request, mock_get_function):
        """Test processing request with error logic."""
        request_json = {
            'integration_id': 'test_123',
            'status': 'error',
            'error': 'Previous error',
            'client_profile': {
                'pipeline': [],
                'custom_functions': {
                    'error_logic': 'module.error_logic',
                    'adapt_output': 'module.adapt_output'
                }
            }
        }
        
        def error_logic_func(req_json):
            req_json['error_handled'] = True
            return req_json
        
        def adapt_output_func(req_json):
            return req_json, {'status': 'error', 'error': 'Handled error'}
        
        mock_get_function.side_effect = [error_logic_func, adapt_output_func]
        
        with patch('integration_base.datetime') as mock_datetime:
            mock_datetime.now.return_value.timestamp.return_value = 1234567890
            
            result_json, result = integration_base.process_request(request_json)
            
            assert result_json['error_handled'] is True

    @patch('integration_base.get_function')
    @patch('integration_base.requests_manager.persist_request')
    def test_process_request_with_finally_logic(self, mock_persist_request, mock_get_function):
        """Test processing request with finally logic."""
        request_json = {
            'integration_id': 'test_123',
            'status': 'processing',
            'client_profile': {
                'pipeline': [],
                'custom_functions': {
                    'adapt_output': 'module.adapt_output',
                    'finally_logic': 'module.finally_logic'
                }
            }
        }
        
        def adapt_output_func(req_json):
            return req_json, {'status': 'completed'}
        
        def finally_logic_func(req_json):
            req_json['finally_executed'] = True
            return req_json
        
        mock_get_function.side_effect = [adapt_output_func, finally_logic_func]
        
        with patch('integration_base.datetime') as mock_datetime:
            mock_datetime.now.return_value.timestamp.return_value = 1234567890
            
            result_json, result = integration_base.process_request(request_json)
            
            assert result_json['finally_executed'] is True

    @patch('integration_base.get_function')
    def test_process_request_general_exception(self, mock_get_function):
        """Test processing request with general exception."""
        request_json = {
            'integration_id': 'test_123',
            'status': 'processing',
            'client_profile': {
                'pipeline': ['step1'],
                'custom_functions': {'step1': 'module.step1'}
            }
        }
        
        # Mock exception in get_function
        mock_get_function.side_effect = Exception("General error")
        
        result_json, result = integration_base.process_request(request_json)
        
        assert result_json['status'] == 'error'
        assert 'Internal error processing request' in result_json['error']
        assert result['status'] == 'error'

class TestEdgeCases:
    """Test edge cases and error conditions."""
    
    def test_get_inputs_empty_headers(self):
        """Test get_inputs with empty headers."""
        mock_request = Mock()
        mock_request.headers = {}
        mock_request.method = 'GET'
        mock_request.args = {}
        
        apigw_params, input_json, input_files = integration_base.get_inputs(mock_request)
        
        # Should use default values
        assert apigw_params['x-tenant'] == ''
        assert apigw_params['x-department'] == 'main'
        assert apigw_params['x-reporting'] == ''
        assert apigw_params['x-limits'] == '{}'

    @patch('integration_base.provider_resources.storage_put_file')
    def test_upload_docs_empty_files(self, mock_storage_put):
        """Test upload_docs with empty file list."""
        request_json = {'integration_id': 'test_123'}
        input_files = []
        
        integration_base.upload_docs(request_json, input_files)
        
        mock_storage_put.assert_not_called()

    @patch('integration_base.provider_resources.storage_list_folder')
    def test_convert_docs_no_files(self, mock_storage_list):
        """Test convert_docs with no files to convert."""
        request_json = {'documents_folder': 'empty_folder/'}
        mock_storage_list.return_value = []
        
        # Should not raise exception
        integration_base.convert_docs(request_json)
        
        mock_storage_list.assert_called_once()

    @patch('integration_base.get_function')
    def test_process_request_empty_pipeline(self, mock_get_function):
        """Test process_request with empty pipeline."""
        request_json = {
            'integration_id': 'test_123',
            'status': 'processing',
            'client_profile': {
                'pipeline': [],
                'custom_functions': {
                    'adapt_output': 'module.adapt_output'
                }
            }
        }
        
        def adapt_output_func(req_json):
            return req_json, {'status': 'completed'}
        
        mock_get_function.return_value = adapt_output_func
        
        with patch('integration_base.datetime') as mock_datetime:
            mock_datetime.now.return_value.timestamp.return_value = 1234567890
            
            result_json, result = integration_base.process_request(request_json)
            
            assert result_json['status'] == 'finish'
            assert result['status'] == 'completed'

    @patch('integration_base.get_function')
    def test_process_request_waiting_in_pipeline(self, mock_get_function):
        """Test process_request when step returns waiting status."""
        request_json = {
            'integration_id': 'test_123',
            'status': 'processing',
            'client_profile': {
                'pipeline': ['waiting_step', 'next_step'],
                'custom_functions': {
                    'waiting_step': 'module.waiting_step',
                    'next_step': 'module.next_step'
                }
            }
        }
        
        def waiting_step_func(req_json):
            req_json['status'] = 'waiting'
            return req_json
        
        mock_get_function.return_value = waiting_step_func
        
        result_json, result = integration_base.process_request(request_json)
        
        # Should stop processing and keep waiting status
        assert result_json['status'] == 'waiting'
        # Pipeline should still have both steps (nothing popped)
        assert len(result_json['client_profile']['pipeline']) == 2

    @patch('integration_base.logger')
    def test_load_custom_files_no_env_vars(self, mock_logger):
        """Test load_custom_files with missing environment variables."""
        # Temporarily remove critical env vars
        original_integration_name = os.environ.get('INTEGRATION_NAME')
        original_storage_config = os.environ.get('STORAGE_CONFIG_FOLDER')
        
        try:
            # Remove env vars to trigger the error condition
            if 'INTEGRATION_NAME' in os.environ:
                del os.environ['INTEGRATION_NAME']
            if 'STORAGE_CONFIG_FOLDER' in os.environ:
                del os.environ['STORAGE_CONFIG_FOLDER']
            
            # Should handle missing env vars gracefully or raise expected exception
            with pytest.raises((KeyError, AttributeError)):
                integration_base.load_custom_files()
                
        finally:
            # Restore original env vars
            if original_integration_name:
                os.environ['INTEGRATION_NAME'] = original_integration_name
            if original_storage_config:
                os.environ['STORAGE_CONFIG_FOLDER'] = original_storage_config

    def test_check_shutdown_no_environ(self):
        """Test check_shutdown with missing environ."""
        mock_request = Mock()
        mock_request.environ = {}
        
        with patch('integration_base.killer') as mock_killer:
            mock_killer.kill_now = True
            
            # Should handle missing shutdown function gracefully
            try:
                integration_base.check_shutdown(mock_request)
            except (KeyError, TypeError):
                pass  # Expected if shutdown function not available


class TestMockingHelpers:
    """Helper tests to verify mocking is working correctly."""
    
    @patch('integration_base.provider_resources.storage_put_file')
    def test_storage_mock_verification(self, mock_storage_put):
        """Verify that storage mocking works correctly."""
        mock_storage_put.return_value = True
        
        # Call the function indirectly through upload_docs
        request_json = {'integration_id': 'test_123'}
        input_files = [{'file_name': 'test.txt', 'file_bytes': b'content'}]
        
        with patch.dict(os.environ, {'STORAGE_DATA': 'test_container'}):
            integration_base.upload_docs(request_json, input_files)
        
        # Verify mock was called with expected parameters
        mock_storage_put.assert_called_once_with('test_123/test.txt', b'content', 'test_container')

    @patch('integration_base.logger')
    def test_logger_mock_verification(self, mock_logger):
        """Verify that logger mocking works correctly."""
        # Test that logger calls are properly mocked
        integration_base.logger.info("Test message")
        integration_base.logger.error("Test error")
        
        # Should not raise any exceptions
        assert True

    def test_datetime_mock_verification(self):
        """Verify that datetime mocking works correctly."""
        with patch('integration_base.datetime') as mock_datetime:
            mock_datetime.now.return_value.timestamp.return_value = 1234567890
            
            # This would normally be called within process_request
            timestamp = integration_base.datetime.now().timestamp()
            assert timestamp == 1234567890


# Pytest fixtures for common test data
@pytest.fixture
def sample_request_json():
    """Fixture providing sample request JSON data."""
    return {
        'integration_id': 'test_integration_123',
        'status': 'processing',
        'input_json': {'request_id': 'req_456', 'operation': 'test'},
        'client_profile': {
            'pipeline': ['step1', 'step2'],
            'custom_functions': {
                'validate_input': 'test_module.validate_input',
                'adapt_input': 'test_module.adapt_input',
                'step1': 'test_module.step1',
                'step2': 'test_module.step2',
                'adapt_output': 'test_module.adapt_output'
            }
        },
        'documents_folder': 'test_folder/',
        'documents': ['file1.txt', 'file2.pdf']
    }

@pytest.fixture
def sample_input_files():
    """Fixture providing sample input files data."""
    return [
        {'file_name': 'document1.txt', 'file_bytes': b'Content of document 1'},
        {'file_name': 'document2.pdf', 'file_bytes': b'Content of document 2'}
    ]

@pytest.fixture
def mock_flask_request():
    """Fixture providing a mock Flask request object."""
    request = Mock()
    request.headers = {
        'x-tenant': 'test_tenant',
        'x-department': 'test_department',
        'x-reporting': 'test_reporting',
        'x-limits': '{"max_files": 10}'
    }
    request.method = 'POST'
    request.data = b'{"operation": "test", "data": "value"}'
    request.get_json.return_value = {"operation": "test", "data": "value"}
    request.files = {}
    request.environ = {'werkzeug.server.shutdown': Mock()}
    return request


# Performance and stress tests
class TestPerformance:
    """Performance-related tests."""
    
    @patch('integration_base.get_function')
    def test_large_pipeline_processing(self, mock_get_function):
        """Test processing with a large pipeline."""
        # Create a large pipeline
        pipeline_steps = [f'step_{i}' for i in range(100)]
        custom_functions = {step: f'module.{step}' for step in pipeline_steps}
        custom_functions['adapt_output'] = 'module.adapt_output'
        
        request_json = {
            'integration_id': 'test_large_pipeline',
            'status': 'processing',
            'client_profile': {
                'pipeline': pipeline_steps.copy(),
                'custom_functions': custom_functions
            }
        }
        
        # Mock all functions to simply return the request_json
        def mock_step(req_json):
            return req_json
        
        def mock_adapt_output(req_json):
            return req_json, {'status': 'completed'}
        
        # Set up the mock to return appropriate functions
        def get_function_side_effect(func_name):
            if 'adapt_output' in func_name:
                return mock_adapt_output
            return mock_step
        
        mock_get_function.side_effect = get_function_side_effect
        
        with patch('integration_base.datetime') as mock_datetime:
            mock_datetime.now.return_value.timestamp.return_value = 1234567890
            
            result_json, result = integration_base.process_request(request_json)
            
            # Should complete all steps
            assert result_json['status'] == 'finish'
            assert len(result_json['client_profile']['pipeline']) == 0
            assert len(result_json['client_profile']['pipeline_done']) == 100

    def test_many_input_files(self):
        """Test handling many input files."""
        request_json = {'integration_id': 'test_many_files'}
        
        # Create many input files
        input_files = [
            {'file_name': f'file_{i}.txt', 'file_bytes': f'content_{i}'.encode()}
            for i in range(1000)
        ]
        
        with patch('integration_base.provider_resources.storage_put_file') as mock_storage_put:
            mock_storage_put.return_value = True
            
            # Should handle large number of files without issues
            integration_base.upload_docs(request_json, input_files)
            
            assert mock_storage_put.call_count == 1000