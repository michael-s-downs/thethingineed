### This code is property of the GGAO ###

import pytest
from unittest.mock import MagicMock, patch
from common.errors.genaierrors import PrintableGenaiError, GenaiError
from basemanager import AbstractManager  # Update with your actual module path

class TestAbstractManager(AbstractManager):
    # This subclass is used to create an instance of AbstractManager for testing purposes.
    pass

@pytest.fixture
def manager():
    return TestAbstractManager()

def test_get_defaults_with_existing_param(manager, mocker):
    defaults_dict = {'param1': 'value1'}
    param_name = 'param1'
    
    param_value = manager.get_defaults(defaults_dict, param_name)
    
    assert param_value == 'value1'

def test_get_defaults_with_missing_param(manager, mocker):
    defaults_dict = {}
    param_name = 'param1'
    
    with pytest.raises(PrintableGenaiError, match="Default param not found. Key: <param1>"):
        manager.get_defaults(defaults_dict, param_name)

def test_get_param_with_existing_param(manager, mocker):
    params_dict = {'param1': 'value1'}
    defaults_dict = {'param1': 'default_value1'}
    param_name = 'param1'
    
    param_value = manager.get_param(params_dict, param_name, str, defaults_dict)
    
    assert param_value == 'value1'

def test_get_param_with_missing_param(manager, mocker):
    params_dict = {}
    defaults_dict = {'param1': 'default_value1'}
    param_name = 'param1'
    
    param_value = manager.get_param(params_dict, param_name, str, defaults_dict)
    
    assert param_value == 'default_value1'

def test_get_param_with_wrong_type(manager, mocker):
    params_dict = {'param1': 123}
    defaults_dict = {'param1': 'default_value1'}
    param_name = 'param1'
    
    param_value = manager.get_param(params_dict, param_name, str, defaults_dict)
    
    assert param_value == 'default_value1'

def test_get_param_mandatory_with_missing_param(manager, mocker):
    params_dict = {}
    defaults_dict = {'param1': 'default_value1'}
    param_name = 'param1'
    
    with pytest.raises(PrintableGenaiError, match="Mandatory param <query> not found in template params"):
        manager.get_param(params_dict, param_name, str, defaults_dict, mandatory=True)


def test_raise_genai_error(manager):
    with pytest.raises(GenaiError, match="Some error message"):
        manager.raise_GenaiError(500, "Some error message")

def test_raise_printable_genai_error(manager):
    with pytest.raises(PrintableGenaiError, match="Some error message"):
        manager.raise_PrintableGenaiError(404, "Some error message")


