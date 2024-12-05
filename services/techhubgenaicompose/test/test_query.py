### This code is property of the GGAO ###

import os
os.environ['URL_LLM'] = "test_url"
os.environ['URL_RETRIEVE'] = "test_retrieve"
import pytest
from unittest.mock import MagicMock, patch
from compose.query import expansion, filter_query, reformulate_query
from common.errors.genaierrors import PrintableGenaiError


@patch('compose.query.ExpansionFactory')
def test_expansion(mock_expansion_factory):
    # Arrange
    exp_type = 'some_type'
    params = {'param1': 'value1'}
    query = 'some query'
    actions_confs = {'action1': 'config1'}
    
    mock_exp = MagicMock()
    mock_exp.process.return_value = 'expected result'
    mock_expansion_factory.return_value = mock_exp

    # Act
    result = expansion(exp_type, params, query, actions_confs)

    # Assert
    mock_expansion_factory.assert_called_once_with(exp_type)
    mock_exp.process.assert_called_once_with(query, params, actions_confs)
    assert result == 'expected result'

def test_expansion_invalid_type():
    # Arrange
    exp_type = 'invalid_type'
    params = {'param1': 'value1'}
    query = 'some query'
    actions_confs = {'action1': 'config1'}

    # Act & Assert
    with pytest.raises(PrintableGenaiError):
        expansion(exp_type, params, query, actions_confs)


@patch('compose.query.FilterFactory')
def test_filter_query(mock_filter_factory):
    # Arrange
    exp_type = 'some_type'
    params = {'param1': 'value1'}
    query = 'some query'
    actions_confs = {'action1': 'config1'}
    
    mock_exp = MagicMock()
    mock_exp.process.return_value = 'expected result'
    mock_filter_factory.return_value = mock_exp

    # Act
    result = filter_query(exp_type, params, query, actions_confs)

    # Assert
    mock_filter_factory.assert_called_once_with(exp_type)
    mock_exp.process.assert_called_once_with(query, params, actions_confs)
    assert result == 'expected result'

def test_filter_invalid_type():
    # Arrange
    exp_type = 'invalid_type'
    params = {'param1': 'value1'}
    query = 'some query'
    actions_confs = {'action1': 'config1'}

    # Act & Assert
    with pytest.raises(PrintableGenaiError):
        filter_query(exp_type, params, query, actions_confs)


@patch('compose.query.ReformulateFactory')
def test_reformulate_query(mock_reformulate_factory):
    # Arrange
    exp_type = 'some_type'
    params = {'param1': 'value1'}
    query = 'some query'
    actions_confs = {'action1': 'config1'}
    
    mock_exp = MagicMock()
    mock_exp.process.return_value = 'expected result'
    mock_reformulate_factory.return_value = mock_exp

    # Act
    result = reformulate_query(exp_type, params, query, actions_confs)

    # Assert
    mock_reformulate_factory.assert_called_once_with(exp_type)
    mock_exp.process.assert_called_once_with(query, params, actions_confs)
    assert result == 'expected result'

def test_reformulate_invalid_type():
    # Arrange
    exp_type = 'invalid_type'
    params = {'param1': 'value1'}
    query = 'some query'
    actions_confs = {'action1': 'config1'}

    # Act & Assert
    with pytest.raises(PrintableGenaiError):
        filter_query(exp_type, params, query, actions_confs)
