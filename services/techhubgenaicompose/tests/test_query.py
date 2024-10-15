### This code is property of the GGAO ###

import pytest
from unittest.mock import MagicMock, patch
from compose.query import expansion
from common.errors.genaierrors import PrintableGenaiError


@pytest.mark.asyncio(loop_scope="session")
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



