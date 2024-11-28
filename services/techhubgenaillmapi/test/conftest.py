import pytest
from unittest.mock import MagicMock, patch

@pytest.fixture(autouse=True)
def patch_load_secrets():
    with patch("common.utils.load_secrets") as mock_function:
        mock_function.return_value = MagicMock()
        yield mock_function