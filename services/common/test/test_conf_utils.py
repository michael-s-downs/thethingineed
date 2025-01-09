### This code is property of the GGAO ###

import os
import json
import pytest
from unittest.mock import patch, mock_open, MagicMock

from conf_utils import (
    get_type,
    document_types,
    document_types_alias,
    find_type,
    get_profile,
    get_model,
)

# Mock global paths
custom_folder = "/mocked/path/"
types_folder = custom_folder + "types_config/"
profiles_folder = custom_folder + "profiles/"
profiles_map_path = custom_folder + "profiles_map.json"
models_map_path = custom_folder + "models_map.json"

# Sample data for mocking
mock_types_files = ["type1.json", "type2.json"]
mock_types_config = {
    "type1": {"_client_alias": ["alias1", "alias2"]},
    "type2": {"_client_alias": ["alias3"]},
}
mock_profiles_map = {"department1": "profile1", "tenant1": "profile2"}
mock_profiles_config = {"profile_name": "profile1", "some_key": "value"}
mock_models_map = {"model1": {"config_key": "config_value"}}


@pytest.fixture
def setup_env(monkeypatch):
    monkeypatch.setenv("INTEGRATION_NAME", "integration_test")
    monkeypatch.setenv("DEFAULT_PROFILE", "default")


@patch("os.listdir", return_value=mock_types_files)
def test_document_types(mock_listdir):
    types = document_types(folder_path=types_folder)
    assert types == ["type1", "type2"]


@patch("os.listdir", return_value=mock_types_files)
@patch("conf_utils.get_type", side_effect=lambda x, profile, folder_path: mock_types_config[x])
def test_document_types_alias(mock_get_type, mock_listdir):
    aliases = document_types_alias(profile="profile1", folder_path=types_folder)
    assert aliases == ["alias1", "alias2", "alias3"]


@patch("os.path.exists", return_value=True)
@patch("builtins.open", new_callable=mock_open, read_data=json.dumps(mock_types_config["type1"]))
def test_get_type(mock_open_file, mock_path_exists):
    config = get_type("type1", profile="profile1", folder_path=types_folder)
    assert config == mock_types_config["type1"]


@patch("os.path.exists", return_value=False)
def test_get_type_not_found(mock_path_exists):
    config = get_type("unknown_type", folder_path=types_folder)
    assert config == {}
    # Ensure the warning log is triggered


@patch("os.listdir", return_value=mock_types_files)
@patch("conf_utils.get_type", side_effect=lambda x, profile, folder_path: mock_types_config[x])
def test_find_type(mock_get_type, mock_listdir):
    found = find_type("alias1", profile="profile1", folder_path=types_folder)
    assert found == "type1"

    found = find_type("unknown_alias", default_type="default_type", profile="profile1", folder_path=types_folder)
    assert found == "default_type"

    find_type("unknown_aliasss", default_type="other", profile="profile1", folder_path=types_folder)

@patch("os.listdir", return_value=mock_types_files)
@patch("conf_utils.get_type", side_effect=lambda x, profile, folder_path: mock_types_config[x])
def test_find_type_direct_match(mock_get_type, mock_listdir):
    # Case: client_alias matches document_type directly
    found = find_type("type1", folder_path=types_folder)
    assert found == "type1"  # Direct match triggers found_type = document_type; break

    # Case: default_type matches document_type directly
    found = find_type("unknown_alias", default_type="type2", folder_path=types_folder)
    assert found == "type2"  # Direct match with default_type triggers found_type = document_type; break


@patch("builtins.open", new_callable=mock_open, read_data=json.dumps(mock_profiles_map))
@patch("builtins.open", new_callable=mock_open, read_data=json.dumps(mock_profiles_config))
def test_get_profile(mock_open_profiles, mock_open_map, setup_env):
    with patch("os.path.exists", return_value=True):
        profile = get_profile(department="department1", tenant="tenant1")
        assert profile["profile_name"] == "profile1"

    with patch("builtins.open", side_effect=False):
        with pytest.raises(Exception):
            get_profile(department="unknown", tenant="unknown")


@patch("builtins.open", new_callable=mock_open, read_data=json.dumps(mock_models_map))
def test_get_model(mock_open_models):
    model = get_model("model1")
    assert model["config_key"] == "config_value"

    with pytest.raises(Exception):
        get_model("unknown_model")
