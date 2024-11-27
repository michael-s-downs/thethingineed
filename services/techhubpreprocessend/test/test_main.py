import pytest
from unittest.mock import MagicMock, patch
from main import PreprocessEndDeployment
from unittest import mock

# Fixtures to simulate common inputs
@pytest.fixture
def json_input_valid():
    return {
            "generic": {
                "origins": {
                    "ocr": "aws-ocr"
                },
                "project_conf": {
                    "department": "DEPARTMENT_NAME",
                    "extract_tables": False,
                    "force_ocr": False,
                    "laparams": "none",
                    "process_id": "ir_index_20240327_103734_876982_c9ass3",
                    "process_type": "ir_index",
                    "project_type": "text",
                    "report_url": "",
                    "timeout_id": "timeout_id_develop:ir_index_20240327_103734_876982_c9ass3",
                    "timeout_sender": 60,
                    "url_sender": "uhis-cdac-develop--q-integration-sender"
                }
            },
            "specific": {
                "dataset": {
                    "dataset_key": "ir_index_20240327_103734_876982_c9ass3:ir_index_20240327_103734_876982_c9ass3"
                }
            }
        }


@pytest.fixture
def json_input_invalid():
    return {
            "specific": {
                "dataset": {
                    "dataset_key": "ir_index_20240327_103734_876982_c9ass3:ir_index_20240327_103734_876982_c9ass3"
                }
            }
        }

# Fixture for the deployment instance
@pytest.fixture
def preprocess_deployment():
    return PreprocessEndDeployment()


def test_max_num_queue(preprocess_deployment):
    assert preprocess_deployment.max_num_queue == 1

@pytest.fixture(autouse=True)
def mock_aws_credentials(monkeypatch):
    monkeypatch.setenv('AWS_ACCESS_KEY', 'mock_access_key')
    monkeypatch.setenv('AWS_SECRET_KEY', 'mock_secret_key')
    monkeypatch.setenv('AWS_REGION_NAME', 'mock_region_name')

def test_process_success_ir_index(mocker, preprocess_deployment, json_input_valid):
    # Simulamos las funciones que se usan en el método process
    mocker.patch('main.get_generic', return_value={'mock_generic': True})
    mocker.patch('main.get_specific', return_value={'mock_specific': True})
    mocker.patch('main.get_project_config', return_value={'process_type': 'ir_index'})
    mocker.patch('main.get_status_code', return_value=1)
    mocker.patch('main.update_status')


    must_continue, message, next_service = preprocess_deployment.process(json_input_valid)


    assert must_continue is True
    assert next_service == 'genai_infoindexing'
    assert message == json_input_valid

def test_process_success_preprocess(mocker, preprocess_deployment, json_input_valid):

    mocker.patch('main.get_generic', return_value={'mock_generic': True})
    mocker.patch('main.get_specific', return_value={'mock_specific': True})
    mocker.patch('main.get_project_config', return_value={'process_type': 'preprocess'})
    mocker.patch('main.get_status_code', return_value=1)
    mocker.patch('main.update_status')

    must_continue, message, next_service = preprocess_deployment.process(json_input_valid)

    assert must_continue is True
    assert next_service == 'flowmgmt_checkend'
    assert message == json_input_valid

def test_process_generic_error(mocker, preprocess_deployment, json_input_valid):

    mocker.patch('main.get_generic', side_effect=Exception("Error in generic"))
    mocker.patch('main.get_status_code', return_value=1)
    mocker.patch('main.update_status')

    must_continue, message, next_service = preprocess_deployment.process(json_input_valid)

    assert must_continue is True
    assert next_service == 'flowmgmt_checkend'

def test_process_status_key_error(mocker, preprocess_deployment,json_input_invalid):
    mocker.patch('main.get_generic', return_value={'mock_generic': True})
    mocker.patch('main.get_specific', return_value={'mock_specific': True})
    mocker.patch('main.get_dataset_status_key', side_effect=[Exception("Error getting dataset status key")])

    with pytest.raises(Exception) as context:
        must_continue, message, next_service = preprocess_deployment.process(json_input_valid)
    assert str(context.value) == "Error getting dataset status key"


def test_getting_redis_configuration_error(mocker, preprocess_deployment, json_input_valid):
    mocker.patch('main.set_db', return_value={})
    mocker.patch('main.get_generic', return_value={'mock_generic': True})
    mocker.patch('main.get_specific', return_value={'mock_specific': True})
    mocker.patch('main.get_dataset_status_key', return_value={'mock_dataset_status_key': True})
    mocker.patch.dict('main.db_dbs', {}, clear=True)

    with pytest.raises(Exception) as context:
        must_continue, message, next_service = preprocess_deployment.process(json_input_valid)
        assert str(context.value) == "Error getting redis configuration"

def test_parsing_parameter_error(mocker, preprocess_deployment, json_input_valid):
    mocker.patch('main.get_generic', return_value={'mock_generic': {'key':'value'}})
    mocker.patch('main.get_specific', return_value={'mock_specific': {'key':'value'}})
    mocker.patch('main.get_dataset_status_key', return_value={'mock_dataset_status_key': 'id_089'})
    mocker.patch('main.db_dbs', return_value={'mock_db_dbs': 0})
    mocker.patch('main.get_project_config', side_effect=Exception("Error parsing parameters of configuration"))
    mocker.patch('main.update_status')

    must_continue, message, next_service = preprocess_deployment.process(json_input_valid)
    assert must_continue is True
    assert message == json_input_valid
    assert next_service == 'flowmgmt_checkend'


def test_process_status_code_error(mocker, preprocess_deployment, json_input_valid):

    mocker.patch('main.get_status_code', side_effect=Exception("Error getting status"))
    mocker.patch('main.update_status')

    must_continue, message, next_service = preprocess_deployment.process(json_input_valid)

    assert must_continue is True
    assert message ==json_input_valid
    assert next_service == 'flowmgmt_checkend'

def test_process_status_error(mocker, preprocess_deployment, json_input_valid):
    mocker.patch('main.get_generic', return_value={'mock_generic': True})
    mocker.patch('main.get_specific', return_value={'mock_specific': True})
    mocker.patch('main.db_dbs', return_value={'status': 'mock_status'})
    mocker.patch('main.get_project_config', return_value={'process_type': 'ir_index'})
    mocker.patch('main.get_status_code', return_value=500)
    mocker.patch('main.get_value', return_value={'msg': 'Error in preprocess'})
    mocker.patch('main.update_status')

    must_continue, message, next_service = preprocess_deployment.process(json_input_valid)
    assert must_continue is True
    assert message == json_input_valid
    assert next_service == 'flowmgmt_checkend'


def test_process_invalid_input(mocker, preprocess_deployment, json_input_invalid):
    mocker.patch('main.get_generic', return_value={'mock_generic': False})
    mocker.patch('main.get_specific', return_value={'mock_specific': False})
    mocker.patch('main.get_project_config', return_value={'process_type': 'unknown'})
    mocker.patch('main.get_status_code', return_value=-1)
    mocker.patch('main.update_status')

    must_continue, message, next_service = preprocess_deployment.process(json_input_invalid)

    assert must_continue is True
    assert message ==json_input_invalid
    assert next_service == 'genai_infoindexing'
