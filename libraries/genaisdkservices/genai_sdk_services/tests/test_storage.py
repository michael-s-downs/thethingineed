### This code is property of the GGAO ###


import pytest

from genai_sdk_services.storage import StorageController
from genai_sdk_services.services.storage import S3Service


@pytest.fixture()
def get_config():
    config = {
        "user_functions": [],
        "storage_credentials": {
            "aws_buckets": {
                "test": {
                    "access_key": "",
                    "secret_key": ""
                }
            }
        }
    }

    return config


def test_init(get_config):
    """ Test controller is initiated with the correspondent services"""
    sc = StorageController(config=get_config)

    assert S3Service in sc.services


def test_get_service(get_config):
    """ Test services instantiated are the right ones for that type of file """
    sc = StorageController(config=get_config)

    assert isinstance(sc._get_origin("aws_buckets"), S3Service)


def test_get_service_fail(get_config):
    """ Test an exception will be raised if type is not supported """
    sc = StorageController(config=get_config)
    with pytest.raises(ValueError, match="Type not supported"):
        sc._get_origin("notsupported")


def test_add_origin(get_config):
    """ Test origins are added when calling to a method """
    sc = StorageController(config=get_config)
    try:
        sc.check_file(("aws_buckets", "test"), "test.txt")
    except Exception:
        pass

    assert "aws_buckets" in sc.origins
    assert isinstance(sc.origins['aws_buckets'], S3Service)
