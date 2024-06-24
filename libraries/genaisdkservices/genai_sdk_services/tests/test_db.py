### This code is property of the GGAO ###


import pytest

from genai_sdk_services.db import DBController
from genai_sdk_services.services.db import RedisService, AthenaService


@pytest.fixture()
def get_config():
    config = {
        "user_functions": [],
        "db_credentials": {
            "mysql": {
                "test": {
                    "host": "test",
                    "user": "root",
                    "password": "test"
                }
            },
            "redis": {
                "status": {
                    "host": "test",
                    "port": 6379,
                    "db": 2,
                    "password": ""
                },
                "persistence": {
                    "host": "test",
                    "port": 6379,
                    "db": 3,
                    "password": ""
                }
            },
            "athena": {
                "test": {
                    "access_key": "",
                    "secret_key": "",
                    "bucket": "test"
                }
            }
        }
    }

    return config


def test_init(get_config):
    """ Test controller is initiated with the correspondent services"""
    dbc = DBController(config=get_config)

    assert RedisService in dbc.services
    assert AthenaService in dbc.services


def test_get_service(get_config):
    """ Test services instantiated are the right ones for that type of file """
    dbc = DBController(config=get_config)

    assert isinstance(dbc._get_origin("redis"), RedisService)
    assert isinstance(dbc._get_origin("athena"), AthenaService)


def test_get_service_fail(get_config):
    """ Test an exception will be raised if type is not supported """
    dbc = DBController(config=get_config)
    with pytest.raises(ValueError, match="Type not supported"):
        dbc._get_origin("notsupported")
