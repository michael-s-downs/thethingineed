### This code is property of the GGAO ###


import pytest
import pandas as pd

from genai_sdk_services.data_bunch import DataBunchController
from genai_sdk_services.services.dataset import HDF5Service, CSVService, CSVPathsService, AthenaService, ParquetService


@pytest.fixture()
def get_dataframe():

    input = pd.DataFrame.from_dict({
        'text': ["Esto es un texto de prueba.",
                 "Más texto con el que trabajar.",
                 "Zaragoza es una ciudad y un municipio de España, ",
                 "capital de la provincia homónima y de la comunidad autónoma de Aragón.",
                 "Esto es un texto de prueba.",
                 "Más texto con el que trabajar.",
                 "Zaragoza es una ciudad y un municipio de España, ",
                 "capital de la provincia homónima y de la comunidad autónoma de Aragón."
                 ],
        'label': [1, 2, 1, 2, 1, 2, 1, 2]
    })

    return input


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
        },
        "db_credentials": {
            "mysql": {
                "test": {
                    "host": "test",
                    "user": "root",
                    "password": "test"
                }
            },
            "athena": {
                "test": {
                    "access_key": "test",
                    "secret_key": "test",
                    "bucket": "test"
                }
            }
        }
    }

    return config


def test_init(get_config):
    """ Test controller is initiated with the correspondent services"""
    dbc = DataBunchController(config=get_config)

    assert HDF5Service in dbc.services
    assert CSVService in dbc.services
    assert CSVPathsService in dbc.services
    assert AthenaService in dbc.services
    assert ParquetService in dbc.services


def test_get_service(get_config):
    """ Test services instantiated are the right ones for that type of file """
    dbc = DataBunchController(config=get_config)

    assert isinstance(dbc._get_origin("hdf5"), HDF5Service)
    assert isinstance(dbc._get_origin("csv"), CSVService)
    assert isinstance(dbc._get_origin("csv_paths"), CSVPathsService)
    assert isinstance(dbc._get_origin("athena"), AthenaService)
    assert isinstance(dbc._get_origin("parquet"), ParquetService)


def test_get_service_fail(get_config):
    """ Test an exception will be raised if type is not supported """
    dbc = DataBunchController(config=get_config)
    with pytest.raises(ValueError, match="Type not supported"):
        dbc._get_origin("notsupported")


def test_split(get_config, get_dataframe):
    """ Test an exception will be raised if type is not supported """
    dbc = DataBunchController(config=get_config)
    X_train, X_test, y_train, y_test = dbc.split_dataset(("csv"), get_dataframe, label="label",
                                                         test_size=0.2, stratify=None, random_state=42)

    assert len(X_train) == len(y_train)
    assert len(X_test) == len(y_test)

    assert len(X_train) == round(len(get_dataframe)*0.8)
    assert len(X_test) == round(len(get_dataframe)*0.2)
