### This code is property of the GGAO ###


import pytest

from genai_sdk_services.queue_controller import QueueController
from genai_sdk_services.services.queue_service import AWSQueueService


def test_init():
    """ Test controller is initiated with the correspondent services"""
    qc = QueueController()

    assert AWSQueueService in qc.services


def test_get_service():
    """ Test services instantiated are the right ones for that type of file """
    qc = QueueController()

    assert isinstance(qc._get_origin("aws"), AWSQueueService)


def test_get_service_fail():
    """ Test an exception will be raised if type is not supported """
    qc = QueueController()
    with pytest.raises(ValueError, match="Type not supported"):
        qc._get_origin("notsupported")


def test_add_origin():
    """ Test origins are added when calling to a method """
    qc = QueueController()
    try:
        qc.read(("aws", "test"), delete=False)
    except:
        pass

    assert "aws" in qc.origins
    assert isinstance(qc.origins['aws'], AWSQueueService)