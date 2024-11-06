### This code is property of the GGAO ###


import pytest

from genai_sdk_services.files import FilesController
from genai_sdk_services.services.file import ImageService
from genai_sdk_services.services.file import PdfService
from genai_sdk_services.services.file import TxtService


@pytest.fixture()
def get_config():
    config = {
        "user_functions": []
    }

    return config


def test_init(get_config):
    fc = FilesController(config=get_config)
    assert ImageService in fc.services
    assert PdfService in fc.services
    assert TxtService in fc.services


# def test_get_type(get_config):
#     """ Test type returned is the right one for the file"""
#     fc = FilesController(config=get_config)
#     assert fc.get_type("test.txt") == "plain"
#     assert fc.get_type("test.TXT") == "plain"
#
#     assert fc.get_type("test.pdf") == "pdf"
#     assert fc.get_type("test.PDF") == "pdf"
#
#     assert fc.get_type("test.jpeg") == "jpeg"
#     assert fc.get_type("test.JPEG") == "jpeg"
#
#     assert fc.get_type("test.jpg") == "jpeg"
#     assert fc.get_type("test.JPG") == "jpeg"
#
#     assert fc.get_type("test.png") == "png"
#     assert fc.get_type("test.PNG") == "png"


def test_get_file(get_config):
    """ Test services instantiated are the right ones for that type of file """
    fc = FilesController(config=get_config)
    assert isinstance(fc._get_file("txt"), TxtService)
    assert isinstance(fc._get_file("plain"), TxtService)
    assert isinstance(fc._get_file("pdf"), PdfService)
    assert isinstance(fc._get_file("jpeg"), ImageService)
    assert isinstance(fc._get_file("jpg"), ImageService)
    assert isinstance(fc._get_file("png"), ImageService)


def test_get_file_fail(get_config):
    fc = FilesController(config=get_config)
    with pytest.raises(ValueError, match="Type not supported"):
        fc._get_file("notsupported")
