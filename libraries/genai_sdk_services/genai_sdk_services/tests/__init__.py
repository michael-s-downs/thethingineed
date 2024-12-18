### This code is property of the GGAO ###


import csv
import os

import pytest

from genai_sdk_services import DIR_RESOURCES_TESTS


@pytest.fixture(scope="session")
def fixture_csv():
    def _csv_loader(path, delimiter=";"):
        f = open(os.path.join(DIR_RESOURCES_TESTS, path))
        return csv.reader(f, delimiter=delimiter)

    return _csv_loader
