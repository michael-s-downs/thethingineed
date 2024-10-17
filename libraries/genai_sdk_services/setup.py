import genai_sdk_services
import sys
from setuptools import setup, find_packages

requirements = []
if (3, 8) <= sys.version_info < (3, 9):
    with open('requirements_38.txt') as f:
        requirements = f.read().splitlines()

setup(
    name="genai-sdk-services",
    version=genai_sdk_services.__version__,
    author=genai_sdk_services.__author__,
    install_requires=requirements,
    include_package_data=True,
    packages=find_packages(),
    description="GENAI SDK Services",
)
