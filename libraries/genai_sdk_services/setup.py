import genai_sdk_services
import sys
from setuptools import setup, find_packages

requirements = []
if (3, 11) <= sys.version_info < (3, 12):
    with open('requirements_311.txt') as f:
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
