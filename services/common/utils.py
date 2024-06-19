### This code is property of the GGAO ###


"""
Utils and common functions of Dolffia services
"""
# Native imports
import os
from shutil import rmtree


def convert_service_to_queue(service_name: str, provider: str = "aws") -> str:
    """ Convert Dolffia service_name to Queue name

    :param service_name: Identifier of the service to get SQS name of
    :param provider: Cloud provider
    :return: str - Name of the queue
    """
    queue_name = f"Q_{service_name.replace(' ', '').replace('-', '_').upper()}"
    queue = queue_name if provider == "aws" else os.getenv(queue_name, service_name)

    return queue


def convert_service_to_endpoint(service_name: str) -> str:
    """ Convert Dolffia service_name to endpoint

    :param service_name: Identifier of the service to endpoint of
    :return: str - Endpoint
    """
    return f"/{service_name.replace(' ', '').replace('-', '_').lower()}"


def remove_local_files(path: str):
    """ Remove local files by path

    :param path: Path of files
    """
    rmtree(os.path.dirname(path).split("/")[0])


def convert_to_queue_extractor(extractor_name: str) -> str:
    """ Convert Dolffia extractor_name to Queue name

        :param extractor_name: Identifier of the extractor to get SQS name
        :return: str - Name of the queue
        """
    extractor_name = f"{extractor_name}-EXTRACTOR"
    return f"Q_{extractor_name.replace(' ', '').replace('-', '_').upper()}"

def get_error_word_from_exception(ex, json_string) -> str:
    """Get the word that caused the error in the json string

    Args:
        ex (Exception): Exception raised
        json_string (str): Json string causing the error

    Returns:
        error_param (str): The word that caused the error
    """
    error_param = []
    idx = int(str(ex).split("char ")[1].replace(")", ""))
    for i in range(idx, len(json_string)):
        if json_string[i] == "," or json_string[i] == "}" or json_string[i] == "]" or json_string[i] == "\\" or json_string[i] == " ":
            break
        error_param.append(json_string[i])
    error_param = "".join(error_param)
    return error_param