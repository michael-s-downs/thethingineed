### This code is property of the GGAO ###


"""
Utils and common functions of Genai services
"""
# Native imports
import os, re
import json
from shutil import rmtree

SECRETS_ROOT_PATH = '/secrets'
ELASTICSEARCH_INDEX = lambda index, embedding_model: re.sub(r'[\\/,:|>?*<\" \\]', "_", f"{index}_{embedding_model}").lower()


def convert_service_to_queue(service_name: str, provider: str = "aws") -> str:
    """ Convert Genai service_name to Queue name

    :param service_name: Identifier of the service to get SQS name of
    :param provider: Cloud provider
    :return: str - Name of the queue
    """
    queue_name = f"Q_{service_name.replace(' ', '').replace('-', '_').upper()}"
    queue = queue_name if provider == "aws" else os.getenv(queue_name, service_name)

    return queue


def convert_service_to_endpoint(service_name: str) -> str:
    """ Convert Genai service_name to endpoint

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
    """ Convert Genai extractor_name to Queue name

        :param extractor_name: Identifier of the extractor to get SQS name
        :return: str - Name of the queue
        """
    extractor_name = f"{extractor_name}-EXTRACTOR"
    return f"Q_{extractor_name.replace(' ', '').replace('-', '_').upper()}"


def load_secrets(vector_storage_needed: bool = True, aws_credentials_needed: bool = True) -> (dict, dict, dict):
    """ Load sensitive content from the secrets
    """
    models_keys_path = os.path.join(os.getenv('SECRETS_PATH', SECRETS_ROOT_PATH), "models", "models.json")
    vector_storages_path = os.path.join(os.getenv('SECRETS_PATH', SECRETS_ROOT_PATH), "vector-storage",
                                        "vector_storage_config.json")
    aws_keys_path = os.path.join(os.getenv('SECRETS_PATH', SECRETS_ROOT_PATH), "aws", "aws.json")
    aws_env_vars = ["AWS_ACCESS_KEY", "AWS_SECRET_KEY"]

    # Load AWS credentials
    if aws_credentials_needed:
        if os.path.exists(aws_keys_path):
            with open(aws_keys_path, "r") as file:
                aws_credentials = json.load(file)
        elif os.getenv(aws_env_vars[0], ""):
            aws_credentials = {
                'access_key': os.getenv(aws_env_vars[0]),
                'secret_key': os.getenv(aws_env_vars[1])
            }
        else:
            raise FileNotFoundError(
                f"AWS credentials not found in {aws_keys_path} or in environment variables {aws_env_vars}.")

    # Load models credentials
    if os.path.exists(models_keys_path):
        with open(models_keys_path, "r") as file:
            models_credentials = json.load(file)
    else:
        raise FileNotFoundError(f"Credentials file not found {models_keys_path}.")

    if vector_storage_needed:
        # Load vector storages credentials
        if os.path.exists(vector_storages_path):
            with open(vector_storages_path, "r") as file:
                vector_storages = json.load(file).get("vector_storage_supported")
        else:
            raise FileNotFoundError(f"Vector storages file not found {vector_storages_path}.")

        return models_credentials, vector_storages, aws_credentials
    return models_credentials, aws_credentials


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
        if json_string[i] == "," or json_string[i] == "}" or json_string[i] == "]" or json_string[i] == "\\" or \
                json_string[i] == " ":
            break
        error_param.append(json_string[i])
    error_param = "".join(error_param)
    return error_param

def get_models(available_models, available_pools, key, value):
    """ Get the LLM or embedding models filtered by key from the available models

    :param available_models: Available models
    :param available_pools: Available pools
    :param key: Key to get the models from
    :return: dict - Models
    """

    if isinstance(available_models, dict):
        aux_available_models = available_models.copy()
        available_models = []
        for platform, models in aux_available_models.items():
            for model in models:
                available_models.append({**model, 'platform': platform})
    if key == "pool":
        models = []
        for model in available_pools.get(value, []):
            if isinstance(model, dict):
                models.append(model.get("model"))
            else:
                models.append(model)
        return models, None
    elif key in ["embedding_model", "platform", "zone", "model_type"]:
        models = []
        pools = []
        for model in available_models:
            if model.get(key) == value:
                models.append(model.get("model") if model.get('model') else model.get('embedding_model_name'))
                pools.extend(model.get("model_pool", []))
        return models, pools
    else:
        raise ValueError(f"Key {key} not supported.")