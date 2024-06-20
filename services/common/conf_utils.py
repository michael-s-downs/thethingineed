### This code is property of the GGAO ###


# Native imports
import os
import json

# Custom imports
from logging_handler import logger

# Global vars
custom_folder = f"{os.path.dirname(__file__)}/client_specific/{os.getenv('INTEGRATION_NAME')}/"
types_folder = custom_folder + "types_config/"
profiles_folder = custom_folder + "profiles/"
profiles_map_path = custom_folder + "profiles_map.json"
models_map_path = custom_folder + "models_map.json"

document_types = lambda folder_path=types_folder: [file.replace(".json", "") for file in os.listdir(folder_path) if ".json" in file]
document_types_alias = lambda profile="", folder_path=types_folder: [alias for doc_type in document_types(folder_path) for alias in get_type(doc_type, profile=profile, folder_path=folder_path).get('_client_alias', [doc_type])]


def get_type(type_name: str, profile: str="", folder_path: str=types_folder) -> dict:
    """ Get JSON with configuration of type

    :param type_name: Name of type
    :param profile: Name of profile
    :param folder_path: Alternative folder path
    :return: Configuration of type
    """
    type_config = {}

    try:
        # Try to get if exists a specific configuration in profile subfolder
        if profile and os.path.exists(f"{folder_path}{profile}/{type_name}.json"):
            type_path = f"{folder_path}{profile}/{type_name}.json"
        else:
            type_path = f"{folder_path}{type_name}.json"

        type_config = json.loads(open(type_path, 'r').read())
    except:
        logger.warning(f"Type config not found in '{type_path}'")

    return type_config

def find_type(client_alias: str, default_type: str="", profile: str="", folder_path: str=types_folder) -> str:
    """ Find type name by client alias or internal name

    :param client_alias: Client alias for type
    :param default_type: Default type if not found
    :param profile: Name of profile
    :param folder_path: Alternative folder path
    :return: Type name
    """
    found_type = default_type

    for document_type in document_types(folder_path):
        try:
            if document_type in [client_alias, default_type]:
                found_type = document_type
                break

            type_config = get_type(document_type, profile=profile, folder_path=folder_path)
            if client_alias in type_config.get('_client_alias', []):
                found_type = document_type
                break
            if profile:
                found_type = find_type(client_alias, default_type, folder_path=folder_path)
        except:
            pass

    return found_type

def get_profile(department: str, tenant: str) -> dict:
    """ Get profile configuration by department

    :param department: Name of department
    :param tenant: Name of tenant
    :return: Profile configuration
    """
    try:
        profiles_map = json.loads(open(profiles_map_path, 'r').read())
        profile_name = profiles_map.get(department, profiles_map.get(tenant, os.getenv('DEFAULT_PROFILE', "default")))
    except:
        profile_name = "default"

    try:
        profile_path = f"{profiles_folder}{profile_name}.json"
        profile_config = json.loads(open(profile_path, 'r').read())
        profile_config['profile_name'] = profile_name
    except:
        raise Exception(f" Unable to find profile '{profile_path}'")

    return profile_config

def get_model(model_name: str) -> dict:
    """ Get model configuration by model name

    :param model_name: Name of model (for client)
    :return: Model configuration
    """
    try:
        models_map = json.loads(open(models_map_path, 'r').read())
        model_config = models_map[model_name]
        model_config['model_name'] = model_name
    except:
        raise Exception(f" Unable to find model '{model_name}'")

    return model_config
