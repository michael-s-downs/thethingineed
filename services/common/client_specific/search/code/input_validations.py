### This code is property of the GGAO ###


# Native imports
import re
import json
import copy
from typing import Tuple

# Custom imports
import conf_utils
import docs_utils


def _validate_param(input_json: dict, param: str, param_type: object) -> Tuple[bool, list]:
    """ Validate if param exist, correct type and no empty

    :param input_json: JSON with expected param
    :param param: Name of param to validate
    :param param_type: Type of param to validate
    :return: True or False if param is valid and error messages
    """
    messages = []
    valid = True

    if param not in input_json:
        valid = False
        messages.append(f"Param '{param}' is required")
    else:
        if not input_json[param]:
            valid = False
            messages.append(f"Param '{param}' can't be empty")
        else:
            if type(input_json[param]) != param_type:
                valid = False
                messages.append(f"Param '{param}' must be {param_type.__name__} type")

    return valid, messages

def _validate_requestid(request_json: dict) -> Tuple[bool, list]:
    """ Validate param request_id

    :param request_json: Request JSON with all information
    :return: True or False if param is valid and error messages
    """
    valid, messages = _validate_param(request_json['input_json'], 'request_id', str)

    return valid, messages

def _validate_documents_folder(request_json: dict) -> Tuple[bool, list]:
    """ Validate param documents_folder

    :param request_json: Request JSON with all information
    :return: True or False if param is valid and error messages
    """
    valid, messages = _validate_param(request_json['input_json'], 'documents_folder', str)

    return valid, messages

def _validate_index(request_json: dict) -> Tuple[bool, list]:
    """ Validate param index

    :param request_json: Request JSON with all information
    :return: True or False if param is valid and error messages
    """
    valid, messages = _validate_param(request_json['input_json'], 'index', str)

    if valid:
        pattern = "[a-z0-9_]"

        if not bool(re.search(f"^{pattern}+$", request_json['input_json']['index'])):
            valid = False
            messages.append(f"Forbidden chars in param 'index', only allowed {pattern}")

    return valid, messages

def _validate_response_url(request_json: dict) -> Tuple[bool, list]:
    """ Validate param response_url

    :param request_json: Request JSON with all information
    :return: True or False if param is valid and error messages
    """
    valid = True
    messages = []

    if 'response_url' in request_json['input_json']:
        valid, messages = _validate_param(request_json['input_json'], 'response_url', str)

    return valid, messages

def _validate_models(request_json: dict) -> Tuple[bool, list]:
    """ Validate param models

    :param request_json: Request JSON with all information
    :return: True or False if param is valid and error messages
    """
    valid = True
    messages = []

    if "models" in request_json['input_json']:
        valid, messages = _validate_param(request_json['input_json'], 'models', list)

        if valid:
            valid_list = []
            messages_list = []
            models_map = json.loads(open(f"{conf_utils.custom_folder}models_map.json", 'r').read())

            for model in request_json['input_json']['models']:
                if model in models_map:
                    valid_list.append(True)
                else:
                    valid_list.append(False)
                    messages_list.append(f"Model '{model}' is not supported")

            valid = all(valid_list)
            messages.extend(messages_list)

    return valid, messages

def _validate_operation(request_json: dict) -> Tuple[bool, list]:
    """ Validate param operation

    :param request_json: Request JSON with all information
    :return: True or False if param is valid and error messages
    """
    valid, messages = _validate_param(request_json['input_json'], 'operation', str)

    if valid:
        operations = ["indexing", "delete"]

        if request_json['input_json']['operation'] not in operations:
            valid = False
            messages.append(f"Param 'operation' must be one of the supported operations ({', '.join(operations)})")

    return valid, messages

def _validate_filename(doc_metadata: str) -> Tuple[bool, list]:
    """ Validate param file_name

    :param doc_metadata: JSON with document metadata
    :return: True or False if param is valid and error messages
    """
    valid = True
    messages = []

    extension = doc_metadata.split(".")[-1].lower()
    if extension not in docs_utils.formats_acceptable:
        valid = False
        messages.append(f"Extension '{extension}' is not supported, must be one of the supported formats ({', '.join(docs_utils.formats_acceptable)})")

    return valid, messages

def _validate_doctype(doc_metadata: dict) -> Tuple[bool, list]:
    """ Validate param document_type

    :param doc_metadata: JSON with document metadata
    :return: True or False if param is valid and error messages
    """
    valid, messages = _validate_param(doc_metadata, 'document_type', str)

    if valid:
        all_types = list(set(conf_utils.document_types() + conf_utils.document_types_alias()))

        if doc_metadata['document_type'] not in all_types:
            valid = False
            messages.append(f"Param 'document_type' must be one a supported types ({', '.join(all_types)})")

    return valid, messages

def _validate_metadata(doc_metadata: dict) -> Tuple[bool, list]:
    """ Validate param metadata

    :param doc_metadata: JSON with document metadata
    :return: True or False if param is valid and error messages
    """
    valid, messages = _validate_param(doc_metadata, 'metadata', dict)

    return valid, messages

def _validate_contentbinary(doc_metadata: dict) -> Tuple[bool, list]:
    """ Validate param content_binary

    :param doc_metadata: JSON with document metadata
    :return: True or False if param is valid and error messages
    """
    valid, messages = _validate_param(doc_metadata, 'content_binary', str)

    return valid, messages

def _validate_extractfields(doc_metadata: dict) -> Tuple[bool, list]:
    """ Validate param extraction_fields

    :param doc_metadata: JSON with document metadata
    :return: True or False if param is valid and error messages
    """
    valid, messages = _validate_param(doc_metadata, 'extraction_fields', dict)

    return valid, messages

def _validate_docmetadata(doc_matadata: str) -> Tuple[bool, list]:
    """ Validate param document_metadata

    :param doc_matadata: String with document metadata
    :return: True or False if param is valid and error messages
    """
    valid_list = []
    messages_list = []

    for func in [_validate_filename]:
        valid, messages = func(doc_matadata)
        messages_list.extend(messages)
        valid_list.append(valid)

    return all(valid_list), messages_list

def _validate_docsfolder(request_json: dict) -> Tuple[bool, list]:
    """ Validate param documents_folder

    :param request_json: Request JSON with all information
    :return: True or False if param is valid and error messages
    """
    valid, messages = _validate_param(request_json['input_json'], 'documents_folder', str)

    return valid, messages

def _validate_docsmetadata(request_json: dict) -> Tuple[bool, list]:
    """ Validate param documents_metadata

    :param request_json: Request JSON with all information
    :return: True or False if param is valid and error messages
    """
    valid, messages = _validate_param(request_json['input_json'], 'documents_metadata', dict)

    if valid:
        for doc in request_json['input_json']['documents_metadata'].values():
            if not doc.get('content_binary', ""):
                valid = False
                break

        if not valid:
            valid, _ = _validate_docsfolder(request_json)

            if not valid:
                messages.append("No documents received, send serialized as 'content_binary' or specified a valid internal path with 'documents_folder'")

    return valid, messages

def _validate_delete(request_json: dict) -> Tuple[bool, list]:
    """ Validate param delete

    :param request_json: Request JSON with all information
    :return: True or False if param is valid and error messages
    """
    valid, messages = _validate_param(request_json['input_json'], 'delete', dict)

    return valid, messages

def _validate_input_base(request_json: dict, input_files: list, validate_functions: list=[]) -> Tuple[bool, list]:
    """ Validate input and report errors for profile base

    :param request_json: Request JSON with all information
    :param input_files: Input files attached from client
    :param validate_functions: List of functions to validate
    :return: True or False if input is valid and error messages
    """
    valid_list = []
    messages_list = []
    input_json = request_json['input_json']

    if input_json and type(input_json) == dict:
        for func in validate_functions:
            valid, messages = func(request_json)
            messages_list.extend(messages)
            valid_list.append(valid)
    else:
        valid_list.append(False)
        messages_list.append("No JSON received or invalid format (send it as data with application/json content)")

    return all(valid_list), messages_list

def validate_input_default(request_json: dict, input_files: list) -> Tuple[bool, str]:
    """ Validate input and report errors for profile default

    :param request_json: Request JSON with all information
    :param input_files: Input files attached from client
    :return: True or False if input is valid and error messages
    """
    validate_functions = [_validate_index, _validate_operation, _validate_docsmetadata, _validate_response_url, _validate_models]
    valid, messages_list = _validate_input_base(request_json, input_files, validate_functions)

    return valid, ", ".join(messages_list)

def validate_input_delete(request_json: dict, input_files: list) -> Tuple[bool, str]:
    """ Validate input and report errors for profile delete

    :param request_json: Request JSON with all information
    :param input_files: Input files attached from client
    :return: True or False if input is valid and error messages
    """
    validate_functions = [_validate_index, _validate_operation, _validate_response_url, _validate_delete]
    valid, messages_list = _validate_input_base(request_json, input_files, validate_functions)

    return valid, ", ".join(messages_list)

def validate_input_knowler_queue(request_json: dict, input_files: list) -> Tuple[bool, str]:
    """ Validate input and report errors for profile knowler_queue

    :param request_json: Request JSON with all information
    :param input_files: Input files attached from client
    :return: True or False if input is valid and error messages
    """

    input_node = 'APIRequest'
    input_json = request_json['input_json'].get(input_node, {})

    if input_json:
        request_json = copy.deepcopy(request_json)
        request_json['input_json'] = input_json

        if input_json.get('operation', "") == "delete":
            valid, message = validate_input_delete(request_json, input_files)
        else:
            valid, message = validate_input_default(request_json, input_files)
    else:
        valid = False
        message = f"Node '{input_node}' not found in JSON"

    return valid, message