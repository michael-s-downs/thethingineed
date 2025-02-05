### This code is property of the GGAO ###


# Native imports
import re
import json
import copy
from typing import Tuple

# Custom imports
import conf_utils
import docs_utils
from common.storage_manager import ManagerStorage
from common.genai_controllers import storage_containers
from common.ir.validations import is_available_metadata

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
    json_body = request_json['input_json']
    if request_json['input_json'].get('indexation_conf', {}).get('vector_storage_conf', {}):
        json_body = request_json['input_json']['indexation_conf']['vector_storage_conf']

    valid, messages = _validate_param(json_body, 'index', str)

    if valid:
        pattern = "[a-z0-9_]"

        if not bool(re.search(f"^{pattern}+$", json_body['index'])):
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

    json_body = request_json['input_json']
    if request_json['input_json'].get('indexation_conf', {}).get('models', {}):
        json_body = request_json['input_json']['indexation_conf']

    if "models" in json_body:
        valid, messages = _validate_param(json_body, 'models', list)

        if valid:
            valid_list = []
            messages_list = []
            models_map = json.loads(open(f"{conf_utils.custom_folder}models_map.json", 'r').read())

            for model in json_body['models']:
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


def _validate_index_metadata(request_json: dict) -> Tuple[bool, list]:
    """ Validate param index_metadata"""
    valid = True
    messages = []
    index_metadata = request_json['input_json'].get('indexation_conf', {}).get('index_metadata')

    if index_metadata:
        chunking_method = request_json['input_json'].get('indexation_conf', {}).get('chunking_method', {}).get('method', "simple")
        metadata = request_json['input_json'].get('indexation_conf', {}).get('metadata', {})
        if isinstance(index_metadata, list):
            for key in index_metadata:
                if not is_available_metadata(metadata, key, chunking_method):
                    valid = False
                    messages.append(f"The 'index_metadata' key ({key}) does not appear in the passed metadata or in the mandatory metadata for the chunking method '{chunking_method}'")
        elif isinstance(index_metadata, bool):
            # If it is a boolean, it is valid
            pass
        else:
            valid = False
            messages.append("The 'index_metadata' must be a list")    
    return valid, messages


def _validate_metadata_primary_keys(request_json: dict) -> Tuple[bool, list]:
    """ Validate param metadata_primary_keys"""
    valid = True
    messages = []
    metadata_primary_keys = request_json['input_json'].get('indexation_conf', {}).get('vector_storage_conf', {}).get('metadata_primary_keys')

    if metadata_primary_keys:
        chunking_method = request_json['input_json'].get('indexation_conf', {}).get('chunking_method', {}).get('method', "simple")
        metadata = request_json['input_json'].get('indexation_conf', {}).get('metadata', {})
        if isinstance(metadata_primary_keys, list):
            for key in metadata_primary_keys:
                if not is_available_metadata(metadata, key, chunking_method):
                    valid = False
                    messages.append(f"The 'metadata_primary_keys' key ({key}) does not appear in the passed metadata or in the mandatory metadata for the chunking method '{chunking_method}'")
        else:
            valid = False
            messages.append("The 'metadata_primary_keys' must be a list")
    return valid, messages

def _validate_ocr(request_json: dict) -> Tuple[bool, list]:
    # Check ocr and llm_ocr_conf parameters
    messages = []
    valid = True
    valid_ocrs = ['azure-ocr', 'aws-ocr', 'llm-ocr', 'tesseract-ocr']

    if not request_json['input_json'].get('preprocess_conf', {}).get('ocr_conf'):
        return valid, messages
    ocr_conf = request_json['input_json'].get('preprocess_conf', {}).get('ocr_conf')

    if ocr_conf.get('ocr') not in valid_ocrs:
        valid = False
        messages.append(f"The 'ocr' parameter must be one of the supported values: {valid_ocrs}")

    if ocr_conf.get('ocr', "") == 'llm-ocr':
        if ocr_conf.get('llm_ocr_conf'):
            if not isinstance(ocr_conf['llm_ocr_conf'], dict):
                valid = False
                messages.append("The 'llm_ocr_conf' parameter must be a dictionary")
            llm_ocr_conf = ocr_conf['llm_ocr_conf']
            if llm_ocr_conf.get('system') and not llm_ocr_conf.get('query'):
                valid = False
                messages.append("The 'query' parameter is mandatory when 'system' is specified")
            storage_manager = ManagerStorage.get_file_storage({"type": "LLMStorage", "workspace": storage_containers.get('workspace'), "origin": storage_containers.get('origin')})
            available_pools = storage_manager.get_available_pools()
            available_models = storage_manager.get_available_models()
            # Check if the platform is valid
            if llm_ocr_conf.get('platform') and llm_ocr_conf.get('platform') not in available_models:
                valid = False
                messages.append(f"The 'platform' parameter must be one of the supported values: {available_models.keys()}")
            else:
                # Check if the model is valid (for a valid model there must be a valid platform)
                if llm_ocr_conf.get('model') and llm_ocr_conf.get('model') not in available_pools:
                    found = False
                    for model in available_models.get(llm_ocr_conf.get('platform'), []):
                        if llm_ocr_conf.get('model') == model.get('model'):
                            found = True
                            break
                    if not found:
                        valid = False
                        messages.append(f"The 'model': '{llm_ocr_conf.get('model')}' parameter must be in the LLM models config file for the platform '{llm_ocr_conf.get('platform')}'")           
    else:
        if 'llm_ocr_conf' in ocr_conf:
            valid = False
            messages.append("The 'llm_ocr_conf' parameter is only valid when 'ocr' is 'llm-ocr'")
    return valid, messages


def _validate_chunking_method(request_json: dict) -> Tuple[bool, list]:
    # Check chunking methods (simple not necessary as all mandatory params, have default ones)
    messages = []
    valid = True

    json_body = {}
    if request_json['input_json'].get('indexation_conf', {}).get('chunking_method', {}):
        json_body = request_json['input_json']['indexation_conf']['chunking_method']
    else:
        if request_json['input_json'].get('chunking_method', {}):
            json_body = request_json['input_json']['chunking_method']
        if request_json['input_json'].get('window_length'):
            json_body['window_length'] = request_json['input_json']['window_length']
        if request_json['input_json'].get('window_overlap'):
            json_body['window_overlap'] = request_json['input_json']['window_overlap']

    method = json_body.get('method', "simple")
    if method != "simple":
        if method == "recursive": 
            sub_window_overlap = json_body.get('sub_window_overlap') 
            sub_window_length = json_body.get('sub_window_length')
            if not (sub_window_overlap and sub_window_length) or json_body.get('windows'):
                valid = False
                messages.append("The 'recursive' chunking method must only have the 'sub_window_overlap' and 'sub_window_length' key")
            else:
                if sub_window_length > json_body.get('window_length', 300):
                    valid = False
                    messages.append("The 'sub_window_length' must be less than 'window_length' (300 by default)")
                if sub_window_overlap > json_body.get('window_overlap', 10):
                    valid = False
                    messages.append("The 'sub_window_overlap' must be less than 'window_overlap' (10 by default)")
        elif method == "surrounding_context_window":
            if not json_body.get('windows') or (json_body.get('sub_window_overlap') or json_body.get('sub_window_length')):
                valid = False
                messages.append("The 'surrounding_context_window' chunking method must only have the 'windows' key")
        else:
            valid = False
            messages.append(f"Method '{method}' is not a valid chunking type")
    else:
        if json_body.get('sub_window_overlap') or json_body.get('sub_window_length') or json_body.get('windows'):
            valid = False
            messages.append("The 'simple' chunking method does not need 'sub_window_overlap', 'sub_window_length' or 'windows' parameters")
    return valid, messages

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
    validate_functions = [_validate_index, _validate_operation, _validate_docsmetadata, _validate_response_url, 
                          _validate_models, _validate_chunking_method, _validate_ocr, 
                          _validate_metadata_primary_keys, _validate_index_metadata]
    valid, messages_list = _validate_input_base(request_json, input_files, validate_functions)

    return valid, ", ".join(messages_list)

def validate_input_delete(request_json: dict, input_files: list) -> Tuple[bool, str]:
    """ Validate input and report errors for profile delete

    :param request_json: Request JSON with all information
    :param input_files: Input files attached from client
    :return: True or False if input is valid and error messages
    """
    validate_functions = [_validate_index, _validate_operation, _validate_response_url, _validate_delete,]
    valid, messages_list = _validate_input_base(request_json, input_files, validate_functions)

    return valid, ", ".join(messages_list)

def validate_input_queue(request_json: dict, input_files: list) -> Tuple[bool, str]:
    """ Validate input and report errors for profile queue

    :param request_json: Request JSON with all information
    :param input_files: Input files attached from client
    :return: True or False if input is valid and error messages
    """

    input_node = 'GenaiRequest'
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