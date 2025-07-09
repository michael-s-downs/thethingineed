### This code is property of the GGAO ###


# Native imports
import os
import base64
from typing import Tuple
from copy import deepcopy


def _adapt_input_files(request_json: dict, input_files: list) -> Tuple[dict, list]:
    """ Adapt input files deserializing bytes

    :param request_json: Request JSON with all information
    :param input_files: Input files attached from client
    :return: Request JSON and input files adapted
    """
    input_json = request_json['input_json']

    if 'documents_metadata' in input_json:
        for file_name in list(input_json['documents_metadata'].keys()):
            input_json['documents_metadata'][file_name.replace("_parsed", "")] = input_json['documents_metadata'].pop(file_name)

        for file_name, doc in input_json['documents_metadata'].items():
            if 'content_binary' in doc:
                # Get bytes and replace it with the storage path where will be allocated
                file_bytes = base64.b64decode(doc['content_binary'])
                doc['content_binary'] = f"{request_json['documents_folder']}/{file_name}"

                input_files.append({'file_name': file_name, 'file_bytes': file_bytes})

    return request_json, input_files

def adapt_input_base(request_json: dict, input_files: list) -> Tuple[dict, list]:
    """ Adapt input for profile base

    :param request_json: Request JSON with all information
    :param input_files: Input files attached from client
    :return: Request JSON and input files adapted
    """
    request_json, input_files = _adapt_input_files(request_json, input_files)

    input_json = request_json['input_json']
    request_json['documents_metadata'] = input_json.pop('documents_metadata', {})

    if 'response_url' in input_json:
        request_json['response_url'] = input_json.pop('response_url')
    elif 'response_api' in input_json:
        request_json['response_url'] = input_json.pop('response_api')
        request_json['client_profile']['custom_functions']['response_async'] = "response_calls.response_api_default"
    elif 'response_queue' in input_json:
        request_json['response_url'] = input_json.pop('response_queue')
        request_json['client_profile']['custom_functions']['response_async'] = "response_calls.response_queue_default"
    else:
        request_json['client_profile']['custom_functions'].pop('response_async')

    # Define preprocess configuration
    request_json['preprocess_conf'] = input_json.pop('preprocess_conf', {})


    # Define layout_conf
    layout_conf = request_json['preprocess_conf'].get('layout_conf', {})
    layout = input_json.get('layout', request_json.get('preprocess_conf', {}).get('layout', False)) # Retrocompatibility (must stay inside preprocess_conf)
    request_json['preprocess_conf']['layout_conf'] = {
        'do_lines_text': layout_conf.get('do_lines_text', layout),
        'do_lines_ocr': layout_conf.get('do_lines_ocr', layout),
        'lines_conf': {
            'do_lines_result': layout_conf.get('lines_conf', {}).get('do_lines_result', False),
            'model': "checkpoint-41806"
        },
        'do_titles': layout_conf.get('do_titles', layout),
        'do_tables': layout_conf.get('do_tables', layout),
        'tables_conf': {
            'sep': layout_conf.get('tables_conf', {}).get('sep', "\t")
        }
    }

    # Retrocompatibility for force_ocr
    if 'force_ocr' in input_json:
        request_json['preprocess_conf'].setdefault('ocr_conf', {}).setdefault('force_ocr', input_json.pop('force_ocr'))

    request_json['apigw_params'].update(input_json.get('headers', {}))
    request_json['documents_folder'] = input_json.get('documents_folder', request_json['documents_folder'])
    request_json['persist_preprocess'] = input_json.get('persist_preprocess', False) 

    return request_json, input_files

def adapt_input_default(request_json: dict, input_files: list) -> Tuple[dict, list]:
    """ Adapt input for profile default

    :param request_json: Request JSON with all information
    :param input_files: Input files attached from client
    :return: Request JSON and input files adapted
    """
    request_json, input_files = adapt_input_base(request_json, input_files)

    # Create key documents
    request_json['documents'] = [f"{request_json['documents_folder']}/{doc}".replace("//", "/") for doc in request_json['documents_metadata']]

    # Define pipeline based in operation requested
    operation = request_json['input_json']['operation']
    
    if operation == "preprocess":
        request_json['client_profile']['pipeline'] = ["preprocess"]
    elif operation == "download":
        request_json['client_profile']['pipeline'] = ["download"]
        request_json['client_profile']['custom_functions']['adapt_output'] = "io_adaptations.adapt_output_download" 
    elif operation == "indexing":
        request_json['client_profile']['pipeline'] = ["indexing"]
        
        if request_json['input_json'].get('indexation_conf', {}):
            request_json['indexation_conf'] = request_json['input_json']['indexation_conf']
            request_json['indexation_conf'].setdefault('models', ["techhub-pool-world-ada-002"])
            request_json['indexation_conf'].setdefault('metadata', {})
        else:
            # Retrocompatibility mode
            request_json['indexation_conf'] = {}
            request_json['indexation_conf']['vector_storage_conf'] = {
                'index': request_json['input_json'].get('index')
            }

            # Retrocompatibility mode
            request_json['input_json']['chunking_method'] = request_json['input_json'].get('chunking_method', {})
            # Overwrite if exists, but default values if not
            request_json['input_json']['chunking_method']['window_overlap'] = request_json['input_json'].get('window_overlap', 10)
            request_json['input_json']['chunking_method']['window_length'] = request_json['input_json'].get('window_length', 300)
            request_json['input_json']['chunking_method']['method'] = request_json['input_json'].get('chunking_method', {}).get('method', "simple")
            request_json['input_json']['chunking_method']['sub_window_overlap'] = request_json['input_json'].get('chunking_method', {}).get('sub_window_overlap')
            request_json['input_json']['chunking_method']['sub_window_length'] = request_json['input_json'].get('chunking_method', {}).get('sub_window_length')
            request_json['input_json']['chunking_method']['windows'] = request_json['input_json'].get('chunking_method', {}).get('windows')
        
            # Delete none items from dicts and add chunking method to indexation_conf
            request_json['indexation_conf']['chunking_method'] = {k: v for k, v in request_json['input_json']['chunking_method'].items() if v}
            request_json['input_json']['chunking_method'] = {k: v for k, v in request_json['input_json']['chunking_method'].items() if v}

            # Retrocompatibility mode
            request_json['indexation_conf']['metadata'] = request_json['input_json'].get('metadata', {})
            request_json['indexation_conf']['models'] = request_json['input_json'].get('models', ["techhub-pool-world-ada-002"])

        request_json['indexation_conf']['vector_storage_conf'].setdefault('vector_storage', request_json['client_profile'].get('vector_storage', os.getenv("VECTOR_STORAGE")))

    return request_json, input_files

def adapt_input_queue(request_json: dict, input_files: list) -> Tuple[dict, list]:
    """ Adapt input for profile queue

    :param request_json: Request JSON with all information
    :param input_files: Input files attached from client
    :return: Request JSON and input files adapted
    """
    request_json['tracking'] = deepcopy(request_json['input_json'])

    input_json = request_json['input_json'].pop('GenaiRequest', {})
    request_json['output_json'] = request_json.pop('input_json')
    request_json['input_json'] = input_json

    if request_json['input_json']['operation'] == "delete":
        request_json, input_files = adapt_input_base(request_json, input_files)
        request_json['client_profile']['pipeline'] = ["infodelete"]
    else:
        request_json, input_files = adapt_input_default(request_json, input_files)

    return request_json, input_files

def adapt_output_base(request_json: dict) -> Tuple[dict, dict]:
    """ Adapt output for profile base

    :param request_json: Request JSON with all information
    :return: Request JSON and output result adapted
    """
    result_parsed = {}

    if request_json['status'] == "error":
        result_parsed['status'] = "error"
        result_parsed['error'] = request_json.get('error', "")
    else:
        result_parsed['status'] = "ok"

    if not result_parsed.get('error', ""):
        process_error = all([doc.get('status', "") == "error" for doc in request_json.get('documents_metadata', {}).values()])

        if process_error:
            result_parsed['status'] = "error"
            msg = [doc.get("error", "") for doc in request_json.get('documents_metadata', {}).values()]
            result_parsed['error'] = f"Process component not working: {', '.join(msg)}"

    return request_json, result_parsed

def adapt_output_default(request_json: dict) -> Tuple[dict, dict]:
    """ Adapt output for profile default

    :param request_json: Request JSON with all information
    :return: Request JSON and output result adapted
    """
    request_json, result_parsed = adapt_output_base(request_json)

    result_parsed['request_id'] = request_json['input_json'].get('request_id', request_json['integration_id'])
    result_parsed['index'] = request_json['input_json'].get('index', "")

    # Status generic
    if result_parsed['status'] == "ok":
        result_parsed['status'] = "Finished"
    if result_parsed['status'] == "error":
        result_parsed['status'] = "Error"
        result_parsed.setdefault('error', "Unable to index documents")

    metadata = request_json.get('documents_metadata', {})
    result_parsed['docs'] = metadata
    for file_metadata in metadata:
        metadata[file_metadata]['status'] = "Finished" if result_parsed['status'] == "Finished" else "Error"

    return request_json, result_parsed

def adapt_output_queue(request_json: dict) -> Tuple[dict, dict]:
    """ Adapt output for profile queue

    :param request_json: Request JSON with all information
    :return: Request JSON and output result adapted
    """
    if request_json['input_json']['operation'] == "delete":
        if request_json['status'] == "finish":
            result_parsed = {'status': "finished", 'status_code': 200}
        else:
            result_parsed = {'status': "error", 'status_code': 500, 'error': "Unable to delete documents"}
    else:
        request_json, result_parsed = adapt_output_default(request_json)

    output_json = request_json.pop('output_json', {})
    output_json['GenaiResponse'] = result_parsed
    result_parsed = output_json

    return request_json, result_parsed

def adapt_output_download(request_json: dict) -> Tuple[dict, dict]:
    """ Adapt output for download operation

    :param request_json: Request JSON with all information
    :return: Request JSON and output result adapted
    """
    if request_json.get('status') == 'finish' and 'download_result' in request_json:
        result_parsed = request_json['download_result'].copy()
    else:
        result_parsed = {
            'status': 'error',
            'error': request_json.get('error', 'Unknown error occurred during download')
        }
    
    return request_json, result_parsed