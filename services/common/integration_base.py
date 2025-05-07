### This code is property of the GGAO ###


# Native imports
import os
import sys
import json
import glob
import importlib
from typing import Tuple
from datetime import datetime

# Custom imports
import conf_utils
import docs_utils
import core_calls
import requests_manager
import provider_resources
from logging_handler import logger
from graceful_killer import GracefulKiller
sys.path.append(f"{os.getenv('LOCAL_COMMON_PATH')}/client_specific/{os.getenv('INTEGRATION_NAME')}/code")


# Global vars
killer = GracefulKiller()  # Avoid to kill during process, must check at the end of request


def get_function(function: str) -> object:
    """ Get function based on string (module.function)

    :param function: Name of function
    :return: Function object
    """
    *modules, function = function.split(".")
    module = ".".join(modules)

    return getattr(sys.modules[module], function)

def import_module(path: str):
    """ Import or refresh module dynamically based on string

    :param path: Path of module
    """
    import_path = path.replace("/", ".").replace("\\", ".").replace(".py", "")
    module_name = import_path.split(".")[-1]

    sys.modules[module_name] = __import__(import_path, fromlist=[import_path])
    importlib.reload(sys.modules[module_name])  # Reload in next times
    globals()[module_name] = sys.modules[module_name]

def load_custom_files():
    """ Load custom modules for client,
    download from storage to replace local files
    """
    # Download custom files from storage if exists
    remote_path = os.getenv('STORAGE_CONFIG_FOLDER').format(integration_name=os.getenv('INTEGRATION_NAME'))
    if not provider_resources.storage_download_folder(conf_utils.custom_folder, remote_path, os.getenv('STORAGE_BACKEND')):
        logger.error(f"Can't download config files from '{remote_path}'")

    # Load custom modules from code folder
    code_folder = conf_utils.custom_folder + "code"
    for path in glob.glob(code_folder + "/**/*.py", recursive=True):
        if os.path.isfile(path):
            path = path.replace(os.path.dirname(__file__) + "/", "")
            path = path.replace(os.getenv('LOCAL_COMMON_PATH')+"/", "")
            logger.debug(f"Loading module '{os.path.basename(path)}'")
            import_module(path)

def get_inputs(request: object) -> Tuple[dict, dict, list]:
    """ Get all data from the input request

    :param request: Input request object
    :return: Params from apigateway, JSON and files from client
    """
    apigw_params = {}
    input_json = {}
    input_files = []

    try:
        # Get API-Gateway params
        apigw_params["x-tenant"] = request.headers.get("x-tenant", "")
        apigw_params["x-department"] = request.headers.get("x-department", "main")
        apigw_params["x-reporting"] = request.headers.get("x-reporting", "")
        apigw_params["x-limits"] = request.headers.get("x-limits", "{}")

        # Get JSON input
        if request.data:
            input_json = request.get_json(force=True)

            if type(input_json) != dict:
                input_json = {}
        elif request.form:
            input_json = request.form.to_dict()

            if len(input_json) == 1 and '' in input_json:
                input_json = json.loads(list(input_json.values())[0])

        # Get input files attached
        for key, file in request.files.items(True):
            input_files.append({'file_name': file.filename, 'file_bytes': file.stream.read()})
    except:
        if type(request) == dict:
            # Input request directly via queue instead of API
            apigw_params = {'x-department': os.getenv('TENANT'), 'x-tenant': os.getenv('TENANT')}
            input_json = request
        else:
            logger.warning("Something was wrong getting request inputs")

    return apigw_params, input_json, input_files

def check_shutdown(request: object):
    """ If kill signal received,
    force flask to shutdown

    :param request: Input request object
    """
    if killer.kill_now:
        shutdown = request.environ.get('werkzeug.server.shutdown')
        shutdown()
        logger.info("---- Stopping service...")

def upload_docs(request_json: dict, input_files: list):
    """ Upload received documents to storage

    :param request_json: Request JSON with all information
    :param input_files: Input files from client to upload
    """
    folder = request_json['integration_id']
    container = os.getenv('STORAGE_DATA')
    mount_path = os.getenv('DATA_MOUNT_PATH', "")
    documents_folder = request_json.get('documents_folder', "")

    for file in input_files:
        provider_resources.storage_put_file(f"{folder}/{file['file_name']}", file['file_bytes'], container)

    if mount_path:
        # Change paths to remote folder
        request_json['documents_folder'] = f"{folder}/{documents_folder.replace(mount_path, '')}".replace("//", "/")
        local_documents = request_json.pop('documents', [])
        request_json['documents'] = []

        if mount_path in documents_folder:
            logger.debug(f"Uploading documents from mount path '{mount_path}'")

            for local_document in local_documents:
                if os.path.exists(local_document):
                    if os.path.isfile(local_document):
                        remote_document = f"{folder}/{local_document.replace(mount_path, '')}".replace("//", "/")
                        result = provider_resources.storage_upload_file(local_document, remote_document, container)

                        if result:
                            request_json['documents'].append(remote_document)
                        else:
                            logger.error(f"Unable to upload file '{local_document}'")
                    else:
                        logger.warning(f"Document path must be a file '{local_document}'")
                else:
                    logger.warning(f"Document path not found '{local_document}'")
        else:
            logger.warning(f"Documents folder '{documents_folder}' not inside mounted path '{mount_path}'")

def convert_docs(request_json: dict):
    """ Convert documents into a supported format,
    upload to storage with other name to preserve both

    :param request_json: Request JSON with all information
    """
    folder = request_json['documents_folder']
    container = os.getenv('STORAGE_DATA')
    files = provider_resources.storage_list_folder(folder, container, recursivity=False, extensions_include=docs_utils.formats_convert)

    for file_name in files:
        file_bytes = provider_resources.storage_get_file(file_name, container)
        file_name, file_bytes = docs_utils.document_conversion(file_name, file_bytes)
        provider_resources.storage_put_file(file_name, file_bytes, container)

def list_docs(request_json: dict) -> dict:
    """ List documents of storage folder
    and append to the request JSON

    :param request_json: Request JSON with all information
    :return: Request JSON with all information
    """
    if 'documents' not in request_json:
        folder = request_json['documents_folder']
        container = os.getenv('STORAGE_DATA')
        files = provider_resources.storage_list_folder(folder, container, recursivity=False, extensions_include=docs_utils.formats_pass)

        request_json['documents'] = files

    return request_json

def delete_data(request_json: dict):
    """ Delete documents and results generated for the request

    :param request_json: Request JSON with all information
    """
    if requests_manager.storage_delete_request:
        logger.debug("Deleting documents from container")
        provider_resources.storage_remove_files(request_json['integration_id'], os.getenv('STORAGE_DATA'))

        if not request_json.get('persist_preprocess', False):
            logger.debug("Deleting results from container and database")
            core_calls.delete(request_json)

def receive_request(request: object) -> Tuple[dict, dict]:
    """ Logic to receive request from client,
    validate and adapt input from client,
    upload and convert documents to storage,
    generate request with all information

    :param request: Input request object
    :return: Request JSON with all information and result (first response for async)
    """
    try:
        apigw_params, input_json, input_files = get_inputs(request)
        request_json = requests_manager.generate_request(apigw_params, input_json)
        logger.info(f"-- Generated request '{request_json['integration_id']}' ('{input_json.get('request_id', '')}')")

        logger.debug("Validating input")
        validate_input = get_function(request_json['client_profile']['custom_functions']['validate_input'])
        valid, msg = validate_input(request_json, input_files)

        if valid:
            logger.debug("Adapting input")
            adapt_input = get_function(request_json['client_profile']['custom_functions']['adapt_input'])
            request_json, input_files = adapt_input(request_json, input_files)

            logger.debug(f"Request JSON: {request_json}")
            logger.debug(f"Input files: {[file['file_name'] for file in input_files]}")

            logger.debug(f"Uploading {len(input_files)} documents to container")
            upload_docs(request_json, input_files)

            logger.debug("Converting documents to supported format")
            convert_docs(request_json)

            logger.debug("Listing all supported documents from container")
            request_json = list_docs(request_json)

            result = {'status': "processing", 'request_id': request_json['input_json'].get('request_id', request_json['integration_id'])}
        else:
            msg_error = f"Bad input: {msg}"
            logger.warning(msg_error)
            request_json['input_json'].pop('documents_metadata', None) # Avoid message too long error
            result = {'status': "error", 'error': msg_error}
    except:
        msg_error = "Internal error"
        logger.error(msg_error, exc_info=True)
        result = {'status': "error", 'error': msg_error}

    # Insert status info in request json (Use locals in case var doesn't exist)
    request_json = {**locals().get('request_json', {}), **result, **{'type': "request"}}

    # Already responded here in error case
    if request_json['status'] == "error":
        request_json.get('client_profile', {}).get('custom_functions', {}).pop('response_async', "")

    return request_json, result

def process_request(request_json: dict) -> Tuple[dict, dict]:
    """ Logic to process request received,
    run pipeline steps from configuration,
    adapt output response for client,
    send response if async request

    :param request_json: Request JSON with all information
    :return: Request JSON with all information and result parsed
    """
    result = {}
    custom_functions = request_json['client_profile']['custom_functions']

    if request_json['status'] != "waiting":
        try:
            # Do init logic in any case (if defined)
            if 'init_logic' in custom_functions:
                init_logic = get_function(custom_functions.pop('init_logic'))
                request_json = init_logic(request_json)
        except:
            logger.warning(f"Error preprocessing request {request_json['integration_id']}", exc_info=True)

        logger.info(f"---- Processing request '{request_json['integration_id']}'")

        try:
            # Not continue pipeline if error or waiting
            while request_json['status'] == "processing":
                pipeline = request_json['client_profile']['pipeline']

                if len(pipeline) > 0:
                    # Get first step as current
                    current_step = pipeline[0]
                    current_func = get_function(custom_functions[current_step])

                    try:
                        logger.info(f"-- Running step '{current_step}' of request '{request_json['integration_id']}'")
                        request_json = current_func(request_json)
                    except:
                        msg_error = f"Pipeline error in step '{current_step}' of request '{request_json['integration_id']}'"
                        logger.error(msg_error, exc_info=True)
                        request_json.update({'status': "error", 'error': msg_error})

                    # If step completed quit from the pipeline
                    if request_json['status'] not in ["waiting", "error"]:
                        request_json['client_profile']['pipeline_done'] = request_json['client_profile'].get('pipeline_done', [])
                        request_json['client_profile']['pipeline_done'].append(pipeline.pop(0))  # Save steps done in other list

                # Finished if no more steps
                if len(pipeline) == 0:
                    request_json['status'] = "finish"

            # Update info with process_ids to delete
            requests_manager.persist_request(request_json)

            # When request is completed ok or ko
            if request_json['status'] in ["finish", "error"]:
                request_json['ts_fin'] = datetime.now().timestamp()
                logger.info(f"---- Completed process ({request_json['status'].upper()}) for request '{request_json['integration_id']}'")

                try:
                    # Do error logic in error case (if defined)
                    if request_json['status'] == "error" and 'error_logic' in custom_functions:
                        error_logic = get_function(custom_functions['error_logic'])
                        request_json = error_logic(request_json)

                    logger.debug("Adapting output")
                    adapt_output = get_function(custom_functions['adapt_output'])
                    request_json, result = adapt_output(request_json)

                    # Response in case async request
                    if 'response_async' in custom_functions:
                        response_async = get_function(custom_functions['response_async'])
                        request_json = response_async(request_json, result)

                    # Do finally logic in any case (if defined)
                    if 'finally_logic' in custom_functions:
                        finally_logic = get_function(custom_functions['finally_logic'])
                        request_json = finally_logic(request_json)
                except:
                    msg_error = f"Error postprocessing request {request_json['integration_id']}"
                    status = {'status': "error", 'error': msg_error}
                    logger.error(msg_error, exc_info=True)
                    result.update(status)
                    request_json.update(status)

                # Update info with final status
                requests_manager.persist_request(request_json)

                # Delete all at the end of process
                logger.info(f"---- Deleting all data of request '{request_json['integration_id']}'")
                delete_data(request_json)
                requests_manager.delete_request(request_json['integration_id'])
        except:
            msg_error = f"Internal error processing request {request_json['integration_id']}"
            status = {'status': "error", 'error': msg_error}
            logger.error(msg_error, exc_info=True)
            result.update(status)
            request_json.update(status)

        logger.debug(f"Request JSON: {request_json}")
        logger.debug(f"Response JSON: {result}")

    return request_json, result