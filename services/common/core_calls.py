### This code is property of the GGAO ###


#TODO: review and optimize functions 'classification_sync', 'extraction_sync', 'classification_async', 'extraction_async'

# Native imports
import os
import time
import json
import random
import string
from datetime import datetime

# Installed imports
import numpy as np
import pandas as pd

# Custom imports
import conf_utils
import docs_utils
import core_api
from logging_handler import logger


# Global vars
request_polling_sleep = 5  # seconds
request_polling_timeout = 30 * 60  # seconds
request_flow_timeout = int(os.getenv('REQUEST_FLOW_TIMEOUT', "30"))  # minutes
request_internal_timeout = int(os.getenv('REQUEST_INTERNAL_TIMEOUT', "30"))  # minutes


def classification_sync(request_json: dict) -> dict:
    """ Process request with step classification in sync mode,
    collect documents to process without classification,
    process needed documents in sync mode if possible,
    send to process needed documents in async mode
    and wait for finish if sync classification not possible

    :param request_json: Request JSON with all information
    :return: Request JSON with all information
    """
    files_need_classification = []
    apigw_params = request_json['apigw_params']
    folder = request_json['documents_folder']
    files = request_json.get('documents', [])
    metadata = request_json.setdefault('documents_metadata', {})
    process_ids = request_json.setdefault('process_ids', {})
    ts_init = request_json['ts_init']

    # Params to fill call template
    request_params = {}
    request_params['ocr'] = request_json.get('preprocess_conf').get('ocr_conf', {}).get('ocr', request_json['client_profile']['default_ocr'])
    request_params['force_ocr'] = request_json.get('preprocess_conf', {}).get('ocr_conf', {}).get('force_ocr', request_json['client_profile'].get('force_ocr', False))
    request_params['folder'] = folder
    request_params.update(conf_utils.get_model(request_json['client_profile']['model']))

    # Collect docs to process
    for file_path in files:
        file_name = docs_utils.parse_file_name(file_path, folder)
        file_metadata = metadata.setdefault(file_name, {})
        document_type = file_metadata.get('document_type', "")

        if document_type:
            # If type from client set max confidence
            file_metadata.setdefault('document_type_confidence', 1.0)
        else:
            files_need_classification.append(file_path)

    # Call corresponding classification
    if not request_params.get('multilabel', False):
        # Sync call must process one by one
        for file_path in files_need_classification:
            file_name = docs_utils.parse_file_name(file_path, folder)
            file_metadata = metadata.get(file_name, {})

            api_response = core_api.sync_classification_multiclass_request(apigw_params, request_params, file_path)
            logger_response = {'process_id': api_response['process_id'], 'status': api_response['status']}
            logger.info(f"- Calling 1 SYNC CLASSIFICATION MULTICLASS for request '{request_json['integration_id']}' {logger_response}")

            if api_response['status'] == "finish":
                file_metadata['document_type'] = api_response['result'][0]['category']
                file_metadata['document_type_confidence'] = api_response['result'][0]['confidence']
                file_metadata['categories'] = api_response['result']
                file_metadata['process_id'] = api_response['process_id']
                file_metadata['ocr_used'] = request_params['ocr']
                file_metadata['async'] = False
                process_ids[api_response['process_id']] = api_response['status']
    else:
        # Async call can process in bulk
        api_response = core_api.async_classification_multilabel_request(apigw_params, request_params, files_need_classification)
        logger.info(f"- Calling {len(files_need_classification)} ASYNC CLASSIFICATION MULTILABEL for request '{request_json['integration_id']}' {api_response}")

        status = api_response['status']
        if status != "error":
            try:
                while status == "waiting":
                    # Initialize time since start request
                    ts_now = datetime.now().timestamp()
                    status = core_api.async_status_request(apigw_params, api_response['process_id'])['status']

                    # Check if time request is greater than timeout
                    if ts_now - ts_init >= request_polling_timeout:
                        msg_error = "Timeout expired calling the API service getting status"
                        logger.error(msg_error, exc_info=True)
                        status = "error"
                        api_response['status'] = "error"
                        api_response['error'] = "timeout"
                        break

                    # Sleep time in seconds by global var
                    time.sleep(request_polling_sleep)
            except:
                msg_error = f"Error getting status for request {request_json['integration_id']}"
                logger.error(msg_error, exc_info=True)
                api_response['status'] = "error"

            try:
                if status == "ready":
                    api_response = core_api.async_result_request(apigw_params, api_response['process_id'])
            except:
                msg_error = f"Error getting results for request {request_json['integration_id']}"
                logger.error(msg_error, exc_info=True)
                api_response['status'] = "error"

        # Add information for document
        for file_path in files_need_classification:
            file_name = docs_utils.parse_file_name(file_path, folder)
            file_metadata = metadata.get(file_name, {})

            file_metadata['process_id'] = api_response['process_id']
            file_metadata['ocr_used'] = request_params['ocr']
            file_metadata['async'] = True

            for result in api_response.get('results', {}):
                if result['filename'] == file_name:
                    file_metadata['categories'] = result['categories']
            if api_response.get('error', ""):
                file_metadata['error'] = api_response['error']

        process_ids[api_response['process_id']] = api_response['status']

    return request_json

def extraction_sync(request_json: dict) -> dict:
    """ Process request with step extraction in sync mode,
    collect documents to process without extraction,
    process needed documents in sync mode if possible,
    send to process needed documents in async mode
    and wait for finish if sync extraction not possible

    :param request_json: Request JSON with all information
    :return: Request JSON with all information
    """
    files_need_extraction = []
    files_need_extraction_meta = []
    apigw_params = request_json['apigw_params']
    folder = request_json['documents_folder']
    files = request_json.get('documents', [])
    metadata = request_json.setdefault('documents_metadata', {})
    process_ids = request_json.setdefault('process_ids', {})
    ts_init = request_json['ts_init']
    request_params = {}

    # Collect docs to process
    for file_path in files:
        file_name = docs_utils.parse_file_name(file_path, folder)
        file_metadata = metadata.get(file_name, {})

        if file_metadata:
            # Check if has type for extraction
            if file_metadata.get('document_type', ""):
               files_need_extraction.append(file_path)
            else:
                file_metadata.update({'status': "error", 'error': "Param 'document_type' required for extraction"})
        else:
            file_metadata.update({'status': "error", 'error': "Param 'document_type' required for extraction"})

    # Send to process needed files
    is_extraction_sync = []
    if files_need_extraction:
        for file_path in files_need_extraction:
            file_name = docs_utils.parse_file_name(file_path, folder)
            file_metadata = metadata.setdefault(file_name, {})
            doc_type = file_metadata['document_type']

            # Get config for file and append list to check if extraction is sync or async
            type_conf = conf_utils.get_type(doc_type, profile=request_json["client_profile"]['profile_name'])
            is_extraction_sync.append(type_conf.get('_sync_extraction', False))
            is_extraction_sync.append(type_conf.get('_need_preprocess', True))

            # Check if ocr_used is the same for extraction to reuse process_id
            if file_metadata.get('process_id', ""):
                type_ocr = type_conf.get('_ocr', request_json['client_profile']['default_ocr'])

                if file_metadata.get('ocr_used', "") != type_ocr:
                    file_metadata.pop('process_id', "")

            # Get metadata fields for the dict list
            files_need_extraction_meta.append({'file_path': file_path, 'document_type': doc_type, 'process_id': file_metadata.get('process_id', "")})

        if all(is_extraction_sync):
            # Sync call must process one by one
            for file_path in files_need_extraction:
                file_name = docs_utils.parse_file_name(file_path, folder)
                file_metadata = metadata.setdefault(file_name, {})
                doc_type = file_metadata['document_type']
                type_conf = conf_utils.get_type(doc_type, profile=request_json["client_profile"]['profile_name'])
                process_id = file_metadata.get('process_id', "")

                # Params to fill call template
                request_params['ocr'] = type_conf.get('_ocr', request_json['client_profile']['default_ocr'])
                request_params['force_ocr'] = type_conf.get('_force_ocr', True)
                request_params['process_id'] = process_id
                request_params['doc_type'] = doc_type
                request_params['params_for_extraction'] = metadata
                request_params['fields_to_extract'] = [field for field in type_conf.keys() if not field.startswith("_")]

                # Call extraction for current type
                api_response = core_api.sync_extraction_request(apigw_params, request_params, file_path)

                logger_response = {'process_id': api_response['process_id'], 'status': api_response['status']}
                logger.info(f"- Calling {len(files_need_extraction)} SYNC EXTRACTION {doc_type.upper()} for request '{request_json['integration_id']}' reusing dataset '{process_id}' {logger_response}")

                if api_response['status'] == "finish":
                    file_metadata['document_fields'] = api_response['result']
                    file_metadata['process_id'] = api_response['process_id']

                file_metadata['ocr_used'] = request_params['ocr']
                file_metadata['async'] = False
                process_ids[api_response['process_id']] = api_response['status']
        else:
            # Group files by document_type and process_id
            files_need_extraction_pd = pd.DataFrame(files_need_extraction_meta, columns=['file_path', 'document_type', 'process_id'])
            files_need_extraction_groups = files_need_extraction_pd.replace(np.nan, "").groupby(['document_type', 'process_id'])

            # Send each group in a different call
            for (doc_type, process_id) in files_need_extraction_groups.groups:
                files_group = files_need_extraction_groups.get_group((doc_type, process_id))['file_path'].tolist()

                # Params to fill call template
                request_params = {}
                request_params['process_id'] = process_id
                type_conf = conf_utils.get_type(doc_type, profile=request_json["client_profile"]['profile_name'])
                request_params['doc_type'] = doc_type
                request_params['folder'] = folder
                request_params['ocr'] = type_conf.get('_ocr', request_json['client_profile']['default_ocr'])
                request_params['force_ocr'] = type_conf.get('_force_ocr', True)
                request_params['need_preprocess'] = type_conf.get('_need_preprocess', True)
                request_params['params_for_extraction'] = metadata
                request_params['fields_to_extract'] = [field for field in type_conf.keys() if not field.startswith("_")]

                # Call extraction for current group type
                api_response = core_api.async_extraction_request(apigw_params, request_params, files_group)
                logger.info(f"- Calling {len(files_group)} ASYNC EXTRACTION {doc_type.upper()} for request '{request_json['integration_id']}' reusing dataset '{process_id}' {api_response}")

                # Update request with status and process_id
                process_id = api_response.get('process_id', "")
                process_ids[process_id] = api_response['status']

                # Update files with status and process_id
                for file_path in files_group:
                    file_name = docs_utils.parse_file_name(file_path, folder)
                    file_metadata = metadata.setdefault(file_name, {})
                    file_metadata.update(api_response)

                    # Save for traceability
                    file_metadata['ocr_used'] = request_params['ocr']
                    file_metadata['async'] = True

            # Check pending processes and continue if ready
            for process_id in [process_id for process_id, status in process_ids.items() if status == "waiting"]:
                status = process_ids[process_id]
                try:
                    while status == "waiting":
                        # Initialize time since start request
                        ts_now = datetime.now().timestamp()
                        status = core_api.async_status_request(apigw_params, process_id)['status']

                        # Check if time request is greater than timeout
                        if ts_now - ts_init >= request_polling_timeout:
                            msg_error = "Timeout expired calling the API service getting status"
                            logger.error(msg_error, exc_info=True)
                            status = "error"
                            for file_metadata in metadata.values():
                                if file_metadata.get('process_id', "") == process_id:
                                    file_metadata.update({'status': "error", 'error': "timeout"})
                            process_ids[process_id] = status
                            break

                        # Sleep time in seconds by global var
                        time.sleep(request_polling_sleep)
                except:
                    msg_error = f"Error getting status for request {request_json['integration_id']}"
                    logger.error(msg_error, exc_info=True)
                    request_json['process_ids'][process_id] = "error"
                    for file_metadata in metadata.values():
                        if file_metadata.get('process_id', "") == process_id:
                            file_metadata.update({'status': "error", 'error': msg_error})

                try:
                    if status != "error":
                        # Get result of each doc processed with this process_id
                        results = core_api.async_result_request(apigw_params, process_id)['results']
                        logger.info(f"- Getting {len(results)} ASYNC EXTRACTION results for request '{request_json['integration_id']}'")

                        for result in results:
                            file_name = docs_utils.parse_file_name(result['filename'], folder)
                            file_metadata = metadata.setdefault(file_name, {})

                            # Normalize results to JSON format
                            for key, value in result['entities'].items():
                                # Convert field into dictionary or list if possible
                                try:
                                    if type(value) == str and ('{' in value or '[' in value):
                                        result['entities'][key] = json.loads(value.replace("'", "\"").replace("None", "null"))
                                except:
                                    pass

                                # Parse value to value-confidence format if necessary
                                if type(value) != dict or 'confidence' not in value:
                                    result['entities'][key] = {'value': value, 'confidence': 0.99}

                            file_metadata['document_fields'] = result['entities']

                            # Change to already processed
                            file_metadata['status'] = "finish"

                        # Change process status to finish
                        request_json['process_ids'][process_id] = "finish"
                except:
                    msg_error = f"Error getting result for request {request_json['integration_id']}"
                    logger.error(msg_error, exc_info=True)
                    request_json['process_ids'][process_id] = "error"
                    for file_metadata in metadata.values():
                        if file_metadata.get('process_id', "") == process_id:
                            file_metadata.update({'status': "error", 'error': msg_error})

    return request_json

def preprocess(request_json: dict) -> dict:
    """ Process request with step preprocess in any mode,
    collect documents to process without classification,
    send to process needed documents in async or queue mode,
    check pending processes and finish if ready

    :param request_json: Request JSON with all information
    :return: Request JSON with all information
    """
    apigw_params = request_json['apigw_params']
    folder = request_json['documents_folder']
    files = request_json.get('documents', [])
    metadata = request_json.setdefault('documents_metadata', {})
    files_need_preprocess = []
    process_id = request_json.get('input_json', {}).get('process_id')

    if files:
        for file_path in files:
            file_name = docs_utils.parse_file_name(file_path, folder)
            file_metadata = metadata.get(file_name, {})

            # Not processing and not finish with error
            if file_metadata.get('status', "") not in ["waiting", "ready", "error"]:
                files_need_preprocess.append(file_path)

        if files_need_preprocess:
            request_params = {
                'folder': folder,
                'ocr': request_json.get('preprocess_conf', {}).get('ocr_conf', {}).get('ocr', request_json['client_profile']['default_ocr']),
                'force_ocr': request_json.get('preprocess_conf', {}).get('ocr_conf', {}).get('force_ocr', request_json['client_profile'].get('force_ocr', False)),
                'timeout': round(len(files) * request_json['client_profile'].get('request_flow_timeout', request_flow_timeout), 0),
                'preprocess_conf': request_json.get('preprocess_conf', {}),
                'integration': request_json,
                'tracking': request_json.get('tracking', {})
            }

            if process_id:
                request_params['process_id'] = process_id
                request_params['preprocess_conf']['preprocess_reuse'] = True
            else:
                request_params['process_id'] = f"preprocess_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}_{''.join([random.choice(string.ascii_lowercase + string.digits) for i in range(6)])}"

            # Check if we should use queue or async mode
            if os.getenv('CORE_QUEUE_PROCESS_URL', ""):
                api_response = core_api.queue_preprocess_request(apigw_params, request_params, files_need_preprocess)
                logger.info(f"- Calling {len(files_need_preprocess)} QUEUE PREPROCESS for request '{request_json['integration_id']}' {api_response}")
            else:
                api_response = core_api.async_preprocess_request(apigw_params, request_params, files_need_preprocess)
                logger.info(f"- Calling {len(files_need_preprocess)} ASYNC PREPROCESS for request '{request_json['integration_id']}' {api_response}")

            process_id = api_response.get('process_id', "")
            process_ids = request_json.setdefault('process_ids', {})
            process_ids[process_id] = api_response['status']

            # Update files with status and process_id
            for file_path in files:
                file_name = docs_utils.parse_file_name(file_path, folder)
                file_metadata = metadata.setdefault(file_name, {})
                file_metadata.update(api_response)
                file_metadata['ocr_used'] = request_params['ocr']
                file_metadata['async'] = "queue" if os.getenv('CORE_QUEUE_PROCESS_URL', "") else True

        # Process ready or error status
        for process_id in [process_id for process_id, status in request_json.get('process_ids', {}).items() if status in ["ready", "error"]]:
            for file_metadata in metadata:
                if metadata[file_metadata]['process_id'] == process_id:
                    metadata[file_metadata]['status'] = metadata[file_metadata]['status'].replace("ready", "finish")

            request_json['process_ids'][process_id] = request_json['process_ids'][process_id].replace("ready", "finish")

        request_json['status'] = "waiting" if "waiting" in request_json.get('process_ids', {}).values() else "processing"
        request_json['status'] = "error" if "error" in request_json.get('process_ids', {}).values() else request_json['status']
    else:
        logger.error("No files to preprocess")
        request_json['status'] = "error"  # Break flow here to avoid errors

    # Set status to finish
    if request_json['input_json']['operation'] == "preprocess" and request_json['status'] == "processing":
        request_json['status'] = "finish"

    return request_json

def classification_async(request_json: dict) -> dict:
    """ Process request with step classification in async mode,
    collect documents to process without classification,
    send to process needed documents in async mode,
    check pending processes and finish if ready

    :param request_json: Request JSON with all information
    :return: Request JSON with all information
    """
    files_need_classification = []
    apigw_params = request_json['apigw_params']
    folder = request_json['documents_folder']
    files = request_json.get('documents', [])
    metadata = request_json.setdefault('documents_metadata', {})
    model_conf = conf_utils.get_model(request_json['client_profile']['model'])

    # Collect docs to process
    for file_path in files:
        file_name = docs_utils.parse_file_name(file_path, folder)
        file_metadata = metadata.get(file_name, {})

        if file_metadata:
            # Already processed or type from client
            if any([True for key in ['document_type', 'categories'] if key in file_metadata]):
                if 'document_type' in file_metadata:
                    # If type from client set max confidence
                    file_metadata.setdefault('document_type_confidence', 1.0)
            else:
                # Not processing and not finish with error
                if file_metadata.get('status', "") not in ["waiting", "ready", "error"]:
                    files_need_classification.append(file_path)
        else:
            files_need_classification.append(file_path)

    # Send to process needed files
    if files_need_classification:
        # Params to fill call template
        request_params = {}
        request_params['folder'] = folder
        request_params['ocr'] = request_json.get('preprocess_conf', {}).get('ocr_conf', {}).get('force_ocr', request_json['client_profile']['default_ocr'])
        request_params['force_ocr'] = request_json.get('preprocess_conf', {}).get('ocr_conf', {}).get('force_ocr', request_json['client_profile'].get('force_ocr', False))
        request_params['timeout'] = round(len(files) * request_json['client_profile'].get('request_flow_timeout', request_flow_timeout), 0)
        request_params['tracking'] = request_json.get('tracking', {})
        request_params.update(model_conf)
        doc_type = (list(metadata.values())[0] if metadata else {}).get('document_type_expected', "")
        if doc_type:
            type_conf = conf_utils.get_type(doc_type, profile=request_json["client_profile"]['profile_name'])
            request_params['ocr'] = type_conf.get('_ocr', request_params['ocr'])
            request_params['force_ocr'] = type_conf.get('_force_ocr', True)
            request_params['need_preprocess'] = type_conf.get('_need_preprocess', True)

        # Call corresponding classification
        if model_conf.get('multilabel', False):
            api_response = core_api.async_classification_multilabel_request(apigw_params, request_params, files_need_classification)
            logger.info(f"- Calling {len(files_need_classification)} ASYNC CLASSIFICATION MULTILABEL for request '{request_json['integration_id']}' {api_response}")
        else:
            api_response = core_api.async_classification_multiclass_request(apigw_params, request_params, files_need_classification)
            logger.info(f"- Calling {len(files_need_classification)} ASYNC CLASSIFICATION MULTICLASS for request '{request_json['integration_id']}' {api_response}")

        # Update request with status and process_id
        process_id = api_response.get('process_id', "")
        process_ids = request_json.setdefault('process_ids', {})
        process_ids[process_id] = api_response['status']

        # Update files with status and process_id
        for file_path in files_need_classification:
            file_name = docs_utils.parse_file_name(file_path, folder)
            file_metadata = metadata.setdefault(file_name, {})
            file_metadata.update(api_response)

            # Save for traceability and compare in extraction
            file_metadata['ocr_used'] = request_params['ocr']
            file_metadata['async'] = True

    # Check pending processes and continue if ready
    for process_id in [process_id for process_id, status in request_json.get('process_ids', {}).items() if status == "ready"]:
        try:
            # Get result of each doc processed with this process_id
            results = core_api.async_result_request(apigw_params, process_id)['results']
            logger.info(f"- Getting {len(results)} ASYNC CLASSIFICATION results for request '{request_json['integration_id']}'")
            for result in results:
                file_name = docs_utils.parse_file_name(result['filename'], folder)
                file_metadata = metadata.setdefault(file_name, {})

                # Set corresponding data
                if model_conf.get('multilabel', False):
                    file_metadata['categories'] = result['categories']
                else:
                    file_metadata['categories'] = result['categories']
                    file_metadata['document_type'] = result['categories'][0]['category']
                    file_metadata['document_type_confidence'] = result['categories'][0]['confidence']

                # Change to already processed
                file_metadata['status'] = "finish"

            # Change process status to finish
            request_json['process_ids'][process_id] = "finish"
        except:
            msg_error = f"Error getting result for request {request_json['integration_id']}"
            logger.error(msg_error, exc_info=True)
            request_json['process_ids'][process_id] = "error"
            for file_metadata in metadata.values():
                if file_metadata.get('process_id', "") == process_id:
                    file_metadata.update({'status': "error", 'error': msg_error})

    # Update request status
    request_json['status'] = "waiting" if "waiting" in request_json.get('process_ids', {}).values() else "processing"

    return request_json

def extraction_async(request_json: dict) -> dict:
    """ Process request with step extraction in async mode,
    collect documents to process without extraction,
    send to process needed documents in async mode,
    check pending processes and finish if ready

    :param request_json: Request JSON with all information
    :return: Request JSON with all information
    """
    files_need_extraction = []
    apigw_params = request_json['apigw_params']
    folder = request_json['documents_folder']
    files = request_json.get('documents', [])
    metadata = request_json.setdefault('documents_metadata', {})

    # Collect docs to process
    for file_path in files:
        file_name = docs_utils.parse_file_name(file_path, folder)
        file_metadata = metadata.get(file_name, {})

        if file_metadata:
            # Check if has type for extraction
            if file_metadata.get('document_type', ""):
                # Not already processed
                if 'document_fields' not in file_metadata:
                    # Not processing and not finish with error
                    if file_metadata.get('status', "") not in ["waiting", "ready", "error"]:
                        files_need_extraction.append(file_path)
            else:
                file_metadata.update({'status': "error", 'error': "Param 'document_type' required for extraction"})
        else:
            file_metadata.update({'status': "error", 'error': "Param 'document_type' required for extraction"})

    # Send to process needed files
    if files_need_extraction:
        # Parse list of paths in list of {file_path, document_type, process_id}
        files_need_extraction_meta = []
        for file_path in files_need_extraction:
            file_name = docs_utils.parse_file_name(file_path, folder)
            file_metadata = metadata.setdefault(file_name, {})
            doc_type = file_metadata['document_type']

            # Check if ocr_used is the same for extraction to reuse process_id
            if file_metadata.get('process_id', ""):
                type_conf = conf_utils.get_type(doc_type, profile=request_json["client_profile"]['profile_name'])
                type_ocr = type_conf.get('_ocr', request_json['client_profile']['default_ocr'])

                if file_metadata.get('ocr_used', "") != type_ocr:
                    file_metadata.pop('process_id', "")

            # Get metadata fields for the dict list
            files_need_extraction_meta.append({'file_path': file_path, 'document_type': doc_type, 'process_id': file_metadata.get('process_id', "")})

        # Group files by document_type and process_id
        files_need_extraction_pd = pd.DataFrame(files_need_extraction_meta, columns=['file_path', 'document_type', 'process_id'])
        files_need_extraction_groups = files_need_extraction_pd.replace(np.nan, "").groupby(['document_type', 'process_id'])

        # Send each group in a different call
        for (doc_type, process_id) in files_need_extraction_groups.groups:
            files_group = files_need_extraction_groups.get_group((doc_type, process_id))['file_path'].tolist()

            # Params to fill call template
            request_params = {}
            request_params['process_id'] = process_id
            type_conf = conf_utils.get_type(doc_type, profile=request_json["client_profile"]['profile_name'])
            request_params['doc_type'] = doc_type
            request_params['folder'] = folder
            request_params['ocr'] = type_conf.get('_ocr', request_json['client_profile']['default_ocr'])
            request_params['force_ocr'] = type_conf.get('_force_ocr', True)
            request_params['timeout'] = round(len(files) * request_json['client_profile'].get('request_flow_timeout', request_flow_timeout), 0)
            request_params['need_preprocess'] = type_conf.get('_need_preprocess', True)
            request_params['params_for_extraction'] = metadata
            request_params['fields_to_extract'] = [field for field in type_conf.keys() if not field.startswith("_")]
            request_params['tracking'] = request_json.get('tracking', {})

            # Call extraction for current group type
            api_response = core_api.async_extraction_request(apigw_params, request_params, files_group)
            logger.info(f"- Calling {len(files_group)} ASYNC EXTRACTION {doc_type.upper()} for request '{request_json['integration_id']}' reusing dataset '{process_id}' {api_response}")

            # Update request with status and process_id
            process_id = api_response.get('process_id', "")
            process_ids = request_json.setdefault('process_ids', {})
            process_ids[process_id] = api_response['status']

            # Update files with status and process_id
            for file_path in files_group:
                file_name = docs_utils.parse_file_name(file_path, folder)
                file_metadata = metadata.setdefault(file_name, {})
                file_metadata.update(api_response)

                # Save for traceability
                file_metadata['ocr_used'] = request_params['ocr']
                file_metadata['async'] = True

    # Check pending processes and continue if ready
    for process_id in [process_id for process_id, status in request_json.get('process_ids', {}).items() if status == "ready"]:
        try:
            # Get result of each doc processed with this process_id
            results = core_api.async_result_request(apigw_params, process_id)['results']
            logger.info(f"- Getting {len(results)} ASYNC EXTRACTION results for request '{request_json['integration_id']}'")
            for result in results:
                file_name = docs_utils.parse_file_name(result['filename'], folder)
                file_metadata = metadata.setdefault(file_name, {})

                # Normalize results to JSON format
                for key, value in result['entities'].items():
                    # Convert field into dictionary or list if possible
                    try:
                        if type(value) == str and ('{' in value or '[' in value):
                            result['entities'][key] = json.loads(value.replace("'", "\"").replace("None", "null"))
                    except:
                        pass

                    # Parse value to value-confidence format if necessary
                    if type(value) != dict or 'confidence' not in value:
                        result['entities'][key] = {'value': value, 'confidence': 0.99}

                # Set corresponding data
                file_metadata['document_fields'] = result['entities']

                # Change to already processed
                file_metadata['status'] = "finish"

            # Change process status to finish
            request_json['process_ids'][process_id] = "finish"
        except:
            msg_error = f"Error getting result for request {request_json['integration_id']}"
            logger.error(msg_error, exc_info=True)
            request_json['process_ids'][process_id] = "error"
            for file_metadata in metadata.values():
                if file_metadata.get('process_id', "") == process_id:
                    file_metadata.update({'status': "error", 'error': msg_error})

    # Update request status
    request_json['status'] = "waiting" if "waiting" in request_json.get('process_ids', {}).values() else "processing"

    return request_json

def indexing(request_json: dict) -> dict:
    """ Process request with step indexing in any mode,
    collect documents to process without indexing,
    send to process needed documents in any mode,
    use the corresponding mode (async, queue),
    check pending processes and finish if ready

    :param request_json: Request JSON with all information
    :return: Request JSON with all information
    """
    apigw_params = request_json['apigw_params']
    folder = request_json['documents_folder']
    models = request_json['indexation_conf']['models']
    files = request_json.get('documents', [])
    metadata = request_json.setdefault('documents_metadata', {})
    models_map = json.loads(open(f"{conf_utils.custom_folder}models_map.json", 'r').read())
    files_need_indexing = []
    process_id = request_json.get('input_json', {}).get('process_id') 

    if files:
        for file_path in files:
            file_name = docs_utils.parse_file_name(file_path, folder)
            file_metadata = metadata.get(file_name, {})

            # Not processing and not finish with error
            if file_metadata.get('status', "") not in ["waiting", "ready", "error"]:
                files_need_indexing.append(file_path)

        if files_need_indexing:
            request_params = {
                'folder': folder,
                'timeout': round(len(files) * request_json['client_profile'].get('request_flow_timeout', request_flow_timeout), 0),
                'indexation_conf': request_json['indexation_conf'],
                'preprocess_conf': request_json['preprocess_conf'],
                'integration': request_json,
                'tracking': request_json.get('tracking', {})
            }

            if process_id:
                request_params['process_id'] = process_id
                request_params['preprocess_conf']['preprocess_reuse'] = True
            else:
                request_params['process_id'] = f"ir_index_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}_{''.join([random.choice(string.ascii_lowercase + string.digits) for i in range(6)])}"
                
            request_params['indexation_conf']['models'] = [models_map[model] for model in models]
            request_params['preprocess_conf'].setdefault('ocr_conf', {}).setdefault('ocr', request_json['client_profile']['default_ocr'])
            request_params['preprocess_conf'].setdefault('ocr_conf', {}).setdefault('force_ocr', request_json['client_profile'].get('force_ocr', False))


            if os.getenv('CORE_QUEUE_PROCESS_URL', ""):
                api_response = core_api.queue_indexing_request(apigw_params, request_params, files_need_indexing)
                logger.info(f"- Calling {len(files_need_indexing)} QUEUE INDEXING for request '{request_json['integration_id']}' for index '{request_params['indexation_conf']['vector_storage_conf']['index']}' {api_response}")
            else:
                api_response = core_api.async_indexing_request(apigw_params, request_params, files_need_indexing)
                logger.info(f"- Calling {len(files_need_indexing)} ASYNC INDEXING for request '{request_json['integration_id']}' for index '{request_params['indexation_conf']['vector_storage_conf']['index']}' {api_response}")

            process_id = api_response.get('process_id', "")
            process_ids = request_json.setdefault('process_ids', {})
            process_ids[process_id] = api_response['status']

            # Update files with status and process_id
            for file_path in files:
                file_name = docs_utils.parse_file_name(file_path, folder)
                file_metadata = metadata.setdefault(file_name, {})
                file_metadata.update(api_response)
                file_metadata['ocr_used'] = request_params['preprocess_conf']['ocr_conf']['ocr']
                file_metadata['async'] = "queue" if os.getenv('CORE_QUEUE_PROCESS_URL', "") else True

        for process_id in [process_id for process_id, status in request_json.get('process_ids', {}).items() if status in ["ready", "error"]]:
            for file_metadata in metadata:
                if metadata[file_metadata]['process_id'] == process_id:
                    metadata[file_metadata]['status'] = metadata[file_metadata]['status'].replace("ready", "finish")

            request_json['process_ids'][process_id] = request_json['process_ids'][process_id].replace("ready", "finish")

        request_json['status'] = "waiting" if "waiting" in request_json.get('process_ids', {}).values() else "processing"
        request_json['status'] = "error" if "error" in request_json.get('process_ids', {}).values() else request_json['status']
    else:
        logger.error("No files to index")
        request_json['status'] = "error" # Break flow here to avoid error in preprocess/indexing services

    return request_json

def delete(request_json: dict) -> dict:
    """ Delete request results generated in any mode,
    use the corresponding mode (sync, async, queue)

    :param request_json: Request JSON with all information
    :return: Request JSON with all information
    """
    for process_id in request_json.get('process_ids', []):
        if process_id:
            metadata = request_json.get('documents_metadata', {})
            is_async = "unknown"
            for key in metadata:
                is_async = metadata[key].get('async', "unknown")
                break

            if is_async == "queue":
                logger.debug(f"Deleting queue results from {process_id}")
                core_api.queue_delete_request(request_json['apigw_params'], process_id, request_json['tracking'])
            elif is_async or is_async == "unknown":
                logger.debug(f"Deleting async results from {process_id}")
                core_api.async_delete_request(request_json['apigw_params'], process_id, request_json['tracking'])
            elif not is_async or is_async == "unknown":
                logger.debug(f"Deleting sync results from {process_id}")
                core_api.sync_delete_request(request_json['apigw_params'], process_id, request_json['tracking'])

    return request_json