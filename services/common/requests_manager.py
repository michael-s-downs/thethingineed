### This code is property of the GGAO ###


# Native imports
import os
import json
import random
import string
from copy import deepcopy
from datetime import datetime

# Custom imports
import conf_utils
import docs_utils
import provider_resources
from logging_handler import logger


# Global vars
current_requests = {}
current_processes_map = {}
storage_delete_request = eval(os.getenv('STORAGE_DELETE_REQUEST', "True"))
storage_persist_request = eval(os.getenv('STORAGE_PERSIST_REQUEST', "True"))
pointers_folder = os.getenv('STORAGE_PERSIST_FOLDER').format(integration_name=os.getenv('INTEGRATION_NAME'))


def generate_request(apigw_params: dict, input_json: dict) -> dict:
    """ Create request JSON with necessary information

    :param apigw_params: Params from apigateway
    :param input_json: Input JSON from client
    :return: Request JSON with all information
    """
    current_time = datetime.now()
    request_json = {'ts_init': current_time.timestamp()}

    integration_id = f"{apigw_params['x-department']}/request_{current_time.strftime('%Y%m%d_%H%M%S_%f')}_{''.join([random.choice(string.ascii_lowercase + string.digits) for i in range(6)])}"
    request_json['integration_id'] = integration_id
    request_json['documents_folder'] = input_json.get('documents_folder', integration_id)

    request_json['apigw_params'] = apigw_params
    request_json['input_json'] = input_json

    request_json['client_profile'] = conf_utils.get_profile(apigw_params['x-department'], apigw_params['x-tenant'])
    logger.debug(f"Mapping profile '{apigw_params['x-department']}' -> '{request_json['client_profile']['profile_name']}'")

    return request_json

def persist_request(request_json: dict):
    """ Add request and process_ids to global vars,
    upload summary pointer and full JSON to storage

    :param request_json: Request JSON with all information
    """
    integration_id = request_json.get('integration_id', "")

    if integration_id:
        # Add to global vars
        logger.debug(f"Persisting request in memory '{integration_id}'")
        current_requests[integration_id] = request_json
        current_processes_map.update({process_id: integration_id for process_id in request_json.get('process_ids', {}).keys()})

        if storage_persist_request:
            logger.debug(f"Persisting request in storage '{integration_id}'")

            # Persist in storage full json inside department
            path = integration_id + "/full_request.json"
            provider_resources.storage_put_file(path, json.dumps(request_json), os.getenv('STORAGE_BACKEND'))

            # Persist in storage summary as pointer to full json path
            summary_data = {}
            summary_data['ts_init'] = request_json.get('ts_init', 0)
            summary_data['integration_id'] = request_json.get('integration_id', "")
            summary_data['documents_count'] = len(request_json.get('documents', []))
            summary_data['status'] = request_json.get('status', "error")

            path = pointers_folder + integration_id + ".json"
            provider_resources.storage_put_file(path, json.dumps(summary_data), os.getenv('STORAGE_BACKEND'))
    else:
        logger.error("Bad request, without the minimum information unable to process or respond")

def restore_requests():
    """ Add to global vars pending requests from storage
    """
    if storage_persist_request:
        # Avoid to restore completed requests not deleted in debug mode
        if storage_delete_request:
            list_files = provider_resources.storage_list_folder(pointers_folder, os.getenv('STORAGE_BACKEND'))
            logger.info(f"---- Restoring pending requests ({len(list_files)})")

            for file in list_files:
                integration_id = file.replace(pointers_folder, "").replace(".json", "")
                logger.debug(f"Restoring request '{integration_id}'")

                try:
                    # Get full json from storage
                    path = integration_id + "/full_request.json"
                    request_json = json.loads(provider_resources.storage_get_file(path, os.getenv('STORAGE_BACKEND')))

                    # Add to global vars
                    current_requests[integration_id] = request_json
                    current_processes_map.update({process_id: integration_id for process_id in request_json.get('process_ids', {}).keys()})
                except:
                    logger.warning(f"Unable to restore request '{integration_id}'")

def delete_request(integration_id: str):
    """ Delete request from global vars and
    summary pointer and full request from storage

    :param integration_id: ID of request
    """
    logger.debug("Deleting metadata from memory")

    # Delete request and process_ids from global vars
    request_json = current_requests.pop(integration_id, {})
    for process_id in request_json.get('process_ids', {}).keys():
        current_processes_map.pop(process_id, "")

    if storage_delete_request:
        logger.debug("Deleting metadata from container")

        container = os.getenv('STORAGE_BACKEND')
        pointer_file_path = pointers_folder + integration_id + ".json"

        # Delete pointer and request from storage
        provider_resources.storage_remove_files(pointer_file_path, container)
        provider_resources.storage_remove_files(integration_id, container)

def check_timeout(timeout: int):
    """ Change status to error if timeout expired

    :param timeout: Maximum time in minutes
    """
    ts_now = datetime.now().timestamp()

    # Use a copy to conserve size
    for integration_id in deepcopy(current_requests):
        if ts_now - current_requests[integration_id]['ts_init'] > timeout * 60:
            logger.debug(f"Request '{integration_id}' expired")

            current_requests[integration_id]['status'] = "error"
            current_requests[integration_id]['error'] = "timeout"

            # Discard if request already processed by other sender instance
            if not provider_resources.storage_validate_folder(integration_id, os.getenv('STORAGE_BACKEND')):
                delete_request(integration_id)

def update_request(response_json: dict):
    """ Change request status to continue
    with the corresponding process_id

    :param response_json: Response JSON from Dolffia
    """
    process_id = response_json.get('pid', "")
    request_json = response_json.get('integration', {})
    filename = response_json.get('filename', "")

    if process_id:

        if request_json:
            integration_id = request_json.get('integration_id', "")

            # Simulate status after send request to process
            process_ids = request_json.setdefault('process_ids', {})
            process_ids[process_id] = "waiting"
            request_json['status'] = "waiting"

            # Simulate metadata after send request to process
            if filename:
                filename = docs_utils.parse_file_name(filename, request_json['documents_folder'])

                file_metadata = request_json.get('documents_metadata', {}).setdefault(filename, {})
                file_metadata['ocr_used'] = request_json.get('client_profile', {}).get('default_ocr', "")
                file_metadata['async'] = "queue" if os.getenv('DOLFFIA_QUEUE_DELETE_URL', "") else True
                file_metadata['status'] = "waiting"
                file_metadata['process_id'] = process_id

            # Update tracking pipeline with process steps
            request_json.setdefault('tracking', {}).update(response_json.get('tracking', {}))

            # Add to global vars
            current_requests[integration_id] = request_json
            current_processes_map.update({process_id: integration_id for process_id in request_json.get('process_ids', {}).keys()})

        integration_id = current_processes_map.get(process_id, "")

        # Get request from other sender instance
        if not integration_id:
            restore_requests()
            integration_id = current_processes_map.get(process_id, "")

        if integration_id:
            request_json = current_requests[integration_id]

            # Change global status to continue
            request_json['status'] = "processing"

            # Change status to ready or error
            status = response_json['status']
            request_json['process_ids'][process_id] = status

            # Change doc status and insert error for traceability
            for doc in request_json.setdefault('documents_metadata', {}).values():
                if doc.get('process_id', "") == process_id:
                    doc['status'] = status

                    if response_json.get('error', ""):
                        doc['error'] = response_json['error']

            # Update info with new status
            persist_request(request_json)
        else:
            logger.warning(f"No request corresponding to process_id '{process_id}'")
    else:
        logger.warning("Bad response from Dolffia without process_id")
