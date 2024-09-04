### This code is property of the GGAO ###


# Native imports
import os
import re
import json
import requests
from datetime import datetime

# Custom imports
import provider_resources
from logging_handler import logger


def send_any_message(url: str, message: dict) -> bool:
    """ Send message via queue or API

    :param url: URL to send message
    :param message: JSON message to send
    :return: True or False if send ok
    """
    valid = True

    # Detect if URL is a queue: if not start with http or https, or start with https://sqs, or contain --q- or --Q-
    is_queue = re.match(r"(^(?!.*https?://))|https://sqs.*|.*--[qQ]-.*", url)

    if is_queue:
        try:
            provider_resources.qc.set_credentials((provider_resources.provider, url), url=url)

            logger.debug(f"-- Sending message via queue: {message}")
            if not provider_resources.queue_write_message(message, url):
                raise Exception(f"Unable to write in queue '{url}'")

            logger.debug(f"-- Message sent via queue to '{url}'")
        except:
            valid = False
            logger.error(f"-- Unable to send message via queue to '{url}'")
    else:
        try:
            logger.debug(f"-- Sending message via API: {message}")
            requests.post(url, json=message, verify=False)

            logger.debug(f"-- Message sent via API to '{url}'")
        except:
            valid = False
            logger.error(f"-- Unable to send message via API to '{url}'")

    return valid

def generate_tracking_message(request_json: dict, service_name: str, tracking_type: str) -> dict:
    """ Add tracking step to pipeline

    :param request_json: Request JSON with all information
    :param service_name: Service name to add step to pipeline
    :param tracking_type: Tracking type INPUT or OUTPUT
    :return: Request JSON with all information
    """
    tracking_request = request_json.setdefault('tracking', {})
    tracking_request.setdefault('request_id', request_json['input_json'].get('request_id', request_json['integration_id']))
    pipeline = tracking_request.setdefault('pipeline', [])

    # Avoid to repeat step
    last_step = pipeline[-1] if pipeline else {}
    new_step = {'ts': round(datetime.now().timestamp(), 3), 'step': service_name.upper(), 'type': tracking_type}

    # Update ts if already inserted (some services do it inside process)
    if last_step and last_step['step'] == new_step['step'] and last_step['type'] == new_step['type']:
        last_step.update(new_step)
    else:
        pipeline.append(new_step)

    return request_json

def send_tracking_message(request_json: dict, service_name: str, tracking_type: str) -> dict:
    """ Add tracking step to pipeline and send tracking message if enabled

    :param request_json: Request JSON with all information
    :param service_name: Service name to add step to pipeline
    :param tracking_type: Tracking type INPUT or OUTPUT
    :return: Request JSON with all information
    """
    url = os.getenv(f'TRACKING_{tracking_type}_URL', "")
    request_json = generate_tracking_message(request_json, service_name, tracking_type)

    if url:
        logger.info(f"---- Sending tracking {tracking_type} message to '{url}'")
        send_any_message(url, request_json['tracking'])

    return request_json

def send_tracking_input(request_json: dict) -> dict:
    """ Init logic for profile,
    send tracking input message

    :param request_json: Request JSON with all information
    :return: Request JSON with all information
    """
    request_json = send_tracking_message(request_json, "integration_sender", "INPUT")

    return request_json

def send_tracking_output(request_json: dict) -> dict:
    """ Finally logic for profile,
    send tracking output message

    :param request_json: Request JSON with all information
    :return: Request JSON with all information
    """
    request_json = send_tracking_message(request_json, "integration_sender", "OUTPUT")

    return request_json

def no_timeout_response(request_json: dict) -> dict:
    """ Error logic for profile,
    no response if error is timeout

    :param request_json: Request JSON with all information
    :return: Request JSON with all information
    """
    if request_json.get('error', "") == "timeout":
        request_json['client_profile']['custom_functions'].pop('response_async', "")

    return request_json

def sync_infodelete_request(apigw_params: dict, request_params: dict) -> bool:
    """ Send request to delete indexed documents

    :param apigw_params: Params from apigateway
    :param request_params: Params to fill JSON request
    :return: True or False if delete is successfully
    """
    url = os.getenv('API_SYNC_INFODELETE_URL')
    url = f"http://{url}" if "http" not in url else url

    try:
        logger.debug("Calling API sync infodelete service")
        response = requests.post(url, headers=apigw_params, data=json.dumps(request_params))
        response_json = response.json()

        if type(response_json) != dict or 'status' not in response_json or response_json['status'] != "finished":
            raise Exception(f"Bad response from the API service '{url}'")

        status = True
    except:
        status = False
        logger.error("Error calling API sync infodelete service", exc_info=True)

    return status

def infodelete_sync(request_json: dict) -> dict:
    """ Process request with step infodelete in sync mode

    :param request_json: Request JSON with all information
    :return: Request JSON with all information
    """
    apigw_params = request_json['apigw_params']
    request_params = {'index': request_json['input_json']['index'], 'delete': request_json['input_json']['delete']}

    logger.info(f"- Calling SYNC INFODELETE for index '{request_params['index']}'")
    api_response = sync_infodelete_request(apigw_params, request_params)

    request_json['status'] = "finished" if api_response else "error"

    return request_json