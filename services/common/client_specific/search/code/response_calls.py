### This code is property of the GGAO ###


# Native imports
import os

# Installed imports
import requests

# Custom imports
import provider_resources
from logging_handler import logger
from custom_operations import send_any_message

def response_api_default(request_json: dict, result: dict, url: str="") -> dict:
    """ Send asynchronous response to client with the results via API

    :param request_json: Request JSON with all information
    :param result: Result parsed for client
    :param url: URL to send response
    :return: Request JSON with all information
    """
    url = url if url else request_json['response_url']
    url = f"http://{url}" if "http" not in url else url

    if not request_json.get('response_sent', False):
        try:
            logger.debug(f"Sending async response to '{url}'")
            requests.post(url, json=result, verify=False)

            request_json['response_sent'] = True
            logger.info(f"---- Response sent ({request_json['status'].upper()}) to '{url}' for request '{request_json['integration_id']}'")
        except:
            request_json['response_sent'] = False
            logger.warning(f"---- Unable to sent response to '{url}' for request '{request_json['integration_id']}'")

    return request_json

def response_queue_default(request_json: dict, result: dict, url: str="") -> dict:
    """ Send asynchronous response to client with the results via queue

    :param request_json: Request JSON with all information
    :param result: Result parsed for client
    :param url: URL to send response
    :return: Request JSON with all information
    """
    queue = url if url else request_json['response_url']

    if not request_json.get('response_sent', False):
        try:
            provider_resources.qc.set_credentials((provider_resources.provider, queue), url=queue)

            logger.debug(f"Inserting response in queue '{queue}'")
            if not provider_resources.queue_write_message(result, queue):
                raise Exception(f"Unable to write in queue '{queue}'")

            request_json['response_sent'] = True
            logger.info(f"---- Response sent ({request_json['status'].upper()}) to '{queue}' for request '{request_json['integration_id']}'")
        except:
            request_json['response_sent'] = False
            logger.warning(f"---- Unable to sent response to '{queue}' for request '{request_json['integration_id']}'")

    return request_json

def response_adaptive(request_json: dict, result: dict) -> dict:
    """ Response logic for profile,
    send async response with API or queue

    :param request_json: Request JSON with all information
    :param result: Result to send
    :return: Request JSON with all information
    """
    url = request_json.get('response_url', "")

    if url:
        status = request_json.get('status', "").upper()
        logger.info(f"---- Response sent ({status}) to '{url}' for request '{request_json['integration_id']}'")
        request_json['response_sent'] = send_any_message(url, result)
    else:
        logger.warning(f"---- No response URL for request '{request_json['integration_id']}'")
        request_json['response_sent'] = False
        
    return request_json
