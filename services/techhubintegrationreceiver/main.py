### This code is property of the GGAO ###


# Native imports
import os
import sys
import json

# Installed imports
from flask import Flask, request

# Custom imports
sys.path.append(os.getenv('LOCAL_COMMON_PATH'))
from integration_base import *


# Global vars
app = Flask(__name__)

with app.app_context():
    logger.info(f"---- Loading custom files ({os.getenv('INTEGRATION_NAME').upper()})")
    load_custom_files()

@app.route('/process-async', methods=['POST', 'GET'])
@app.route('/process', methods=['POST','GET'])
def process() -> str:
    """ Endpoint to receive requests from client,
    deserialize, convert and upload documents to storage,
    generate full request and insert in queue for sender

    :return: JSON response in plain text
    """
    logger.info("---- Request received")
    request_json, result = receive_request(request)

    logger.debug("Queuing request")
    if not provider_resources.queue_write_message(request_json):
        logger.error(f"Unable to write in queue '{provider_resources.queue_url}'")
        result = {'status': "error", 'error': "Internal error"}
        request_json.update(result)

    check_shutdown(request)

    logger.info(f"---- Response sent ({request_json['status'].upper()}) for request '{request_json['integration_id']}'")
    return json.dumps(result, sort_keys=False, ensure_ascii=False), 200 if result['status'] != "error" else 400, {'Content-Type': 'application/json'}

@app.route('/process-sync', methods=['POST', 'GET'])
def process_sync() -> str:
    """ Endpoint to receive requests from client,
    deserialize, convert and upload documents to storage,
    generate full request and process synchronously

    :return: JSON response in plain text
    """
    logger.info("---- Request received")
    request_json, result = receive_request(request)
    request_json, result = process_request(request_json)
    check_shutdown(request)
    logger.info(f"---- Response sent ({request_json['status'].upper()}) for request '{request_json['integration_id']}'")
    status_code = 200
    if result['status'] == "error":
        status_code = 404 if "not found" in result.get('error', '').lower() else 400

    return json.dumps(result, sort_keys=False, ensure_ascii=False), status_code, {'Content-Type': 'application/json'}
        
@app.route('/healthcheck', methods=['GET'])
def healthcheck() -> str:
    """ Endpoint to check if pod is running

    :return: JSON response in plain text
    """
    logger.info("---- Healthcheck received")
    return json.dumps({'status': "ok"}, sort_keys=False), 200, {'Content-Type': 'application/json'}

@app.route('/killcheck', methods=['GET'])
def killcheck() -> str:
    """ Endpoint to force shutdown check

    :return: JSON response in plain text
    """
    logger.info("---- Killcheck received")
    check_shutdown(request)
    return json.dumps({'status': "ok"}, sort_keys=False), 200, {'Content-Type': 'application/json'}

@app.route('/reloadconfig', methods=['GET'])
def reloadconfig() -> str:
    """ Endpoint to reload custom files located in storage

    :return: JSON response in plain text
    """
    logger.info(f"---- Reloading custom files ({os.getenv('INTEGRATION_NAME').upper()})")
    load_custom_files()
    return json.dumps({'status': "ok"}, sort_keys=False), 200, {'Content-Type': 'application/json'}

if __name__ == '__main__':
    logger.info(f"---- Launching service ({'NON ' if not requests_manager.storage_delete_request else ''}DELETE MODE)")
    app.run(host="0.0.0.0", debug=False, port=8888, use_reloader=False)
