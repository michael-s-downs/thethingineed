### This code is property of the GGAO ###


# Native imports
import os
import re
import json
import requests
import tempfile
from datetime import datetime

# Custom imports
import provider_resources
from logging_handler import logger
from genai_controllers import storage_containers, list_files, download_file


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
            if "Documents not found for filters:" not in response_json.get('error_message', ""):
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

def download_preprocess_data(request_json: dict) -> dict:
    """
    Downloads preprocess data based on a process_id from request_json
    
    :param request_json: Request JSON with all information including process_id
    :return: Request JSON with download results
    """
    try:
        # Extract parameters from request
        process_id = request_json['input_json'].get('process_id')
        cells_param = request_json['input_json'].get('cells', 'false')
        include_cells = cells_param.lower() == 'true'
        department = request_json.get('apigw_params', {}).get('x-department', 'main')
        
        # Call the original download function logic
        result = {
            'status': 'ok',
            'text': {
                'full_document': {},
                'pages': {}
            },
            'cells': {
                'words': [],
                'paragraphs': [],
                'lines': []
            }
        }
        
        workspace = storage_containers['workspace']
        
        # Find request_id and list files 
        first_level_files = list_files(workspace, f"{department}/{process_id}/")
        
        if not first_level_files:
            raise Exception(f"Process ID '{process_id}' not found")
        
        request_id = None
        for file_path in first_level_files:
            for part in file_path.split('/'):
                if part.startswith("request_"):
                    request_id = part
                    break
            if request_id:
                break
        
        if not request_id:
            raise Exception(f"Could not find request_id for process_id: {process_id}")
        
        base_path = f"{department}/{process_id}/{request_id}"
        all_files = list_files(workspace, base_path)
        
        # Read file content
        def read_file(file_path):
            temp_file = os.path.join(tempfile.gettempdir(), os.path.basename(file_path))
            download_file(workspace, (file_path, temp_file))
            with open(temp_file, 'r', encoding='utf-8') as f:
                content = f.read()
            os.remove(temp_file)
            return content.replace('\n', '\\n\\n')
        
        # Determine structure
        has_text_txt = any("/text/txt/" in f for f in all_files)
        
        # Sort files by type 
        full_document_files = []
        pages_files = []
        words_files = []
        paragraphs_files = []
        lines_files = []
        
        if has_text_txt:
            # text/txt
            for file_path in all_files:
                if file_path.endswith(".txt"):
                    if "/text/txt/" in file_path and not "/pags/" in file_path:
                        full_document_files.append(file_path)
                    elif "/text/txt/pags/" in file_path:
                        pages_files.append(file_path)
        else:
            # text/ocr 
            for file_path in all_files:
                if file_path.endswith(".txt"):
                    if "/text/ocr/" in file_path and not "/pags/" in file_path:
                        full_document_files.append(file_path)
                    elif "/text/ocr/pags/" in file_path:
                        pages_files.append(file_path)
        
        # Cells files
        if include_cells:
            for file_path in all_files:
                if file_path.endswith(".txt") and "/cells/txt/" in file_path:
                    if "_words.txt" in file_path:
                        words_files.append(file_path)
                    elif "_paragraphs.txt" in file_path:
                        paragraphs_files.append(file_path)
                    elif "_lines.txt" in file_path:
                        lines_files.append(file_path)
        
        # Process full document files
        for file_path in full_document_files:
            try:
                file_name = os.path.basename(file_path)
                result['text']['full_document'][file_name] = read_file(file_path)
                logger.info(f"Added full document: {file_name}")
            except Exception as e:
                logger.warning(f"Error reading file: {str(e)}")
        
        # Process pages files
        if pages_files:
            logger.info("Added pages")
            for file_path in pages_files:
                try:
                    file_name = os.path.basename(file_path)
                    page_match = re.search(r'_pag_(\d+)', file_name)
                    page_key = page_match.group(1) if page_match else file_name
                    result['text']['pages'][page_key] = read_file(file_path)
                except Exception as e:
                    logger.warning(f"Error reading file: {str(e)}")
        
        # Process cells files if requested
        if include_cells:
            # Process words 
            for file_path in words_files:
                try:
                    content = read_file(file_path)
                    result['cells']['words'] = json.loads(content)
                    logger.info("Added words")
                except Exception as e:
                    logger.warning(f"Error reading words file: {str(e)}")
            
            # Process paragraphs 
            for file_path in paragraphs_files:
                try:
                    content = read_file(file_path)
                    result['cells']['paragraphs'] = json.loads(content)
                    logger.info("Added paragraphs")
                except Exception as e:
                    logger.warning(f"Error reading paragraphs file: {str(e)}")
            
            # Process lines 
            for file_path in lines_files:
                try:
                    content = read_file(file_path)
                    result['cells']['lines'] = json.loads(content)
                    logger.info("Added lines")
                except Exception as e:
                    logger.warning(f"Error reading lines file: {str(e)}")
        
        # Update request_json with results
        request_json['status'] = 'finish'
        request_json['download_result'] = result
        
        logger.info(f"---- Download preprocess data successful for process_id '{process_id}'")
        
    except Exception as e:
        logger.error(f"Error downloading preprocess data: {str(e)}", exc_info=True)
        request_json['status'] = 'error'
        request_json['error'] = str(e)
    
    return request_json