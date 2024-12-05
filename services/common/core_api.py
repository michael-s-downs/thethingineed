### This code is property of the GGAO ###


# Native imports
import os
import json
import requests

# Installed imports
import pandas as pd

# Custom imports
import provider_resources
from logging_handler import logger


# Glovar vars
templates_path = f"{os.path.dirname(__file__)}/api_templates/"


def _generate_dataset(files: list, folder: str, metadata: dict = {}) -> str:
    """ Create CSV and upload to storage folder

    :param files: Files to include in dataset
    :param folder: Folder to allocate dataset
    :param metadata: Metadata to include in dataset
    :return: Path of dataset generated in storage
    """
    # Generate same dataset for same files in a subfolder to avoid list it
    dataset_path = f"{folder}/datasets/{hash(str(files))}.csv".replace("//", "/")

    dataset = pd.DataFrame(files, columns=["Url"])
    dataset['CategoryId'] = "Null"
    if metadata:
        for key, value in metadata.items():
            dataset[key] = value
    dataset = dataset.to_csv(sep=",", header=True, index=False)

    logger.debug(f"Uploading to container dataset '{dataset_path}'")
    provider_resources.storage_put_file(dataset_path, dataset, os.getenv('STORAGE_DATA'))

    return dataset_path

def _sync_preprocessing_request_generate(request_params: dict, request_file: str) -> dict:
    """ Fill JSON template to send sync preprocessing request

    :param request_params: Params to fill JSON template
    :param request_file: File to process
    :return: JSON to send request
    """
    template = json.loads(open(templates_path + "sync_preprocess.json", 'r').read())
    template['filename'] = request_file

    if 'force_ocr' in request_params:
        template['force_ocr'] = request_params['force_ocr']
    if 'ocr' in request_params:
        template['ocr_origin'] = request_params['ocr']
    if 'languages' in request_params:
        template['language'] = request_params['languages'][0]
    if request_params.get('tracking', {}):
        template['tracking'] = request_params['tracking']

    return template

def _sync_classification_multiclass_request_generate(request_params: dict, request_file: str) -> dict:
    """ Fill JSON template to send sync classification multiclass request

    :param request_params: Params to fill JSON template
    :param request_file: File to process
    :return: JSON to send request
    """
    template = json.loads(open(templates_path + "sync_classification.json", 'r').read())
    template['model'] = request_params['model_path']

    if 'process_id' in request_params:
        template['id_p'] = request_params['process_id']
    else:
        template['filename'] = request_file

    if 'force_ocr' in request_params:
        template['force_ocr'] = request_params['force_ocr']
    if 'ocr' in request_params:
        template['ocr_origin'] = request_params['ocr']
    if 'top_classes' in request_params:
        template['top_classes'] = request_params['top_classes']
    if 'languages' in request_params:
        template['language'] = request_params['languages'][0]
    if request_params.get('tracking', {}):
        template['tracking'] = request_params['tracking']

    return template

def _sync_extraction_request_generate(request_params: dict, request_file: str) -> dict:
    """ Fill JSON template to send sync extraction request

    :param request_params: Params to fill JSON template
    :param request_file: File to process
    :return: JSON to send request
    """
    template = json.loads(open(templates_path + "sync_extraction.json", 'r').read())
    template['id_type'] = request_params['doc_type']
    template['id_p'] = request_params['process_id']
    template['_filename'] = request_file
    
    if 'force_ocr' in request_params:
        template['force_ocr'] = request_params['force_ocr']
    if 'ocr' in request_params:
        template['ocr_origin'] = request_params['ocr']
    if request_params.get('fields_to_extract', []):
        template['fields'] = request_params['fields_to_extract']
    if request_params.get('params_for_extraction', {}):
        template['params_extraction'] = request_params['params_for_extraction']
    if request_params.get('tracking', {}):
        template['tracking'] = request_params['tracking']

    return template

def _async_preprocess_request_generate(request_params: dict, request_files: list) -> dict:
    """ Fill JSON template to send async preprocessing request

    :param request_params: Params to fill JSON template
    :param request_files: Files to process
    :return: JSON to send request
    """
    template = json.loads(open(templates_path + "async_preprocess.json", 'r').read())
    template['timeout_sender'] = request_params['timeout']
    template['dataset_conf']['dataset_path'] = request_params['folder']

    dataset_path = _generate_dataset(request_files, request_params['folder'])

    template['dataset_conf']['dataset_csv_path'] = dataset_path
    template['url_sender'] = provider_resources.queue_url

    if 'force_ocr' in request_params:
        template['force_ocr'] = request_params['force_ocr']
    if 'ocr' in request_params:
        template['origins']['ocr'] = request_params['ocr']
    if request_params.get('integration', {}):
        template['integration'] = request_params['integration']
    if request_params.get('tracking', {}):
        template['tracking'] = request_params['tracking']

    template['url_sender'] = provider_resources.queue_url

    return template

def _async_classification_multiclass_request_generate(request_params: dict, request_files: list) -> dict:
    """ Fill JSON template to send async classification multiclass request

    :param request_params: Params to fill JSON template
    :param request_files: Files to process
    :return: JSON to send request
    """
    template = json.loads(open(templates_path + "async_classification_multiclass.json", 'r').read())
    template['predict_conf']['models']['text'] = request_params['model_path']

    if not 'csv_method' in request_params:
        dataset_path = _generate_dataset(request_files, request_params['folder'])
    else:
        dataset_path = f"{request_files[0]}"
        template['csv'] = True

    template['dataset_conf']['dataset_csv_path'] = dataset_path
    template['dataset_conf']['dataset_path'] = request_params['folder']

    if 'force_ocr' in request_params:
        template['force_ocr'] = request_params['force_ocr']
    if 'ocr' in request_params:
        template['origins']['ocr'] = request_params['ocr']
    if 'languages' in request_params:
        template['languages'] = request_params['languages']
    if 'top_classes' in request_params:
        template['predict_conf']['top_classes'] = request_params['top_classes']
    if 'process_id' in request_params:
        template['dataset_conf']['dataset_id'] = request_params['process_id'].split(":")[-1]
    if request_params.get('integration', {}):
        template['integration'] = request_params['integration']
    if request_params.get('tracking', {}):
        template['tracking'] = request_params['tracking']

    template['url_sender'] = provider_resources.queue_url

    return template

def _async_classification_multilabel_request_generate(request_params: dict, request_files: list) -> dict:
    """ Fill JSON template to send async classification multilabel request

    :param request_params: Params to fill JSON template
    :param request_files: Files to process
    :return: JSON to send request
    """
    template = json.loads(open(templates_path + "async_classification_multilabel.json", 'r').read())
    template['dataset_conf']['multilabel_conf']['hierarchical_category_tree'] = request_params['tree_csv']
    template['predict_conf']['tree_conf']['tree_path'] = request_params['tree_csv']
    template['predict_conf']['tree_conf']['tree_id'] = request_params['model_path']

    if not 'csv_method' in request_params:
        dataset_path = _generate_dataset(request_files, request_params['folder'])
    else:
        dataset_path = f"{request_files[0]}"
        template['csv'] = True

    template['dataset_conf']['dataset_csv_path'] = dataset_path
    template['dataset_conf']['dataset_path'] = request_params['folder']

    if 'force_ocr' in request_params:
        template['force_ocr'] = request_params['force_ocr']
    if 'ocr' in request_params:
        template['origins']['ocr'] = request_params['ocr']
    if 'languages' in request_params:
        template['languages'] = request_params['languages']
    if 'top_classes' in request_params:
        template['predict_conf']['top_classes'] = request_params['top_classes']
    if 'process_id' in request_params:
        template['dataset_conf']['dataset_id'] = request_params['process_id'].split(":")[-1]
    if request_params.get('integration', {}):
        template['integration'] = request_params['integration']
    if request_params.get('tracking', {}):
        template['tracking'] = request_params['tracking']

    template['url_sender'] = provider_resources.queue_url

    return template

def _async_extraction_request_generate(request_params: dict, request_files: list) -> dict:
    """ Fill JSON template to send async extraction request

    :param request_params: Params to fill JSON template
    :param request_files: Files to process
    :return: JSON to send request
    """
    template = json.loads(open(templates_path + "async_extraction.json", 'r').read())
    template['extract_type'] = request_params['doc_type']

    if not 'csv_method' in request_params:
        dataset_path = _generate_dataset(request_files, request_params['folder'])
    else:
        dataset_path = f"{request_files[0]}"
        template['csv'] = True

    template['dataset_conf']['dataset_csv_path'] = dataset_path
    template['dataset_conf']['dataset_path'] = request_params['folder']

    if 'force_ocr' in request_params:
        template['force_ocr'] = request_params['force_ocr']
    if 'ocr' in request_params:
        template['origins']['ocr'] = request_params['ocr']
    if 'languages' in request_params:
        template['languages'] = request_params['languages']
    if request_params.get('process_id', ""):
        template['dataset_conf']['dataset_id'] = request_params['process_id'].split(":")[-1]
    if not request_params.get('need_preprocess', True):
        template['dataset_conf']['dataset_id'] = "0"
    if request_params.get('fields_to_extract', []):
        template['fields_to_extract'] = request_params['fields_to_extract']
    if request_params.get('params_for_extraction', {}):
        template['params_extraction'] = request_params['params_for_extraction']
    if request_params.get('integration', {}):
        template['integration'] = request_params['integration']
    if request_params.get('tracking', {}):
        template['tracking'] = request_params['tracking']

    template['url_sender'] = provider_resources.queue_url

    return template

def _async_indexing_request_generate(request_params: dict, request_files: list) -> dict:
    """ Fill JSON template to send async indexing request

    :param request_params: Params to fill JSON template
    :param request_files: Files to process
    :return: JSON to send request
    """
    template = json.loads(open(templates_path + "async_indexing.json", 'r').read())

    if not 'csv_method' in request_params:
        dataset_path = _generate_dataset(request_files, request_params['folder'], request_params['index_conf']['metadata'])
    else:
        dataset_path = f"{request_files[0]}"
        template['csv'] = True

    template['dataset_conf']['dataset_csv_path'] = dataset_path
    template['dataset_conf']['dataset_path'] = request_params['folder']
    template['index_conf'] = request_params['index_conf']

    if 'force_ocr' in request_params:
        template['force_ocr'] = request_params['force_ocr']
    if 'ocr' in request_params:
        template['origins']['ocr'] = request_params['ocr']
    if 'languages' in request_params:
        template['languages'] = request_params['languages']
    if 'timeout' in request_params:
        template['timeout_sender'] = request_params['timeout']
    if request_params.get('process_id', ""):
        template['dataset_conf']['dataset_id'] = request_params['process_id'].split(":")[-1]
    if request_params.get('layout_conf', {}):
        template['preprocess_conf']['layout_conf'] = request_params['layout_conf']
    if request_params.get('models', {}):
        template['index_conf']['models'] = request_params['models']
    if request_params.get('integration', {}):
        template['integration'] = request_params['integration']
    if request_params.get('tracking', {}):
        template['tracking'] = request_params['tracking']

    template['url_sender'] = provider_resources.queue_url

    return template

def _sync_preprocess_request(apigw_params: dict, request_params: dict, request_file: str) -> dict:
    """ Send request and get results for sync preprocess

    :param apigw_params: Params from apigateway
    :param request_params: Params to fill JSON template
    :param request_file: File to process
    :return: Results of process
    """
    result = {}
    url = os.getenv('API_SYNC_PREPROCESS_URL')
    url = f"http://{url}" if "http" not in url else url

    try:
        logger.debug("Calling API sync preprocess service")
        request = _sync_preprocessing_request_generate(request_params, request_file)
        response = requests.post(url, headers=apigw_params, json=request)

        if response.status_code != 200:
            raise Exception(f"Bad response (status={response.status_code}) from the API service '{url}'")

        result['process_id'] = response.json()['id_p']
        result['status'] = "ok"
    except:
        result['status'] = "error"
        logger.error("Error calling API sync preprocess service", exc_info=True)

    return result

def sync_classification_multiclass_request(apigw_params: dict, request_params: dict, request_file: str) -> dict:
    """ Send request and get results for sync classification multiclass

    :param apigw_params: Params from apigateway
    :param request_params: Params to fill JSON template
    :param request_file: File to process
    :return: Results of process
    """
    result = {}
    url = os.getenv('API_SYNC_CLASSIFY_URL')
    url = f"http://{url}" if "http" not in url else url
    
    try:
        preprocess = _sync_preprocess_request(apigw_params, request_params, request_file)
        request_params['process_id'] = preprocess.get('process_id', "")

        if preprocess['status'] == "error":
            raise Exception("Preprocessing error")

        logger.debug("Calling API sync classification service")
        request = _sync_classification_multiclass_request_generate(request_params, request_file)
        response = requests.post(url, headers=apigw_params, json=request)
        result['process_id'] = response.json()['id_p']

        if response.status_code != 200:
            raise Exception(f"Bad response (status={response.status_code}) from the API service '{url}'")

        result['status'] = "finish"
        result['result'] = response.json()['result']
    except:
        result['process_id'] = request_params.get('process_id', "")
        result['status'] = "error"
        logger.error("Error calling API sync classification service", exc_info=True)

    return result

def sync_extraction_request(apigw_params: dict, request_params: dict, request_file: str) -> dict:
    """ Send request and get results for sync extraction

    :param apigw_params: Params from apigateway
    :param request_params: Params to fill JSON template
    :param request_file: File to process
    :return: Results of process
    """
    result = {}
    url = os.getenv('API_SYNC_EXTRACT_URL')
    url = f"http://{url}" if "http" not in url else url

    try:
        # If need preprocess
        if not request_params.get('process_id', ""):
            preprocess = _sync_preprocess_request(apigw_params, request_params, request_file)
            request_params['process_id'] = preprocess.get('process_id', "")

            if preprocess['status'] == "error":
                raise Exception("Preprocessing error")

        logger.debug("Calling API sync extraction service")
        request = _sync_extraction_request_generate(request_params, request_file)
        response = requests.post(url, headers=apigw_params, json=request)
        result['process_id'] = request_params['process_id']

        if response.status_code != 200:
            raise Exception(f"Bad response (status={response.status_code}) from the API service '{url}'")

        result['status'] = "finish"
        result['result'] = response.json()['result']
    except:
        result['status'] = "error"
        logger.error("Error calling API sync extraction service", exc_info=True)

    return result

def async_preprocess_request(apigw_params: dict, request_params: dict, request_files: list) -> dict:
    """ Send request for async preprocess

    :param apigw_params: Params from apigateway
    :param request_params: Params to fill JSON template
    :param request_files: Files to process
    :return: Status of process
    """
    result = {}
    url = os.getenv('API_ASYNC_PROCESS_URL')
    url = f"http://{url}" if "http" not in url else url

    try:
        logger.debug("Calling API async preprocess service")
        request = _async_preprocess_request_generate(request_params, request_files)
        response = requests.post(url, headers=apigw_params, json=request)
        response_json = response.json()

        if type(response_json) != dict or 'dataset_status_key' not in response_json or response_json['dataset_status_key'] == "error":
            raise Exception(f"Bad response from the API service '{url}'")

        result['status'] = "waiting"
        result['process_id'] = response_json['dataset_status_key']
    except:
        result['status'] = "error"
        logger.error("Error calling API async preprocess service", exc_info=True)

    return result

def async_classification_multiclass_request(apigw_params: dict, request_params: dict, request_files: list) -> dict:
    """ Send request for async classification multiclass

    :param apigw_params: Params from apigateway
    :param request_params: Params to fill JSON template
    :param request_files: Files to process
    :return: Status of process
    """
    result = {}
    url = os.getenv('API_ASYNC_PROCESS_URL')
    url = f"http://{url}" if "http" not in url else url

    try:
        logger.debug("Calling API async classification service")
        request = _async_classification_multiclass_request_generate(request_params, request_files)
        response = requests.post(url, headers=apigw_params, json=request)
        response_json = response.json()

        if type(response_json) != dict or 'dataset_status_key' not in response_json or response_json['dataset_status_key'] == "error":
            raise Exception(f"Bad response from the API service '{url}'")

        result['status'] = "waiting"
        result['process_id'] = response_json['dataset_status_key']
    except:
        result['status'] = "error"
        logger.error("Error calling API async classification service", exc_info=True)

    return result

def async_classification_multilabel_request(apigw_params: dict, request_params: dict, request_files: list) -> dict:
    """ Send request for async classification multilabel

    :param apigw_params: Params from apigateway
    :param request_params: Params to fill JSON template
    :param request_files: Files to process
    :return: Status of process
    """
    result = {}
    url = os.getenv('API_ASYNC_PROCESS_URL')
    url = f"http://{url}" if "http" not in url else url

    try:
        logger.debug("Calling API async classification service")
        request = _async_classification_multilabel_request_generate(request_params, request_files)
        response = requests.post(url, headers=apigw_params, json=request)
        response_json = response.json()

        if type(response_json) != dict or 'dataset_status_key' not in response_json or response_json['dataset_status_key'] == "error":
            raise Exception(f"Bad response from the API service '{url}'")

        result['status'] = "waiting"
        result['process_id'] = response_json['dataset_status_key']
    except Exception as ex:
        result['status'] = "error"
        logger.error("Error calling API async classification service", exc_info=True)

    return result

def async_extraction_request(apigw_params: dict, request_params: dict, request_files: list) -> dict:
    """ Send request for async extraction

    :param apigw_params: Params from apigateway
    :param request_params: Params to fill JSON template
    :param request_files: Files to process
    :return: Status of process
    """
    result = {}
    url = os.getenv('API_ASYNC_PROCESS_URL')
    url = f"http://{url}" if "http" not in url else url

    try:
        logger.debug("Calling API async extraction service")
        request = _async_extraction_request_generate(request_params, request_files)
        response = requests.post(url, headers=apigw_params, json=request)
        response_json = response.json()

        if type(response_json) != dict or 'dataset_status_key' not in response_json or response_json['dataset_status_key'] == "error":
            raise Exception(f"Bad response from the API service '{url}'")

        result['status'] = "waiting"
        result['process_id'] = response_json['dataset_status_key']
    except Exception as ex:
        result['status'] = "error"
        logger.error("Error calling API async extraction service", exc_info=True)

    return result

def async_indexing_request(apigw_params: dict, request_params: dict, request_files: list) -> dict:
    """ Send request for async indexing

    :param apigw_params: Params from apigateway
    :param request_params: Params to fill JSON template
    :param request_files: Files to process
    :return: Status of process
    """
    result = {}
    url = os.getenv('API_ASYNC_PROCESS_URL')
    url = f"http://{url}" if "http" not in url else url

    try:
        logger.debug("Calling API async indexing service")
        request = _async_indexing_request_generate(request_params, request_files)
        response = requests.post(url, headers=apigw_params, json=request)
        response_json = response.json()

        if type(response_json) != dict or 'dataset_status_key' not in response_json or response_json['dataset_status_key'] == "error":
            raise Exception(f"Bad response from the API service '{url}'")

        result['status'] = "waiting"
        result['process_id'] = response_json['dataset_status_key']
    except Exception as ex:
        result['status'] = "error"
        logger.error("Error calling API async indexing service", exc_info=True)

    return result

def queue_indexing_request(apigw_params: dict, request_params: dict, request_files: list) -> dict:
    """ Send request for async indexing

    :param apigw_params: Params from apigateway
    :param request_params: Params to fill JSON template
    :param request_files: Files to process
    :return: Status of process
    """
    result = {}
    queue = os.getenv('API_QUEUE_PROCESS_URL')

    try:
        logger.debug("Calling API queue indexing service")

        request = _async_indexing_request_generate(request_params, request_files)
        request['headers'] = apigw_params

        provider_resources.qc.set_credentials((provider_resources.provider, queue), url=queue)

        logger.debug(f"Inserting request in queue '{queue}'")
        if not provider_resources.queue_write_message(request, queue):
            raise Exception(f"Unable to write in queue '{queue}'")

        result['status'] = "waiting"
        result['process_id'] = request['dataset_conf'].get('dataset_id', "")
    except Exception as ex:
        result['status'] = "error"
        logger.error("Error calling API queue indexing service", exc_info=True)

    return result

def async_status_request(apigw_params: dict, process_id: str) -> dict:
    """ Send request to known the status of async process

    :param apigw_params: Params from apigateway
    :param process_id: Id of process
    :return: Status of process
    """
    result = {}
    url = os.getenv('API_ASYNC_STATUS_URL')
    url = f"http://{url}" if "http" not in url else url

    try:
        logger.debug("Calling API async status service")
        response = requests.post(url, headers=apigw_params, json={'dataset_status_key': process_id})
        response_json = response.json()

        if type(response_json) != dict or 'process_status' not in response_json:
            raise Exception(f"Bad response from the API service '{url}'")

        if response_json['process_status'] == "processing":
            result['status'] = "waiting"
        elif response_json['process_status'] == "finished":
            result['status'] = "ready"
        else:
            raise Exception(f"Bad request status '{response_json['process_status']}'")

        result['process_id'] = process_id
    except:
        result['status'] = "error"
        logger.error("Error calling API async status service", exc_info=True)

    return result

def async_result_request(apigw_params: dict, process_id: str) -> dict:
    """ Send request to get results of async process

    :param apigw_params: Params from apigateway
    :param process_id: Id of process
    :return: Results of process
    """
    result = {}
    url = os.getenv('API_ASYNC_RESULT_URL')
    url = f"http://{url}" if "http" not in url else url

    try:
        logger.debug("Calling API async result service")
        response = requests.post(url, headers=apigw_params, json={'dataset_status_key': process_id})
        response_json = response.json()

        if type(response_json) != dict or 'info' not in response_json or response_json['info'] == "error":
            raise Exception(f"Bad response from the API service '{url}'")

        result['status'] = "finish"
        result['process_id'] = process_id
        result['results'] = response_json['info']

        if not result['results']:
            raise Exception("Results empty")
    except:
        result['status'] = "error"
        result['process_id'] = process_id
        logger.error("Error calling API async result service", exc_info=True)
        raise Exception("Unable to get results from API")

    return result

def async_delete_request(apigw_params: dict, process_id: str, tracking_message: dict) -> bool:
    """ Send request to delete results of sync or async process

    :param apigw_params: Params from apigateway
    :param process_id: Id of process
    :param tracking_message: Message of tracking
    :return: True or False if delete is successfully
    """
    url = os.getenv('API_ASYNC_DELETE_URL')
    url = f"http://{url}" if "http" not in url else url

    try:
        logger.debug("Calling API async delete service")
        request = {'dataset_status_key': process_id, 'tracking': tracking_message}
        response = requests.post(url, headers=apigw_params, data=json.dumps(request))
        response_json = response.json()

        if type(response_json) != dict or 'status' not in response_json or response_json['status'] != "ok":
            raise Exception(f"Bad response from the API service '{url}'")

        status = True
    except:
        status = False
        logger.error("Error calling API async delete service", exc_info=True)

    return status

def sync_delete_request(apigw_params: dict, process_id: str, tracking_message: dict) -> bool:
    """ Send request to delete results of sync or async process

    :param apigw_params: Params from apigateway
    :param process_id: Id of process
    :param tracking_message: Message of tracking
    :return: True or False if delete is successfully
    """
    url = os.getenv('API_SYNC_DELETE_URL')
    url = f"http://{url}" if "http" not in url else url

    try:
        logger.debug("Calling API sync delete service")
        request = {'dataset_status_key': process_id, 'tracking': tracking_message}
        response = requests.post(url, headers=apigw_params, data=json.dumps(request))
        response_json = response.json()

        if type(response_json) != dict or 'status' not in response_json or response_json['status'] != "ok":
            raise Exception(f"Bad response from the API service '{url}'")

        status = True
    except:
        status = False
        logger.error("Error calling API sync delete service", exc_info=True)

    return status

def queue_delete_request(apigw_params: dict, process_id: str, tracking_message: dict) -> bool:
    """ Send request to delete results of sync or async process

    :param apigw_params: Params from apigateway
    :param process_id: Id of process
    :param tracking_message: Message of tracking
    :return: True or False if delete is successfully
    """
    queue = os.getenv('API_QUEUE_DELETE_URL')

    try:
        logger.debug("Calling API queue delete service")

        request = {'dataset_status_key': process_id, 'headers': apigw_params, 'tracking': tracking_message}

        provider_resources.qc.set_credentials((provider_resources.provider, queue), url=queue)

        logger.debug(f"Inserting request in queue '{queue}'")
        if not provider_resources.queue_write_message(request, queue):
            raise Exception(f"Unable to write in queue '{queue}'")

        status = True
    except:
        status = False
        logger.error("Error calling API queue delete service", exc_info=True)

    return status
