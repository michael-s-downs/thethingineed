### This code is property of the GGAO ###


"""
Functions to access and update the Genai status codes and counters
"""
# Native imports
import json
from typing import Union, Dict, Any, Tuple

# Custom imports
from common.genai_controllers import dbc

SEP = ":"


# DB Status functions
def create_status(origin: Tuple[Any, Any], key: str, status: str, msg: str):
    """ Create a new status entry

    :param origin: <tuple(str, str)> uhis_sdk_service.DBController origin
    :param key: Key of the entry to be created
    :param status: Status of the entry
    :param msg: Value of the entry
    """
    if status is not None and msg is not None:
        message = json.dumps({
            'status': status,
            'msg': msg
        })
    elif status is not None:

        message = status
    else:
        message = None

    dbc.insert(origin, key, None, message)


def update_full_status(origin: Union[Any, Any], key: str, status: Union[str, None, int], msg: Union[str, None], **kwargs):
    """ Update a status entry

    :param origin: <tuple(str, str)> uhis_sdk_service.DBController origin
    :param key: Key of the entry to be updated
    :param status: Status of the entry
    :param msg: New value of the entry
    :param kwargs: Other args. Decr/Incr options
    """
    if status is not None and msg is not None:
        message = json.dumps({
            'status': status,
            'msg': msg
        })
    elif status is not None:
        message = status
    else:
        message = None

    dbc.update(origin, key, None, message, **kwargs)


def get_status_code(origin: Union[Any, Any], key: str, format_json=False) -> Union[str, Dict[str, Union[str, list]]]:
    """ Get a status entry

    :param origin: <tuple(str, str)> uhis_sdk_service.DBController origin
    :param key: Key to get status from
    :param format_json: If true will return a dict with the value, if not will return all the value of key
    """
    if format_json:
        return json.loads(dbc.select(origin, key, None)[0]['values'].decode())['status']

    return dbc.select(origin, key, None)


def get_redis_pattern(origin: Union[str, str], pattern: str) -> list:
    """ Get list of keys with match a pattern

    :param origin: <tuple(str, str)> uhis_sdk_service.DBController origin
    :param pattern: Pattern to match
    """
    return dbc.select(origin, None, None, match=pattern)


def get_value(origin: Union[str, str], key: str, format_json=False) -> dict:
    """ Get a value from a key

    :param origin: <tuple(str, str)> uhis_sdk_service.DBController origin
    :param key: Key to get value from
    :param format_json: If true will return a dict with the value, if not will return all the value of key
    """
    if format_json:
        try:
            response = json.loads(dbc.select(origin, key, None)[0]['values'].decode())
        except:
            response = {}
    else:
        response = dbc.select(origin, key, None)

    return response


def get_images(origin: Union[str, str], key: str, format_json=False) -> dict:
    """ Get images from a key

    :param origin: <tuple(str, str)> uhis_sdk_service.DBController origin
    :param key: Key to get value from
    :param format_json: If true will return a dict with the value, if not will return all the value of key
    """
    if format_json:
        return json.loads(dbc.select(origin, key, None)[0]['values'].decode())['images']

    return dbc.select(origin, key, None)


def delete_status(origin: Union[str, str], key: str):
    """ Delete a status entry

    :param origin: <tuple(str, str)> uhis_sdk_service.DBController origin
    :param key: Key to delete
    """
    dbc.delete(origin, [key])


def decr_status_count(origin: Union[str, str], key: str, count: int = 1):
    """ Persist images into Redis

    :param origin: <tuple(str, str)> uhis_sdk_service.DBController origin
    :param key: Counter key
    :param count: Number to decrease. Default: 1
    """
    return dbc.update(origin, key, None, None, decr=count)


def incr_status_count(origin: Union[str, str], key: str, count: int = 1):
    """ Persist images into Redis

    :param origin: <tuple(str, str)> uhis_sdk_service.DBController origin
    :param key: Counter key
    :param count: Number to increase. Default: 1
    """
    return dbc.update(origin, key, None, None, incr=count)


def persist_images(origin: Union[str, str], key: str, msg: str):
    """ Persist images into Redis

    :param origin: <tuple(str, str)> uhis_sdk_service.DBController origin
    :param key: Key of the document the images belong to
    :param msg: Images
    """
    dbc.insert(origin, key, None, msg)


def update_status(origin: Union[str, str], key: str, msg: str):
    """ Update a status entry

    :param origin: <tuple(str, str)> uhis_sdk_service.DBController origin
    :param key: Key of the entry to be updated
    :param msg: New value of the entry
    """
    dbc.update(origin, key, None, msg)


def compose_status_key(process_id, key, counter=False):
    """ Get the status key

    :param process_id: Process id
    :param key: Key to create status key of
    :param counter: True if the status key to be created is a counter
    :return: (String) Status key of the entry
    """
    status_key = SEP.join([process_id, key])

    if counter:
        status_key = SEP.join([status_key, "counter"])

    return status_key


def compose_batch_key(proc_id: str, doc_id: str, batch_id: str) -> str:
    """ Create key for batch

    :param proc_id: Id of process
    :param doc_id: Id of document
    :param batch_id: Id of batch
    :return: str - Key of batch compose by process, document and batch id´s
    """
    return f"{proc_id}:{doc_id}:{batch_id}"


def compose_batch_keys(proc_id: str, doc_id: str, n: int) -> str:
    """ Create keys for batch

    :param proc_id: Id of process
    :param doc_id: Id of document
    :param n: Number of batch
    :return: Key of batch compose by process, document and batch id´s
    """
    for i in range(0, n):
        yield f"{proc_id}:{doc_id}:{str(i)}"


def compose_document_count_key(proc_id: str, doc_id: str) -> str:
    """ Create key for batch by redis

    :param proc_id: Id of process
    :param doc_id: Id of document
    :return: str - Key of batch compose by process and document
    """
    return f"{proc_id}:{doc_id}:ocr_count"


def compose_document_key(proc_id: str, doc_id: str) -> str:
    """ Create key of document

    :param proc_id: Id of process
    :param doc_id: Id of document
    :return: str - Key of document formed by process id and document id
    """
    return f"{proc_id}:{doc_id}"


def compose_cluster_key(dataset_status_key: str, cluster_suffix: str) -> str:
    """ Create key to cluster

    :param dataset_status_key: Id dataset
    :param cluster_suffix: Cluster suffix
    :return: key to cluster
    """
    return f"{dataset_status_key}:{cluster_suffix}"


def compose_model_key(dataset_status_key: str, model_id: str) -> str:
    """ Create key to cluster

    :param dataset_status_key: Id dataset
    :param model_id: Id of model
    :return: key to cluster
    """
    return f"{dataset_status_key}:{model_id}"


def compose_counter_features_key(dataset_status_key: str) -> str:
    """ Create key to cluster

    :param dataset_status_key: Id dataset
    :return: key to features
    """
    return f"{dataset_status_key}:counter_features"


def compose_status_model_key(dataset_status_key: str, symbol: str, model_id: str) -> str:
    """ Create key to train multilabel

    :param dataset_status_key: Id dataset
    :param symbol: Symbol of category
    :param model_id: Id of model
    :return: Status of model to train multilabel
    """
    return f"{dataset_status_key}:{symbol}:{model_id}"


def get_status_paths(origin: Union[str, str], proc_id: str, doc_id: str, ocr_batches: int) -> list:
    """ Create key for batch

    :param origin: <tuple(str, str)> uhis_sdk_service.DBController origin
    :param proc_id: Id of process
    :param doc_id: Id of document
    :param ocr_batches: Number of batches
    :return: list - Paths of all documents
    """
    batch_keys = compose_batch_keys(proc_id, doc_id, ocr_batches)

    all_paths = []
    for batch_key in batch_keys:
        paths = get_status_code(origin, batch_key)
        all_paths.extend(paths)

    all_paths.sort(key=lambda x: json.loads(x['values'].decode())['number'])

    return all_paths


def parse_status_paths(values: list) -> list:
    """ Parse status of json paths

    :param values: str with status
    :return list with paths
    """
    returned_values = []

    for value in values:
        value_values = json.loads(value['values'].decode())
        if value_values['valid']:
            for path in value_values['paths']:
                returned_values.append(path['text'])

    return returned_values


def create_status_count(origin: Union[str, str], key: str, count: int = 0):
    """ Create status in Redis

    :param origin: <tuple(str, str)> uhis_sdk_service.DBController origin
    :param key: Key ocr compose by process and document id
    :param count: Document code created
    """
    dbc.insert(origin, key, None, count)


def parse_status_json(value: dict) -> dict:
    """ Parse status of json to convert in json

    :param value: str with status
    :return str of type json with values
    """
    parsed_values = value[0]['values']
    return json.loads(parsed_values.decode())


def get_num_counter(origin: Union[str, str], key: str) -> int:
    """ Get the count in counter.

    :param origin: <tuple(str, str)> uhis_sdk_service.DBController origin
    :param key: Key of the counter to get count
    :return: (int) Count
    """
    result = dbc.select(origin, key, None)
    if len(result) > 0:
        return int(result[0]['values'])
    else:
        return 0
