### This code is property of the GGAO ###


from flask import request
from typing import Tuple, Dict
from common.ir.utils import get_connector
from common.utils import INDEX_NAME
from common.genai_json_parser import get_exc_info
import elasticsearch.exceptions 
from common.utils import get_models


def get_documents_filenames_handler(deploy, request) -> Tuple[Dict, int]:
    '''Handle the request to retrieve filenames of documents from the specified index.'''
    dat = {}
    dat.update(request.args)
    index = dat.get('index', "").strip()

    if not index:
        return {'status': "error", 'error_message': "Missing parameter: index", 'status_code': 400}, 400

    connector = get_connector(index, deploy.workspace, deploy.vector_storages)

    for model in deploy.all_models:
        index_name = INDEX_NAME(index, model)
        if not connector.exist_index(index_name):
            continue
        try:
            status, result, status_code = connector.get_documents_filenames(index_name)
            connector.close()
            return {'status': status, 'result': {"status_code": status_code, "docs": result, "status": status}}, status_code
        except elasticsearch.NotFoundError:
            deploy.logger.debug(f"Index '{index_name}' not found")
        except Exception as ex:
            deploy.logger.error(f"Error processing operation: {str(ex)}", exc_info=get_exc_info())
            connector.close()
            return {'status': "error", 'error_message': f"Error processing operation: {str(ex)}", 'status_code': 400}, 400

    connector.close()
    return {'status': "error", 'error_message': f"Index '{index}' not found", 'status_code': 400}, 400


def retrieve_documents_handler(deploy, request) -> Tuple[Dict, int]:
    ''' Handle the request to retrieve documents from the specified index using filters. '''
    json_input = request.get_json(force=True)
    deploy.logger.info(f"Request received with data: {json_input}")

    index = json_input.get('index', "")
    filters = json_input.get('filters', {})

    if not filters:
        return {'status': "error", 'error_message': "There must be at least one filter", 'status_code': 400}, 400

    connector = get_connector(index, deploy.workspace, deploy.vector_storages)
    for model in deploy.all_models:
        index_name = INDEX_NAME(index, model)
        try:
            status, result, status_code = connector.get_documents(index_name, filters)
            connector.close()
            return {
                'status': status,
                'result': {"status_code": status_code, "docs": result, "status": status},
            }, status_code
        except elasticsearch.NotFoundError:
            deploy.logger.debug(f"Index '{index_name}' not found")
        except Exception as ex:
            deploy.logger.error(f"Error processing operation: {str(ex)}", exc_info=get_exc_info())
            connector.close()
            return {'status': "error", 'error_message': f"Error processing operation: {str(ex)}", 'status_code': 400}, 400

    connector.close()
    return {'status': "error", 'error_message': f"Index '{index}' not found", 'status_code': 400}, 400


def get_models_handler(deploy) -> Tuple[Dict, int]:
    '''Handles the request to retrieve models based on a specified parameter.'''
    deploy.logger.info("Get models request received")
    
    dat = request.args
    if len(dat) != 1 or next(iter(dat)) not in {'platform', 'pool', 'zone', 'embedding_model'}:
        return {
            "status": "error",
            "error_message": "You must provide only one parameter between 'platform', 'pool', 'zone' and 'embedding_model' param",
            "status_code": 400
        }, 400

    key, value = next(iter(dat.items()))
    models, pools = get_models(deploy.available_models, deploy.available_pools, key, value)
    return {
        "status": "ok",
        "result": {"models": models, "pools": list(set(pools)) if pools else []},
        "status_code": 200
    }, 200


def delete_documents_handler(deploy, request) -> Tuple[Dict, int]:
    '''Handles the request to delete documents from Elasticsearch indexes based on specified filters.'''
    dat = dict(request.args.lists())  # Parse to dict of lists
    deploy.logger.info(f"Request received with data: {dat}")

    index = dat.pop('index', [""])[0]
    filters = dat  # All additional parameters are considered as filters

    connector = get_connector(index, deploy.workspace, deploy.vector_storages)
    deleted_count = 0

    for model in deploy.all_models:
        index_name = INDEX_NAME(index, model)
        if not connector.exist_index(index_name):
            continue
        try:
            result, failures, deleted = connector.delete_documents(index_name, filters)

            if failures:
                deploy.logger.debug(f"Error deleting documents in index '{index_name}': {result}")
            elif deleted == 0:
                deploy.logger.debug(f"Documents not found for filters: '{filters}' in index '{index_name}'")
            else:
                deploy.logger.debug(f"{deleted} chunks deleted for '{index_name}'")
            
            deleted_count += deleted

        except elasticsearch.NotFoundError:
            deploy.logger.debug(f"Index '{index_name}' not found")
        except Exception as ex:
            deploy.logger.error(f"Error processing delete operation: {str(ex)}", exc_info=get_exc_info())
            connector.close()
            return {'status': "error", 'error_message': f"Error processing delete operation: {str(ex)}", 'status_code': 400}, 400

    connector.close()

    if deleted_count > 0:
        return {'status': "finished", 'result': f"Documents that matched the filters were deleted for '{index}'", 'status_code': 200}, 200
    return {'status': "error", 'error_message': f"Documents not found for filters: {filters}", 'status_code': 400}, 400


def delete_index_handler(deploy, request) -> Tuple[Dict, int]:
    '''Handles the request to delete specified Elasticsearch indexes for all available models.'''
    dat = {}
    dat.update(request.args)
    deploy.logger.info(f"Request received with data: {dat}")

    index = dat.get('index', "")
    connector = get_connector(index, deploy.workspace, deploy.vector_storages)
    deleted_count = 0

    for model in deploy.all_models:
        index_name = INDEX_NAME(index, model)
        if not connector.exist_index(index_name):
            continue
        try:
            connector.delete_index(index_name)
            deleted_count += 1
        except elasticsearch.NotFoundError:
            deploy.logger.debug(f"Index '{index_name}' not found")
        except Exception as ex:
            deploy.logger.error(f"Error processing delete index operation: {str(ex)}", exc_info=get_exc_info())
            connector.close()
            return {'status': "error", 'error_message': f"Error processing delete index operation: {str(ex)}", 'status_code': 400}, 400

    connector.close()

    if deleted_count == 0:
        deploy.logger.info(f"Index '{index}' not found for any model")
        return {'status': "error", 'error_message': f"Index '{index}' not found", 'status_code': 400}, 400
    return {'status': "finished", 'result': f"Index '{index}' deleted for '{deleted_count}' models", 'status_code': 200}, 200


def list_indices_handler(deploy) -> Tuple[Dict, int]:
    '''Handles the request to list all indices in the Elasticsearch database.'''
    deploy.logger.info("List indices request received")
    
    connector = get_connector('', deploy.workspace, deploy.vector_storages)
    try:
        indices = connector.list_indices()  
        
        processed_indices = []
        for index in indices:
            parts = index.rsplit("_", 1)
            index_name = parts[0]
            model_name = parts[1] if len(parts) > 1 else "unknown"

            existing_index = next((i for i in processed_indices if i["name"] == index_name), None)
            if existing_index:
                existing_index["models"].append(model_name)
            else:
                processed_indices.append({
                    "name": index_name,
                    "models": [model_name]
                })

        return {
            "status": "ok",
            "indices": processed_indices,
            "status_code": 200
        }, 200

    finally:
        connector.close()
