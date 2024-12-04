### This code is property of the GGAO ###

# Native imports
import os
import json
import re

# Installed imports
from llama_index.embeddings.azure_openai import AzureOpenAIEmbedding
from llama_index.embeddings.bedrock import BedrockEmbedding
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.core.embeddings import BaseEmbedding

# Custom imports
from common.genai_controllers import load_file, provider
from common.ir.connectors import Connector, ManagerConnector

IR_INDICES = "src/ir/index/"
INDEX_STORAGE = lambda index: IR_INDICES + index + ".json"



def modify_index_documents(connector, modify_index_docs: dict, docs: list, index: str, logger) -> list:
    """
    modify_index_docs must be one of two types, Delete and Update:

    Example =>
            modify_index_docs = {
                "delete": {
                    "metadata_key_1": ["match", ...]
                },
                "update": {
                    "metadata_key_2": True,
                    "metadata_key_3": False,
                }
            }
    - To delete exact matches: {key_to_filter: [exact_matches_to_delete, ...]}
    - To update documents with same key_to_filter than the ones in the document store {"key_to_filter": True}
    - To avoid updating documents with same key_to_filter than the ones in the document store {key_to_filter: False}

    Congruences are not controlled due to the inefficiency of the process. Be careful when updating fields.

    :param connector: Access to Elastic Search
    :param modify_index_docs: Configuration to modify
    :param docs: Documents to indexes
    :return: List of documents indexes
    """
    delete_key = "delete"
    update_key = "update"

    for key in modify_index_docs:
        if key not in [delete_key, update_key]:
            raise ValueError(f"'modify_index_docs' only admits two keys: {delete_key} and {update_key}.")

    # Delete given documents
    deletion_dict = modify_index_docs.get(delete_key, {})
    for key_to_filter in deletion_dict:
        deletion_values = deletion_dict[key_to_filter]
        if not isinstance(deletion_values, list) or not all([isinstance(val, str) for val in deletion_values]):
            raise ValueError("All values in 'Delete' field must be of type list and must contain only strings.")
    if len(deletion_dict) > 0:
        result = connector.delete_documents(index_name=index, filters=deletion_dict)
        logger.info(f"Result delete: {result}.")

    # Update documents given the boolean conditions
    update_dict = modify_index_docs.get(update_key, {})
    if update_dict:
        try:
            ds_documents = connector.get_all_documents(index_name=index)
            if ds_documents[2] != 200:
                return docs
        except Exception as exc:
            logger.error(
                "All documents could not be retrieved and thus update won't work. Alternativelly you could use deletion")
            raise exc

        ds_documents = ds_documents[1]
        for key_to_filter in update_dict:
            ds_documents_values = [doc['meta'][key_to_filter] for doc in ds_documents if key_to_filter in doc['meta']]
            new_documents_values = [doc.metadata[key_to_filter] for doc in docs if key_to_filter in doc.metadata]
            keys_in_document_store = [f for f in new_documents_values if f in ds_documents_values]
            if len(new_documents_values) != len(docs):
                frac = f"{len(new_documents_values)}/{len(docs)}"
                raise ValueError(f"Only {frac} documents had the {key_to_filter} key in metadata. "
                                 f"All documents must have the key defined when filtering that key.")

            if len(ds_documents_values) != len(ds_documents):
                logger.warning(f"Not all document-store documents had the {key_to_filter} key in metadata. "
                               f"Consider reindexing with stable metadata keys.")

            update_bool = update_dict[key_to_filter]
            if not isinstance(update_bool, bool):
                raise ValueError(f"All values in 'Update' field must be of type bool. Error in {key_to_filter}.")

            if update_bool:
                logger.info(f"{len(keys_in_document_store)} docs will be updated due to "
                            f"having the same {key_to_filter} as index ones.")
                connector.delete_documents(index_name=index, filters={key_to_filter: keys_in_document_store})
            else:
                n_docs = len(docs)
                docs = [doc for doc in docs if doc['meta'][key_to_filter] not in keys_in_document_store]
                logger.info(f"{n_docs - len(docs)} docs will not be updated due to "
                            f"having the same {key_to_filter} as the index ones.")
    return docs


def get_connector(index: str, workspace, vector_storages) -> Connector:
    """ Get the connector from the vector storage

    param: index: index to get the connector from
    """
    # Get the connector name
    try:
        state_dict = load_file(workspace, INDEX_STORAGE(index))
        if len(state_dict) == 0:
            connector_name = ""
        else:
            connector_name = json.loads(state_dict).get('vector_storage')
    except Exception:
        connector_name = ""
    if not connector_name:
        connector_name = os.getenv('VECTOR_STORAGE', f"elastic-{os.getenv('TENANT')}")

    for vs in vector_storages:
        if vs.get("vector_storage_name") == connector_name:
            connector = ManagerConnector().get_connector(vs)
            if connector:
                connector.connect()
                return connector
    raise ValueError(f"Connector {connector_name} not found in vector storages")


def get_embed_model(model: dict, aws_credentials: dict, is_retrieval: bool) -> BaseEmbedding:
    """ Get the llamaindex embed model

    :param model: model to get the embedding model
    :param aws_credentials: aws credentials to get the embedding model

    :return: BaseEmbedding model to get the embeddings
    """
    platform = model.get('platform')
    if platform == 'azure':
        return AzureOpenAIEmbedding(
            model=model.get('embedding_model'),
            deployment_name=model.get('azure_deployment_name'),
            api_key=model.get('api_key'),
            azure_endpoint=model.get('azure_base_url'),
            api_version=model.get('azure_api_version')
        )
    elif platform == "bedrock":
        if provider == "azure":
            return BedrockEmbedding(
                aws_access_key_id=aws_credentials['access_key'],
                aws_secret_access_key=aws_credentials['secret_key'],
                region_name=model.get('region'),
                model_name=model.get('embedding_model')
            )
        else:
            return BedrockEmbedding(
                region_name=model.get('region'),
                model_name=model.get('embedding_model')
            )
    elif platform == "huggingface":
        if is_retrieval:
            # Normally in huggingface the retrieval model is different from the embedding model
            return HuggingFaceEmbedding(model_name=model.get('retriever_model'))
        return HuggingFaceEmbedding(model_name=model.get('embedding_model'))
    else:
        raise ValueError(f"Platform {platform} not supported")
