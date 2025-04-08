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
from llama_index.embeddings.google_genai import GoogleGenAIEmbedding

# Custom imports
from common.genai_controllers import load_file, provider
from common.ir.connectors import Connector, ManagerConnector

IR_INDICES = "src/ir/index/"
INDEX_STORAGE = lambda index: IR_INDICES + index + ".json"



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
            return HuggingFaceEmbedding(model_name=model.get('retriever_model'), trust_remote_code=True )
        return HuggingFaceEmbedding(model_name=model.get('embedding_model'), trust_remote_code=True)
    elif platform == "vertex":
        return GoogleGenAIEmbedding(
            model_name=model.get('embedding_model'),
            api_key=model.get('api_key'),
        )
    else:
        raise ValueError(f"Platform {platform} not supported")
