### This code is property of the GGAO ###


# Native imports
import json
import re

# Installed imports
import tiktoken

from rescoring import rescore_documents
from flask import Flask, request
import elasticsearch.exceptions
from elasticsearch_adaption import \
    ElasticsearchStoreAdaption  # Custom class that adapts the elasticsearch store to improve filters
# In the version 0.2.0 it's impossible to use multiple filters in the same query
from elasticsearch.helpers.vectorstore import AsyncDenseVectorStrategy, AsyncBM25Strategy
from elasticsearch import AsyncElasticsearch
from llama_index.core import VectorStoreIndex, MockEmbedding
from llama_index.core.llms.mock import MockLLM
from llama_index.core.vector_stores import MetadataFilter, MetadataFilters, FilterCondition
from llama_index.core.base.embeddings.base import BaseEmbedding
from llama_index.core.retrievers import QueryFusionRetriever
from llama_index.core.schema import NodeWithScore, QueryBundle

# Custom imports
from common.deployment_utils import BaseDeployment
from common.genai_controllers import storage_containers, set_storage
from common.genai_json_parser import *
from common.services import GENAI_INFO_RETRIEVAL_SERVICE
from common.ir import get_connector, get_embed_model
from common.utils import load_secrets, ELASTICSEARCH_INDEX
from common.storage_manager import ManagerStorage
from common.indexing.parsers import ManagerParser, ParserInforetrieval
from common.indexing.connectors import Connector
from common.errors.genaierrors import PrintableGenaiError
from common.utils import get_models


class InfoRetrievalDeployment(BaseDeployment):
    MODEL_FORMATS = {
        "bm25--score": "sparse"
    }

    TOKENIZER = {
        "bm25--score": None
    }

    def __init__(self):
        """ Creates the deployment"""
        super().__init__()
        set_storage(storage_containers)

        try:
            self.origin = storage_containers.get('origin')
            self.workspace = storage_containers.get('workspace')
            file_loader = ManagerStorage().get_file_storage(
                {"type": "IRStorage", "workspace": self.workspace, "origin": self.origin})

            self.available_pools = file_loader.get_available_pools()

            self.available_models = file_loader.get_available_embedding_models(inforetrieval_mode=True)

            self.all_models = file_loader.get_unique_embedding_models()
            self.default_embedding_equivalences = file_loader.get_embedding_equivalences()
            self.models_credentials, self.vector_storages, self.aws_credentials = load_secrets()
            self.logger.info(f"---- Inforetrieval initialized")
        except Exception as ex:
            self.logger.error(f"Error loading files: {str(ex)}", exc_info=get_exc_info())

    @property
    def must_continue(self) -> bool:
        """ True if the output should be sent to next step """
        return False

    @property
    def service_name(self) -> str:
        return GENAI_INFO_RETRIEVAL_SERVICE

    @property
    def max_num_queue(self) -> int:
        return 1

    @staticmethod
    def assert_correct_models(index: str, models: List, connector: Connector):
        """ Asserts that the models are correct

        param: models: models to check
        """
        for model in models:
            embedding_model = model.get('embedding_model')
            if not connector.exist_index(ELASTICSEARCH_INDEX(index, embedding_model)) and embedding_model != "bm25":
                raise PrintableGenaiError(400, f"Model '{model.get('alias')}' does not exist for the index '{index}'")

    def get_bm25_vector_store(self, index: str, connector: Connector, es_client: AsyncElasticsearch):
        """ Get the retriever from the model

        param: model: model to get the retriever from
        param: index: index to get the retriever from

        """
        for model in self.all_models:
            index_name = ELASTICSEARCH_INDEX(index, model)
            if connector.exist_index(index_name):
                # Add bm25 retriever (with one index that matches is enough, all indexes in elastic can do this retrieval)
                vector_store = ElasticsearchStoreAdaption(index_name=index_name, es_client=es_client,
                                                          retrieval_strategy=AsyncBM25Strategy())
                return vector_store

        raise PrintableGenaiError(400, "There is no index that matches the passed value")

    def get_retrievers_arguments(self, models: list, index: str, es_client: AsyncElasticsearch,
                                 connector: Connector, query:str) -> list:
        """ Gets the retrievers that exists

        :param models: String that identifies the model
        :param index: String that identifies the index

        return list of retrievers with their identifier
        """
        retrievers = []
        for model in models:
            if model.get('embedding_model') == "bm25":
                vector_store = self.get_bm25_vector_store(index, connector, es_client)
                # MockEmbedding used to avoid errors in the bm25 retriever (OPENAI_API_KEY mandatory)
                embed_model = MockEmbedding(embed_dim=256)
                embed_query = query #BM25 does not use embeddings
            else:
                index_name = ELASTICSEARCH_INDEX(index, model.get('embedding_model'))
                vector_store = ElasticsearchStoreAdaption(index_name=index_name, es_client=es_client,
                                                          retrieval_strategy=AsyncDenseVectorStrategy())
                embed_model = get_embed_model(model, self.aws_credentials, is_retrieval=True)
                embed_query = embed_model.get_query_embedding(query)
            
            retrievers.append((vector_store, embed_model, embed_query, f"{model.get('embedding_model')}--score"))
        return retrievers

    def get_default_models(self, index: str, connector: Connector):
        """ Get the default models for the index

        :param index: index to get the default models

        :return: List of default models
        """
        indexed_models = ["bm25"]
        for model in self.all_models:
            if connector.exist_index(ELASTICSEARCH_INDEX(index, model)):
                indexed_models.append(self.default_embedding_equivalences[model])
    
        # Get the model credentials
        return ParserInforetrieval.get_sent_models(indexed_models, self.available_pools, self.available_models,
                                                   self.models_credentials)

    @staticmethod
    def generate_llama_filters(filters):
        """ Generate llama filters

        :return: Llama filters
        """
        llama_filters = []
        for key, value in filters.items():
            if isinstance(value, list):
                multiple_filter = [MetadataFilter(key=key, value=v) for v in value]
            else:
                multiple_filter = [MetadataFilter(key=key, value=value)]
            llama_filters.append(MetadataFilters(filters=multiple_filter, condition=FilterCondition.OR))

        return MetadataFilters(filters=llama_filters, condition=FilterCondition.AND)


    def retrieve(self, vector_store: ElasticsearchStoreAdaption, embed_model: BaseEmbedding, retriever_type: str,
                 filters: dict, top_k: int, docs: dict, embed_query: list, query: str) -> list:
        """Retrieve documents from a retriever

        :param vector_store: Vector store to use in retrieval
        :param embed_model: Embed model to use in retrieval
        :param query: Query to retrieve
        :param filters: Filters to apply
        :param top_k: Number of documents to retrieve

        :return: List of documents
        """

        vector_store_index = VectorStoreIndex.from_vector_store(vector_store=vector_store, embed_model=embed_model)
        retriever = vector_store_index.as_retriever(similarity_top_k=top_k, filters=self.generate_llama_filters(filters))

        if isinstance(embed_query, list):
            retrieved_docs = retriever.retrieve(QueryBundle(query_str=query, embedding=embed_query))
        else:
            #bm25 does not use embeddings
            retrieved_docs = retriever.retrieve(QueryBundle(query_str=query))
        self.logger.debug(f"{retriever_type} retrieved {len(retrieved_docs)} documents")

        for doc in retrieved_docs:
            self.add_retrieved_document(docs, doc, retriever_type)

        return retrieved_docs
    @staticmethod
    def add_retrieved_document(docs: dict, retrieved_doc: NodeWithScore, retriever_type: str):
        """ Add a retrieved document to the list of documents

        :param docs: Dictionary of documents
        :param retrieved_doc: Retrieved document
        :param retriever_type: Type of retriever
        """
        if retrieved_doc.metadata['snippet_id'] not in docs:
            retrieved_doc.metadata[retriever_type] = retrieved_doc.score
            docs[retrieved_doc.metadata['snippet_id']] = retrieved_doc
        else:
            if retriever_type not in docs[retrieved_doc.metadata['snippet_id']].metadata:
                docs[retrieved_doc.metadata['snippet_id']].metadata[retriever_type] = retrieved_doc.score
                docs[retrieved_doc.metadata['snippet_id']].score += retrieved_doc.score

    def genai_retrieval_strategy(self, retrievers_arguments, input_object):
        docs_by_retrieval = {}
        unique_docs = {}
        for vector_store, embed_model, embed_query, retriever_type in retrievers_arguments:
            docs_tmp = self.retrieve(vector_store, embed_model, retriever_type, input_object.filters,
                                     input_object.top_k, unique_docs, embed_query, input_object.query)

            docs_by_retrieval[retriever_type] = docs_tmp

        # Complete the scores that have not been obtained till this point
        ids_incompleted_docs = self.get_ids_empty_scores(docs_by_retrieval, set(unique_docs.keys()))
        if len(ids_incompleted_docs) > 0:
            self.logger.debug(f"Re-scoring with {', '.join(list(zip(*retrievers_arguments))[3])} retrievers")
            for vector_store, embed_model, embed_query, retriever_type in retrievers_arguments:
                input_object.filters['snippet_id'] = ids_incompleted_docs[retriever_type]
                top_k_new = len(ids_incompleted_docs[retriever_type])
                if top_k_new > 0:
                    self.retrieve(vector_store, embed_model, retriever_type, input_object.filters, top_k_new,
                                  unique_docs, embed_query, input_object.query)

        retrievers = [retriever_type for _, _, _, retriever_type in retrievers_arguments]
        for doc in unique_docs.values():
            for retriever_type in retrievers:
                if retriever_type not in doc.metadata:
                    # Sometimes bm25 does not retrieve all documents
                    doc.metadata[retriever_type] = 0
            doc.score /= len(retrievers_arguments)

        rescored_docs = rescore_documents(unique_docs, input_object.query, input_object.rescoring_function,
                                          self.MODEL_FORMATS, retrievers)

        sorted_docs = sorted(rescored_docs, key=lambda x: x.score, reverse=True)

        return sorted_docs
    @staticmethod
    def get_ids_empty_scores(ids_dict: dict, all_ids: set):
        """Get the document ids that some models have not retrieved and thus don't have scores

        :param ids_dict (dict): Dictionary of ids that each of the models have retriever

        :return dict: ids that do not have scores
        """
        ids_empty_scores = {}
        for retriever_type, ids in ids_dict.items():
            # Difference between all ids and the ids that have been retrieved
            ids_empty_scores[retriever_type] = list(all_ids - set(doc.metadata['snippet_id'] for doc in ids))

        return ids_empty_scores

    @staticmethod
    def parse_output(sorted_documents, query: str = "") -> List:
        """ Parse List of documents to a JSON-Serializable list to return

        :param sorted_documents: List of sorted documents
        :param query: Used only if html_highlights is set to True.

        :return: List of parsed documents
        """
        final_docs = []
        for doc in sorted_documents:
            final_docs.append({
                "id_": doc.id_,
                "meta": doc.metadata,
                "relationships": {
                    "previous_node": doc.node.prev_node.to_dict() if doc.node.prev_node else {},
                    "next_node": doc.node.next_node.to_dict() if doc.node.next_node else {}
                },
                "content": doc.text,
                "score": doc.score
            })
        return final_docs

    def rrf_retrieval_strategy(self, retrievers_arguments, input_object):
        retrievers = []
        for vector_store, embed_model, _, retriever_type in retrievers_arguments:
            vector_store_index = VectorStoreIndex.from_vector_store(vector_store=vector_store, embed_model=embed_model)
            retrievers.append(vector_store_index.as_retriever(filters=self.generate_llama_filters(input_object.filters),
                                                              similarity_top_k=input_object.top_k + 1))
        retriever = QueryFusionRetriever(
            retrievers,
            llm=MockLLM(),
            mode=input_object.strategy_mode,
            similarity_top_k=input_object.top_k + 1,
            num_queries=1,  # set this to 1 to disable query generation
            use_async=True,
            verbose=True
        )
        return retriever.retrieve(input_object.query)

        
    def process(self, json_input: dict):
        try:
            self.logger.debug(f"Data entry: {json_input}")
            input_object = ManagerParser().get_parsed_object({"type": "inforetrieval", "json_input": json_input,
                                                              "available_models": self.available_models,
                                                              "available_pools": self.available_pools,
                                                              "models_credentials": self.models_credentials})
            connector = get_connector(input_object.index, self.workspace, self.vector_storages)
            es_client = AsyncElasticsearch(hosts=f"{connector.scheme}://{connector.host}:{connector.port}",
                                           http_auth=(connector.username, connector.password),
                                           verify_certs=False, timeout=30)

            # If no models are passed, we will retrieve with bm25 and the models used in the indexation process
            if len(input_object.models) == 0:
                input_object.models = self.get_default_models(input_object.index, connector)
            else:
                self.assert_correct_models(input_object.index, input_object.models, connector)

            retrievers_arguments = self.get_retrievers_arguments(input_object.models, input_object.index, es_client,
                                                                 connector, input_object.query)

            if input_object.strategy == "llamaindex_fusion":
                sorted_documents = self.rrf_retrieval_strategy(retrievers_arguments, input_object)
            elif input_object.strategy == "genai_retrieval":
                sorted_documents = self.genai_retrieval_strategy(retrievers_arguments, input_object)

            tokens_report = {}
            for vector_store, embed_model, embed_query, retriever_type in retrievers_arguments:
                # close connections and count query tokens
                tokenizer = self.TOKENIZER.get(retriever_type, "cl100k_base")
                if tokenizer:
                    tokens_report[retriever_type] = len(tiktoken.get_encoding(tokenizer).encode(input_object.query))
                else:
                    tokens_report[retriever_type] = 0
                vector_store.close()
            connector.close()

            if not eval(os.getenv('TESTING', "False")):
                for model, tokens in tokens_report.items():
                    resource = f"retrieval/{model}/tokens"
                    self.report_api(tokens, "", input_object.x_reporting, resource, "", "TOKENS")

                resource = f"retrieval/process/call"
                self.report_api(1, "", input_object.x_reporting, resource, "")

            return self.must_continue, {
                "status_code": 200,
                "docs": self.parse_output(sorted_documents[:input_object.top_k], input_object.query),
                "status": "finished"
            }, ""

        except Exception as ex:
            raise ex


app = Flask(__name__)
deploy = InfoRetrievalDeployment()


@app.route('/process', methods=['POST'])
def sync_deployment() -> Tuple[Dict, int]:
    """ Deploy service in a sync way. """
    dat = request.get_json(force=True)
    apigw_params = {
        'x-tenant': request.headers['x-tenant'],
        'x-department': request.headers['x-department'],
        'x-reporting': request.headers['x-reporting']
    }
    dat.update({"project_conf": apigw_params})
    return deploy.sync_deployment(dat)


@app.route('/delete-documents', methods=['POST'])
def delete_documents() -> Tuple[str, int]:
    """Delete documents that meet certain conditions"""
    json_input = request.get_json(force=True)
    deploy.logger.info(f"Request recieved with data: {json_input}")

    index = json_input.get('index', "")
    connector = get_connector(index, deploy.workspace, deploy.vector_storages)
    response, status_code = manage_actions_delete_elasticsearch(index, "delete-documents", json_input.get('delete', {}), connector)
    connector.close()
    return response, status_code

@app.route('/delete_index', methods=['POST'])
def delete_index() -> Tuple[str, int]:
    """Delete documents that meet certain conditions"""
    json_input = request.get_json(force=True)
    deploy.logger.info(f"Request recieved with data: {json_input}")

    index = json_input.get('index', "")
    connector = get_connector(index, deploy.workspace, deploy.vector_storages)
    response, status_code = manage_actions_delete_elasticsearch(index, "delete_index", {}, connector)
    connector.close()
    return response, status_code

@app.route('/healthcheck', methods=['GET'])
def healthcheck() -> Dict:
    return {"status": "Service available"}


@app.route('/retrieve_documents', methods=['POST'])
def retrieve_documents() -> Tuple[Dict, int]:
    json_input = request.get_json(force=True)
    deploy.logger.info(f"Request recieved with data: {json_input}")

    index = json_input.get('index', "")
    filters = json_input.get('filters', {})
    if len(filters) < 1:
        response = {'status': "error", 'result': "There must at least one filter", 'status_code': 400}
        return response, 400

    connector = get_connector(index, deploy.workspace, deploy.vector_storages)

    response, status_code = manage_actions_get_elasticsearch(index, "retrieve_documents", filters, connector)
    connector.close()
    return response, status_code

@app.route('/get_documents_filenames', methods=['POST'])
def get_documents_filenames() -> Tuple[Dict, int]:
    json_input = request.get_json(force=True)

    if not json_input or 'index' not in json_input or not json_input['index'].strip():
        return {'status': "error", 'result': "Missing parameter: index", 'status_code': 400}, 400

    index = json_input.get('index', "")

    connector = get_connector(index, deploy.workspace, deploy.vector_storages)

    response, status_code = manage_actions_get_elasticsearch(index, "get_documents_filenames", {}, connector)
    connector.close()
    return response, status_code


@app.route('/get_models', methods=['GET'])
def get_available_models() -> Tuple[Dict, int]:
    deploy.logger.info("Get models request received")
    dat = request.args
    if len(dat) != 1 or list(dat.items())[0][0] not in ['platform', 'pool', 'zone', 'embedding_model']:
        return {"status": "error", "error_message":
            "You must provide only one parameter between 'platform', 'pool', 'zone' and 'embedding_model' param", "status_code": 400}, 400
    key, value = list(dat.items())[0]
    models, pools = get_models(deploy.available_models, deploy.available_pools, key, value)
    return {"status": "ok", "result":
        {"models": models, "pools": list(set(pools)) if pools else []}, "status_code": 200}, 200



def manage_actions_get_elasticsearch(index: str, operation: str, filters: dict, connector: Connector):
    """
    Wrapper to perform an action in all the indexes that match the given index

    :param index: name of the index to perform the action
    :param operation: operation to perform
    :param filters: filters to apply
    :param connector: connector to use

    :return: response and status code
    """
    for model in deploy.all_models:
        index_name = ELASTICSEARCH_INDEX(index, model)
        try:
            if operation == "get_documents_filenames":
                # first one is valid as all indexes must have the same documents
                status, result, status_code = connector.get_documents_filenames(index_name)
                response = {'status': status, 'result': {"status_code": status_code, "docs": result, "status": status}}
                return response, status_code
            elif operation == "retrieve_documents":
                # first one is valid as all indexes must have the same documents
                status, result, status_code = connector.get_documents(index_name, filters)
                response = {'status': status, 'result': {"status_code": status_code, "docs": result, "status": status}}
                return response, status_code
            else:
                return {'status': "error", 'result': "Unsupported operation", 'status_code': 400}, 400
        except elasticsearch.NotFoundError:
            deploy.logger.debug(f"Index '{index_name}' not found")
        except Exception as ex:
            deploy.logger.error(f"Error processing operation '{operation}': {str(ex)}", exc_info=get_exc_info())
            return {'status': "error", 'result': f"Error processing operation '{operation}': {str(ex)}",
                    'status_code': 400}, 400

    return {'status': "error", 'result': f"Index '{index}' not found", 'status_code': 400}, 400

def manage_actions_delete_elasticsearch(index: str, operation: str, filters: dict, connector: Connector):
    """
    Wrapper to perform an action in all the indexes that match the given index

    :param index: name of the index to perform the action
    :param operation: operation to perform
    :param filters: filters to apply
    :param connector: connector to use

    :return: response and status code
    """
    results = []
    for model in deploy.all_models:
        index_name = ELASTICSEARCH_INDEX(index, model)
        try:
            # must delete the from all indexes (embeddings models)
            if operation == "delete-documents":
                result = connector.delete_documents(index_name, filters)
                if len(result.body.get('failures', [])) > 0:
                    deploy.logger.debug(f"Error deleting documents in index '{index_name}': {result} ")
                elif result.body.get('deleted', 0) == 0:
                    deploy.logger.debug(
                        f"Documents not found for filters: '{filters}' in index '{index_name}'")
                else:
                    deploy.logger.debug(f"{result.body['deleted']} chunks deleted for '{index_name}'")
            elif operation == "delete_index":
                result = connector.delete_index(index_name)
            else:
                return {'status': "error", 'result': "Unsupported operation", 'status_code': 400}, 400
            results.append(result)
        except elasticsearch.NotFoundError:
            deploy.logger.debug(f"Index '{index_name}' not found")
        except Exception as ex:
            deploy.logger.error(f"Error processing operation '{operation}': {str(ex)}", exc_info=get_exc_info())
            return {'status': "error", 'result': f"Error processing operation '{operation}': {str(ex)}",
                    'status_code': 400}, 400
    if operation == "delete-documents":
        deleted = sum(result.get('deleted', 0) for result in results)
        if deleted == 0:
            return json.dumps({'status': "error",
                               'result': f"Documents not found for filters: {filters}",
                               'status_code': 400}), 400
        elif deleted > 0:
            return json.dumps(
                {'status': "finished", 'result': f"Documents that matched the filters were deleted for '{index}'",
                 'status_code': 200}), 200
        else:
            return json.dumps(
                {'status': "error", 'result': f"Error deleting documents: {results}", 'status_code': 400}), 400
    elif operation == "delete_index":
        if len(results) == 0:
            return json.dumps({'status': "error", 'result': f"Index '{index}' not found", 'status_code': 400}), 400
        else:
            return json.dumps(
                {'status': "finished", 'result': f"Index '{index}' deleted for '{len(results)}' models", 'status_code': 200}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=False, port=8888, use_reloader=False)
