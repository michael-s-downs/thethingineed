### This code is property of the GGAO ###


# Native imports
import json
import re

# Installed imports
import tiktoken

from utils import add_highlights
from rescoring import rescore_documents
from flask import Flask, request
import elasticsearch.exceptions
from elasticsearch_adaption import \
    ElasticsearchStoreAdaption  # Custom class that adapts the elasticsearch store to improve filters
# In the version 0.2.0 it's impossible to use multiple filters in the same query
from elasticsearch.helpers.vectorstore import AsyncDenseVectorStrategy, AsyncBM25Strategy
from elasticsearch import AsyncElasticsearch
from llama_index.core import VectorStoreIndex, MockEmbedding
from llama_index.core.vector_stores import MetadataFilter, MetadataFilters, FilterCondition
from llama_index.core.base.embeddings.base import BaseEmbedding

# Custom imports
from common.deployment_utils import BaseDeployment
from common.genai_controllers import storage_containers, set_storage
from common.genai_json_parser import *
from common.services import GENAI_INFO_RETRIEVAL_SERVICE
from common.ir import get_connector, get_embed_model
from common.utils import load_secrets
from common.indexing.loaders import ManagerLoader
from common.indexing.parsers import ManagerParser, ParserInforetrieval
from common.indexing.connectors import Connector


class InfoRetrievalDeployment(BaseDeployment):
    MODEL_FORMATS = {
        'bm25': "sparse",
        'sentence_transformers': "dense",
        'azure_openai': "dense",  # NOTE: MAYBE WE DO NOT NEED THIS BUT CHECK BEFORE REMOVING
        "text-embedding-ada-002": "dense",
        "cohere.embed-english-v3": "dense"
    }

    TOKENIZER_EQUIVALENCES = {
        "bm25--score": None,
        "text-embedding-ada-002--score": "cl100k_base",
        "sentence-transformers/facebook-dpr-ctx_encoder-single-nq-base--score": "cl100k_base",
        "cohere.embed-english-v3--score": "cl100k_base"
    }

    EMBEDDING_SPECIFIC_KEYS = ["api_key", "azure_api_version", "azure_base_url", "azure_deployment_name"]

    def __init__(self):
        """ Creates the deployment"""
        super().__init__()
        set_storage(storage_containers)

        try:
            self.origin = storage_containers.get('origin')
            self.workspace = storage_containers.get('workspace')
            file_loader = ManagerLoader().get_file_storage(
                {"type": "IRStorage", "workspace": self.workspace, "origin": self.origin})

            # Here different because platform and embedding_model not passed in api call
            self.available_pools = {}
            for key, value in file_loader.get_available_embeddings_pools().items():
                for embedding_models in value.values():
                    for pool, models in embedding_models.items():
                        self.available_pools[pool] = models

            self.available_models = []
            for key, models in file_loader.get_available_models().items():
                for m in models:
                    m["platform"] = key
                self.available_models.extend(models)

            self.all_models = file_loader.get_all_embeddings_models()
            self.default_embedding_equivalences = file_loader.get_embedding_equivalences()
            models_credentials, self.vector_storages, self.aws_credentials = load_secrets()
            # Function load_secret loads full file. Only embeddings key is necessary
            self.models_credentials = models_credentials.get('embeddings')
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
            index_name = re.sub(r'[\\/,|>?*<\" \\]', "_", f"{index}_{model.get('embedding_model')}")
            if not connector.exist_index(index_name) and model.get('embedding_model') != "bm25":
                raise ValueError(f"Model '{model.get('alias')}' does not exist for the index '{index}'")

    def get_bm25_arguments(self, index: str, connector: Connector, es_client: AsyncElasticsearch):
        """ Get the retriever from the model

        param: model: model to get the retriever from
        param: index: index to get the retriever from

        """
        for model in self.all_models:
            index_name = re.sub(r'[\\/,|>?*<\" \\]', "_", f"{index}_{model}")
            if connector.exist_index(index_name):
                # Add bm25 retriever (with one index that matches is enough, all indexes in elastic can do this retrieval)
                vector_store = ElasticsearchStoreAdaption(index_name=index_name,
                                                          es_client=es_client,
                                                          retrieval_strategy=AsyncBM25Strategy())
                # MockEmbedding used to avoid errors in the bm25 retriever (OPENAI_API_KEY mandatory)
                embed_model = MockEmbedding(embed_dim=256)
                return vector_store, embed_model, f"bm25--score"

    def get_retrievers_arguments(self, models: list, index: str, es_client: AsyncElasticsearch,
                                 connector: Connector) -> list:
        """ Gets the retrievers that exists

        :param models: String that identifies the model
        :param index: String that identifies the index

        return list of retrievers with their identifier
        """
        retrievers = []
        # add bm25 retriever first
        if "bm25" in [m.get('embedding_model') for m in models]:
            bm25 = self.get_bm25_arguments(index, connector, es_client)
            if not bm25:
                raise ValueError("There is no index that matches the passed value")
            retrievers.append(bm25)
            models.remove({"alias": "bm25", "embedding_model": "bm25"})

        # add the rest of the retrievers
        for model in models:
            index_name = re.sub(r'[\\/,|>?*<\" \\]', "_", f"{index}_{model.get('embedding_model')}")
            vector_store = ElasticsearchStoreAdaption(index_name=index_name,
                                                      es_client=es_client,
                                                      retrieval_strategy=AsyncDenseVectorStrategy())
            embed_model = get_embed_model(model, self.aws_credentials, is_retrieval=True)
            retrievers.append((vector_store, embed_model, f"{model.get('embedding_model')}--score"))
        return retrievers

    @staticmethod
    def retrieve(vector_store: ElasticsearchStoreAdaption, embed_model: BaseEmbedding, query: str, filters: dict,
                 top_k: int):
        """Retrieve documents from a retriever

        :param vector_store: Vector store to use in retrieval
        :param embed_model: Embed model to use in retrieval
        :param query: Query to retrieve
        :param filters: Filters to apply
        :param top_k: Number of documents to retrieve

        :return: List of documents
        """
        llama_filters = []
        for key, value in filters.items():
            if isinstance(value, list):
                multiple_filter = [MetadataFilter(key=key, value=v) for v in value]
            else:
                multiple_filter = [MetadataFilter(key=key, value=value)]
            llama_filters.append(MetadataFilters(filters=multiple_filter, condition=FilterCondition.OR))

        filters_call = MetadataFilters(filters=llama_filters, condition=FilterCondition.AND)

        vector_store_index = VectorStoreIndex.from_vector_store(vector_store=vector_store, embed_model=embed_model)
        retriever = vector_store_index.as_retriever(similarity_top_k=top_k, filters=filters_call)

        return retriever.retrieve(query)

    def sort_documents(self, docs, query: str, retrievers, rescoring_function: str):
        """ Return set of documents sorted. Creates a dictionary with the documents and their scores,
        it rescores them following different algorithms and then sorts them

        :param docs: Docs to sort
        :param query: Query to compute score. Only used in length and logleght
        :param retrievers: List of retrievers and their respective name
        :param rescoring_function: Rescoring function to be used

        :return: List of documents sorted
        """
        n_retrievers = len(retrievers)
        unique_docs = {}
        for doc in docs:
            if doc.id_ in unique_docs:
                # For some reason in score the mean of the scores is computed.
                # Maybe is not used for anything but it is kept just in case
                unique_docs[doc.id_].score += doc.score
                for sc_field in doc.metadata.keys():
                    if sc_field.endswith("--score"):
                        unique_docs[doc.id_].metadata[sc_field] = doc.metadata[sc_field]
            else:
                # First time stores the document
                unique_docs[doc.id_] = doc

        retriever_types = [name for _, _, name in retrievers]
        for _, doc in unique_docs.items():
            for retriever_type in retriever_types:
                if retriever_type not in doc.metadata:
                    doc.metadata[retriever_type] = 0
            doc.score = doc.score / n_retrievers
        docs = rescore_documents(unique_docs, query, rescoring_function, self.MODEL_FORMATS)

        sorted_docs = sorted(docs, key=lambda x: x.score, reverse=True)

        return sorted_docs

    @staticmethod
    def get_ids_empty_scores(ids_dict: dict):
        """Get the document ids that some models have not retrieved and thus don't have scores

        :param ids_dict (dict): Dictionary of ids that each of the models have retriever

        :return dict: ids that do not have scores
        """
        retriever_types = list(ids_dict.keys())
        ids = set([id_ for retriever in retriever_types for id_ in ids_dict[retriever]])

        if not ids:
            return None

        ids_empty_scores = {}
        for retriever_type in retriever_types:
            retriever_ids = ids_dict[retriever_type]
            ids_aux = []
            for id_ in ids:
                if id_ not in retriever_ids:
                    ids_aux.append(id_)
            ids_empty_scores[retriever_type] = ids_aux

        return ids_empty_scores

    @staticmethod
    def parse_output(sorted_documents, query: str = "", html_highlights: bool = False) -> List:
        """ Parse List of documents to a JSON-Serializable list to return

        :param sorted_documents: List of sorted documents
        :param query: Used only if html_highlights is set to True.
        :param html_highlights: If set to True it will highlight query in html format

        :return: List of parsed documents
        """
        if html_highlights and not query:
            raise KeyError("If html_highlights is set to True, query is mandatory")

        final_docs = []
        for doc in sorted_documents:
            parsed_doc = {
                "id_": doc.id_,
                "meta": doc.metadata,
                "relationships": {
                    "previous_node": doc.node.prev_node.to_dict() if doc.node.prev_node else {},
                    "next_node": doc.node.next_node.to_dict() if doc.node.next_node else {}
                },
                "content": doc.text,
                "score": doc.score

            }

            if html_highlights:
                parsed_doc['content'] = add_highlights(parsed_doc, query, doc.content)
            final_docs.append(parsed_doc)

        return final_docs

    def process(self, json_input: dict):
        try:
            input_object = ManagerParser().get_parsed_object({"type": "inforetrieval", "json_input": json_input,
                                                              "available_models": self.available_models,
                                                              "available_pools": self.available_pools,
                                                              "models_credentials": self.models_credentials})
            connector = get_connector(input_object.index, self.workspace, self.vector_storages)
            es_client = AsyncElasticsearch(hosts=f"{connector.scheme}://{connector.host}:{connector.port}",
                                           http_auth=(connector.username, connector.password),
                                           verify_certs=False, timeout=30)

            # If no models are passed, we will retrieve with bm25 and the models that have been indexed
            if len(input_object.models) == 0:
                indexed_models = ["bm25"]
                for model in self.all_models:
                    index_name = re.sub(r'[\\/,|>?*<\" \\]', "_", f"{input_object.index}_{model}")
                    if connector.exist_index(index_name):
                        indexed_models.append(self.default_embedding_equivalences[model])

                # Get the model credentials
                input_object.models = ParserInforetrieval.get_sent_models(indexed_models, self.available_pools,
                                                                          self.available_models,
                                                                          self.models_credentials)
            else:
                self.assert_correct_models(input_object.index, input_object.models, connector)

            retrievers_arguments = self.get_retrievers_arguments(input_object.models, input_object.index, es_client,
                                                                 connector)

            retrieved_ids = {}
            docs = []
            tokens_report = {}
            for vector_store, embed_model, retriever_type in retrievers_arguments:
                retrieved_ids[retriever_type] = []
                docs_tmp = self.retrieve(vector_store, embed_model, input_object.query, input_object.filters,
                                         input_object.top_k)
                # add tokens from the query to the report
                tokenizer = self.TOKENIZER_EQUIVALENCES.get(retriever_type, "cl100k_base")
                if tokenizer:
                    tokens_report[retriever_type] = len(tiktoken.get_encoding(tokenizer).encode(input_object.query))
                for doc in docs_tmp:
                    doc.metadata.update({retriever_type: doc.score})
                    if 'snippet_id' in doc.metadata:
                        retrieved_ids[retriever_type].append(doc.metadata['snippet_id'])
                docs.extend(docs_tmp)

            # Complete the scores that have not been obtained till this point
            ids_empty_scores = self.get_ids_empty_scores(retrieved_ids)
            if ids_empty_scores is not None:
                self.logger.debug(f"Re-scoring with {', '.join(list(zip(*retrievers_arguments))[2])} retrievers")
                # Complete the scores that are needed
                for vector_store, embed_model, retriever_type in retrievers_arguments:
                    new_ids = ids_empty_scores[retriever_type]
                    input_object.filters['snippet_id'] = new_ids
                    top_k_new = len(new_ids)
                    self.logger.debug(f"Retriever {retriever_type} added {top_k_new} ids")
                    if top_k_new:
                        docs_tmp = self.retrieve(vector_store, embed_model, input_object.query, input_object.filters,
                                                 top_k_new)
                        for doc in docs_tmp:
                            doc.metadata.update({retriever_type: doc.score})
                        docs.extend(docs_tmp)

            sorted_documents = self.sort_documents(docs, input_object.query, retrievers_arguments,
                                                   input_object.rescoring_function)

            for vector_store, embed_model, retriever_type in retrievers_arguments:
                vector_store.close()

            connector.close()

            for model, tokens in tokens_report.items():
                resource = f"retrieval/{model}/tokens"
                self.report_api(tokens, "", input_object.x_reporting, resource, "", "TOKENS")

            resource = f"retrieval/process/call"
            self.report_api(1, "", input_object.x_reporting, resource, "")

            return self.must_continue, {
                "status_code": 200,
                "docs": self.parse_output(sorted_documents[:input_object.top_k], input_object.query,
                                          html_highlights=input_object.add_highlights_bool),
                "status": "finished"
            }, ""

        except Exception as ex:
            self.logger.error(f"{str(ex)} for process", exc_info=get_exc_info())
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
    dat['generic'].update({"project_conf": apigw_params})
    return deploy.sync_deployment(dat)


@app.route('/delete-documents', methods=['POST'])
def delete_documents() -> Tuple[str, int]:
    """Delete documents that meet certain conditions"""
    json_input = request.get_json(force=True)
    deploy.logger.info(f"Request recieved with data: {json_input}")

    index = json_input.get('index', "")
    connector = get_connector(index, deploy.workspace, deploy.vector_storages)
    results = []

    # Deleting documents
    for model in deploy.all_models:
        index_name = re.sub(r'[\\/,|>?*<\" \\]', "_", f"{index}_{model}")
        try:
            result = connector.delete_documents(index_name, json_input['delete'])
            if len(result.body.get('failures', [])) > 0:
                deploy.logger.debug(f"Error deleting documents in index '{index_name}': {result} ")
            elif result.body.get('deleted', 0) == 0:
                deploy.logger.debug(
                    f"Documents not found for filters: '{json_input['delete']}' in index '{index_name}'")
            else:
                deploy.logger.debug(f"{result.body['deleted']} chunks deleted for '{index_name}'")
            results.append(result)
        except elasticsearch.NotFoundError:
            deploy.logger.debug(f"Index '{index_name}' not found")
    connector.close()

    # Giving response to the user based on the deletion in the different indexes
    deleted = sum(result.get('deleted', 0) for result in results)
    if deleted == 0:
        return json.dumps({'status': "error",
                           'result': f"Documents not found for filters: {json_input['delete']}",
                           'status_code': 400}), 400
    elif deleted > 0:
        return json.dumps(
            {'status': "finished", 'result': f"Documents that matched the filters were deleted for '{index}'",
             'status_code': 200}), 200
    else:
        return json.dumps(
            {'status': "error", 'result': f"Error deleting documents: {results}", 'status_code': 400}), 400


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

    for model in deploy.all_models:
        try:
            # When the first index match, return the result (must be identical)
            index_name = re.sub(r'[\\/,|>?*<\" \\]', "_", f"{index}_{model}")
            status, result, status_code = connector.get_documents(index_name, filters)
            response = {'status': status, 'result': {"status_code": status_code, "docs": result, "status": status}}
            return response, status_code
        except elasticsearch.NotFoundError:
            deploy.logger.debug(f"Index '{index}_{model}' not found")
            pass
        except Exception as ex:
            deploy.logger.error(f"Error retrieving documents: {str(ex)}", exc_info=get_exc_info())
            return {'status': "error", 'result': f"Error retrieving documents: {str(ex)}", 'status_code': 400}, 400
    return {'status': "error", 'result': f"Index '{index}' not found", 'status_code': 400}, 400


@app.route('/get_documents_filenames', methods=['POST'])
def get_documents_filenames() -> Tuple[Dict, int]:
    json_input = request.get_json(force=True)

    if not json_input or 'index' not in json_input or not json_input['index'].strip():
        return {'status': "error", 'result': "Missing parameter: index", 'status_code': 400}, 400

    index = json_input.get('index', "")

    connector = get_connector(index, deploy.workspace, deploy.vector_storages)

    for model in deploy.all_models:
        try:
            # When the first index match, return the result (must be identical)
            index_name = re.sub(r'[\\/,|>?*<\" \\]', "_", f"{index}_{model}")
            status, result, status_code = connector.get_documents_filenames(index_name)
            response = {'status': status, 'result': {"status_code": status_code, "docs": result, "status": status}}
            return response, status_code
        except elasticsearch.NotFoundError:
            deploy.logger.debug(f"Index '{index}_{model}' not found")
            pass
        except Exception as ex:
            deploy.logger.error(f"Error retrieving documents: {str(ex)}", exc_info=get_exc_info())
            return {'status': "error", 'result': f"Error retrieving documents: {str(ex)}", 'status_code': 400}, 400
    return {'status': "error", 'result': f"Index '{index}' not found", 'status_code': 400}, 400


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=False, port=8888)
