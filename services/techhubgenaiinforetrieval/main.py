### This code is property of the GGAO ###


# Native imports
from functools import lru_cache
import json
import re
import time
import random

import elasticsearch.exceptions
# Installed imports
import tiktoken
from utils import add_highlights
from rescoring import rescore_documents
from flask import Flask, request
from haystack import Document, Answer

# Custom imports
from common.deployment_utils import BaseDeployment
from common.genai_sdk_controllers import list_files
from common.genai_sdk_controllers import storage_containers, set_storage
from common.dolffia_json_parser import *
from common.services import GENAI_INFO_RETRIEVAL_SERVICE
from common.ir import IR_INDICES, INDEX_S3
from common.errors.dolffiaerrors import PrintableDolffiaError
from deletionapi import DeletionAPI
from common.indexing.loaders import ManagerLoader, DocumentLoader
from common.indexing.vector_storages import ManagerVectorDB
from common.indexing.connectors import ManagerConnector
from common.indexing.parsers import ManagerParser
from common.indexing.retrievers import Retriever

cache = lru_cache(maxsize=2000)


def aslist(generator):
    """Function decorator to transform a generator into a list
    """
    def wrapper(*args, **kwargs):
        return list(generator(*args, **kwargs))
    return wrapper


class InfoRetrievalDeployment(BaseDeployment):

    MODEL_FORMATS = {
        'bm25': "sparse",
        'sentence_transformers': "dense",
        'azure_openai': "dense"
    }
    EMBEDDING_SPECIFIC_KEYS = ["api_key", "azure_api_version", "azure_base_url", "azure_deployment_name"]
    encoding = tiktoken.get_encoding("cl100k_base")

    def __init__(self):
        """ Creates the deployment"""
        super().__init__()
        set_storage(storage_containers)

        try:
            self.origin = storage_containers.get('origin')
            self.workspace = storage_containers.get('workspace')
            file_loader = ManagerLoader().get_file_storage({"type": "s3", "workspace": self.workspace, "origin": self.origin})
            self.available_pools = file_loader.get_available_embeddings_pools()
            self.available_models = file_loader.get_available_models()
            self.load_secrets()
            self.ir_models = {}
            self.logger.debug(f"Started reading retrievers models for the state_dicts stored.")
            inicio = time.time()
            self.load_index_retrievers(file_loader)
            self.logger.debug(f"Retrievers models loaded in {round(time.time() - inicio, 2)} seconds.")
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

    def load_secrets(self):
        models_keys_path = os.path.join(os.getenv('SECRETS_PATH', '/secrets'), "models", "models.json")
        vector_storages_path = os.path.join(os.getenv('SECRETS_PATH', '/secrets'), "vector_storage", "vector_storage_config.json")

        # Load models credentials
        if os.path.exists(models_keys_path):
            with open(models_keys_path, "r") as file:
                self.models_credentials = json.load(file).get("embeddings")
        else:
            raise FileNotFoundError(f"Credentials file not found {models_keys_path}.")

        # Load vector storages credentials
        if os.path.exists(vector_storages_path):
            with open(vector_storages_path, "r") as file:
                self.vector_storages = json.load(file).get("vector_storage_supported")
        else:
            raise FileNotFoundError(f"Vector storages file not found {vector_storages_path}.")

    def load_index_retrievers(self, file_loader: DocumentLoader):
        """ Loads the index configurations from state dicts

        """
        self.ir_models = {}
        try:
            files = list_files(self.workspace, IR_INDICES)
        except KeyError:
            raise ValueError(f"No indices found in {IR_INDICES} folder for the workspace: {self.workspace}")

        for file in files:
            self.logger.debug("Loading index: " + str(file))
            if not file.endswith(".json"):
                continue
            index = os.path.splitext(os.path.basename(file))[0]
            state_dict = file_loader.load_file(self.workspace, file)
            if state_dict:
                state_dict = json.loads(state_dict)
            else:
                self.logger.error(f"Index {index} is empty.")
                continue

            vector_storage = self.get_vector_storage(state_dict.get('vector_storage'))
            if not vector_storage:
                self.logger.error(f" Vector storage: {state_dict.get('vector_storage')} has no vector storage.")

            self.ir_models[index] = {
                'vector_storage': vector_storage,
                'retrievers': {}
            }

    def load_models(self, index, vector_storage, state_dict):
        """ Loads a model configuration for an index, vector_storage and state_dict

        param: index: index to load the configuration
        param: vector_storage: vector storage configuration
        param: state_dict: index models and vector storage data
        """
        # Add the retrievers objects to the ir_models dict
        connector = ManagerConnector().get_connector(vector_storage)
        for model in state_dict.get('models', []):
            vector_db = ManagerVectorDB.get_vector_database(
                {"type": "UhiStack", "connector": connector, "workspace": self.workspace, "origin": self.origin})

            # Special config for bm25
            if model.get("alias") == "bm25":
                try:
                    vector_db.set_document_store(index=index, config=vector_storage,
                                            vector_storage_type=vector_storage.get('vector_storage_type'),
                                            embedding_model=None,
                                            similarity_function=None)
                except ConnectionError:
                    self.ir_models.pop(index)
                    self.logger.error(f"Index {index} connection to elastic: {vector_storage} is not available.", exc_info=get_exc_info())
                    raise ConnectionError(f"Index {index} connection to elastic: {vector_storage} is not available.")
                vector_db.set_retriever(config={})
                self.ir_models[index]['retrievers'][model.get('alias')] = vector_db.retriever
                continue
            try:
                vector_db.set_document_store(index=index, config=vector_storage,
                                             vector_storage_type=vector_storage.get('vector_storage_type'),
                                             embedding_model=model.get('embedding_model'),
                                             retriever=model.get('retriever'), column_name=model.get('column_name'),
                                             similarity_function=model.get("similarity_function", None))
            except ConnectionError:
                self.ir_models.pop(index)
                self.logger.error(f"Index {index} connection to elastic: {vector_storage} is not available.", exc_info=get_exc_info())
                raise ConnectionError(f"Index {index} connection to elastic: {vector_storage} is not available.")
            retriever_config = self.get_retriever_config(model)
            vector_db.set_retriever(config={key: retriever_config.get(key) for key in self.EMBEDDING_SPECIFIC_KEYS},
                                    retriever=retriever_config.get('retriever'), embedding_model=retriever_config.get('embedding_model'))
            self.ir_models[index]['retrievers'][model.get('alias')] = vector_db.retriever

    def get_vector_storage(self, vector_storage_name: str) -> dict:
        """ Gets the vector storage credentials from an alias

        param: vector_storage_name: vector storage name to get data from
        """
        vector_storage = None
        for vs in self.vector_storages:
            if vs.get("vector_storage_name") == vector_storage_name:
                vector_storage = vs
        return vector_storage

    def get_retriever_config(self, state_dict_retriever: dict) -> dict:
        """ Gets the retriever object from a state_dict configuration

        """
        model_selected = {}
        retriever = state_dict_retriever.get('retriever')
        embedding_model = state_dict_retriever.get('embedding_model')
        alias = state_dict_retriever.get('alias')
        # Test if the alias is a pool
        if alias in self.available_pools.get(retriever, {}).get(embedding_model, []):
            alias = random.choice(self.available_pools.get(retriever).get(embedding_model).get(alias).copy())
        # Get the model parameters based on the alias

        for m in self.available_models.get(retriever, []):
            if alias == m.get('embedding_model_name'):
                model_selected = m
                break
        if not model_selected:
            self.logger.info(f"Model {state_dict_retriever.get('alias')} not found in available models")
        if not state_dict_retriever.get('retriever_model'):
            return {
                "alias": state_dict_retriever.get('alias'),
                "embedding_model": state_dict_retriever.get('embedding_model'),
                "column_name": state_dict_retriever.get('column_name', "_" + str(hash(state_dict_retriever['embedding_model']))),
                "retriever": state_dict_retriever.get('retriever'),
                "api_key": self.models_credentials[retriever][alias]['api_key'],
                "azure_api_version": model_selected.get("azure_api_version"),
                "azure_base_url": self.models_credentials[retriever][alias]['azure_base_url'],
                "azure_deployment_name": model_selected.get("azure_deployment_name")
            }
        else:
            return {
                "alias": state_dict_retriever.get('alias'),
                "embedding_model": state_dict_retriever.get('embedding_model'),
                "column_name": state_dict_retriever.get('column_name', "_" + str(hash(state_dict_retriever['embedding_model']))),
                "retriever": state_dict_retriever.get('retriever'),
                "retriever_model": state_dict_retriever.get('retriever_model'),
                "similarity_function": state_dict_retriever.get('similarity')
            }

    def index_in_ir_models(self, index: str):
        """ Loads the configuration for an index (retrievers, vector_storages...)

        param: index: index to get configuration
        """
        state_dict = self.ir_models.get(index)
        if not state_dict or len(state_dict.get('retrievers')) == 0:
            file_loader = ManagerLoader().get_file_storage({"type": "s3", "workspace": self.workspace, "origin": self.origin})
            state_dict = file_loader.load_file(self.workspace, INDEX_S3(index))
            if state_dict:
                state_dict = json.loads(state_dict)
            else:
                raise ValueError(f"Index {index} is empty.")


            vector_storage = self.get_vector_storage(state_dict.get('vector_storage'))
            if not vector_storage:
                raise ValueError(f" Vector storage: {state_dict.get('vector_storage')} has no vector storage.")

            self.ir_models[index] = {
                'vector_storage': vector_storage,
                'retrievers': {}
            }
            self.load_models(index, vector_storage, state_dict)

    @staticmethod
    def exact_search_filters(query, filters):
        """Adds the filter when the search contains quotation marks \"

        Args:
            query (str): Input query
            filters (dict): Dictionary with elasticsearch-like filters

        Returns:
            _type_: _description_
        """
        matches = re.findall(r'"(.*?)"', query)
        if matches:
            if 'match_phrase' not in filters:
                filters['match_phrase'] = []

            filters['match_phrase'].extend([{"match_phrase": {"content": match}} for match in matches])
        return query.replace('"', ""), filters

    @staticmethod
    @aslist
    def get_retrievers(retrievers: dict, models: List[str]):
        """ It filters the loaded retrievers and yields them with their identifier

        :param retrievers: Any of the classes that inherits from Retriever
        :param models: String that identifies the model

        return Generator(Retriever, retriever identifier)
        """
        embedding_models = [r.embedding_model for name, r in retrievers.items()]
        models = [m if m != "bm25" else None for m in models]  # bm25 is the key you use to call, transform it to None

        for model in models:
            if model not in embedding_models:
                raise ValueError(
                    f"You sent a model {model} that was not in the models configuration. Consider reindexing or sending another model")

        for name, r in retrievers.items():
            embedding_model = r.embedding_model
            if not models:
                yield (r, f"{r.mf}--{embedding_model}--score")
            else:
                for model in models:
                    if model == embedding_model:
                        yield (r, f"{r.mf}--{embedding_model}--score")

    def retrieve(self, retriever, query, filters, top_k, retry=0, max_retries=3):
        """Retrieve documents from a retriever

        :param retriever: Retriever to use
        :param query: Query to retrieve
        :param filters: Filters to apply
        :param top_k: Number of documents to retrieve
        :param retry: Number of retries
        :param max_retries: Max number of retries

        :return: List of documents
        """
        if retry >= max_retries:
            raise PrintableDolffiaError(400, "Max retries reached, OpenAI non reachable.")
        try:
            return retriever.retrieve(query=query, filters=filters.copy() if filters else None, top_k=top_k)
        except Exception as e:
            self.logger.warning(f"Error retrieving documents. Retry: {retry + 1}/{max_retries}", exc_info=get_exc_info())
            return self.retrieve(retriever, query, filters, top_k, retry=retry + 1, max_retries=max_retries)

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

    def sort_documents(self, docs: List[Union[Document, Answer]], query: str, retrievers: List[Union[Retriever, str]],
                       rescoring_function: str) -> List[Document]:
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
            if doc.id in unique_docs:
                unique_docs[doc.id].score += doc.score / n_retrievers
                for sc_field in doc.meta.keys():
                    if sc_field.endswith("--score"):
                        unique_docs[doc.id].meta[sc_field] = doc.meta[sc_field]
            else:
                # First time stores the document
                unique_docs[doc.id] = doc

        retriever_types = [name for _, name in retrievers]
        for _, doc in unique_docs.items():
            for retriever_type in retriever_types:
                if retriever_type not in doc.meta:
                    doc.meta[retriever_type] = 0

        docs = rescore_documents(unique_docs, query, rescoring_function, self.MODEL_FORMATS)

        sorted_docs = sorted(docs, key=lambda x: x.score, reverse=True)

        return sorted_docs

    @staticmethod
    def parse_output(sorted_documents: List[Document], query: str = "", html_highlights: bool = False) -> List:
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
            parsed_doc = doc.to_dict()
            try:
                parsed_doc.update({
                    'retrieval_score': doc.retrieval_score,
                })
                if hasattr(doc, "answer"):
                    doc_ = doc.to_dict()
                    parsed_doc.update({
                        'answer': doc_['answer'],
                        'offsets_in_document': doc_['offsets_in_document']
                    })
            except:
                pass

            if html_highlights:
                parsed_doc['content'] = add_highlights(parsed_doc, query, doc.content)
            final_docs.append(parsed_doc)

        return final_docs

    def process(self, json_input: dict):
        try:
            input_object = ManagerParser().get_parsed_object({"type":"inforetrieval", "json_input": json_input})

            self.index_in_ir_models(input_object.index)
            query, filters = self.exact_search_filters(input_object.query, input_object.filters)
            self.logger.debug(f"Filters: {filters}")

            retrievers = self.get_retrievers(self.ir_models[input_object.index]['retrievers'], input_object.models)

            retrieved_ids = {}
            docs = []
            tokens_report = {}
            for retriever, retriever_type in retrievers:
                retrieved_ids[retriever_type] = []
                docs_tmp = self.retrieve(retriever, query, filters, input_object.top_k)
                #add tokens from the query to the report
                if retriever.mf == "openai":
                    tokens = self.encoding.encode(query)
                    tokens_report[retriever.embedding_model] = len(tokens)
                for doc in docs_tmp:
                    doc.meta.update({retriever_type: doc.score})
                    if 'snippet_id' in doc.meta:
                        retrieved_ids[retriever_type].append(doc.meta['snippet_id'])
                docs.extend(docs_tmp)

            # Complete the scores that have not been obtained till this point
            ids_empty_scores = self.get_ids_empty_scores(retrieved_ids)
            if ids_empty_scores is not None:
                self.logger.debug(f"Re-scoring with {', '.join(list(zip(*retrievers))[1])} retrievers")
                # Complete the scores that are needed
                for retriever, retriever_type in retrievers:
                    new_ids = ids_empty_scores[retriever_type]
                    filters['snippet_id'] = new_ids
                    top_k_new = len(new_ids)
                    self.logger.debug(f"Retriever {retriever_type} added {top_k_new} ids")
                    if top_k_new:
                        docs_tmp = self.retrieve(retriever, query, filters, top_k_new)
                        for doc in docs_tmp:
                            doc.meta.update({retriever_type: doc.score})
                        docs.extend(docs_tmp)

            sorted_documents = self.sort_documents(docs, query, retrievers, input_object.rescoring_function)

            for model, tokens in tokens_report.items():
                resource = f"retrieval/{model}/tokens"
                self.report_api(tokens, "", input_object.x_reporting, resource, "", "TOKENS")

            resource = f"retrieval/process/call"
            self.report_api(1, "", input_object.x_reporting, resource, "")

            return self.must_continue, {
                "status_code": 200,
                "docs": self.parse_output(sorted_documents[:input_object.top_k], query, html_highlights=input_object.add_highlights_bool),
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
    deletion_api = DeletionAPI()
    json_input = request.get_json(force=True)
    try:
        deletion_api.check_input(json_input)
        return deletion_api.delete(json_input)
    except ValueError as e:
        return json.dumps({'status': "error", 'result': str(e), 'status_code': 400}), 400
    except elasticsearch.exceptions.ConnectionError:
        return json.dumps({'status': "error",
                           'result': 'Error connecting to the vector database (maybe is unreachable)',
                           'status_code': 500}), 500
    except elasticsearch.exceptions.AuthenticationException:
        return json.dumps({'status': "error",
                           'result': 'Error authenticating to the vector database (maybe wrong credentials)',
                           'status_code': 401}), 401
@app.route('/healthcheck', methods=['GET'])
def healthcheck() -> Dict:
    return {"status": "Service available"}


@app.route('/retrieve_documents', methods=['POST'])
def retrieve_documents() -> Tuple[Dict, int]:
    dat = request.get_json(force=True)
    index = dat.get('index', "")
    filters = dat.get('filters', {})
    if len(filters) == 0:
        response = {'status': "error", 'result': "Filters are empty", 'status_code': 400}
        return response, 400
    ir_model = deploy.ir_models.get(index, None)
    if ir_model is None:
        response = {'status': "error", 'result': "Index not found", 'status_code': 400}
        return response, 400
    connector = ManagerConnector().get_connector(ir_model.get('vector_storage'))
    connector.connect()

    status, result, status_code = connector.get_documents(index, filters)
    response = {'status': status, 'result': {"status_code": status_code, "docs": result, "status": status}}
    return response, status_code


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=False, port=8888)
