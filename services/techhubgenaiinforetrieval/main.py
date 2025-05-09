### This code is property of the GGAO ###


# Native imports

# Installed imports
import tiktoken

from flask import Flask, request
from elasticsearch_adaption import \
    ElasticsearchStoreAdaption  # Custom class that adapts the elasticsearch store to improve filters
# In the version 0.3.3 it's impossible to use multiple filters in the same query
from elasticsearch.helpers.vectorstore import AsyncDenseVectorStrategy, AsyncBM25Strategy
from elasticsearch import AsyncElasticsearch
from llama_index.core import MockEmbedding


# Custom imports
from common.deployment_utils import BaseDeployment
from common.genai_controllers import storage_containers, set_storage
from common.genai_json_parser import *
from retrieval_strategies import ManagerRetrievalStrategies
from common.services import GENAI_INFO_RETRIEVAL_SERVICE
from common.ir.utils import get_connector, get_embed_model 
from common.utils import load_secrets, ELASTICSEARCH_INDEX
from common.storage_manager import ManagerStorage
from common.ir.parsers import ManagerParser, ParserInforetrieval
from common.ir.connectors import Connector
from common.errors.genaierrors import PrintableGenaiError

from endpoints import get_documents_filenames_handler, retrieve_documents_handler, get_models_handler, delete_documents_handler, delete_index_handler, list_indices_handler


class InfoRetrievalDeployment(BaseDeployment):

    TOKENIZER = {
        "bm25--score": None
    }

    STRATEGY_CHUNKING_METHOD_EQUIVALENCE = {
        "genai_retrieval": "simple",
        "recursive_genai_retrieval": "recursive",
        "surrounding_genai_retrieval": "surrounding_context_window",
        "llamaindex_fusion": "simple"
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
                return vector_store, model

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
                vector_store, _ = self.get_bm25_vector_store(index, connector, es_client)
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

    def get_default_models(self, input_object: ParserInforetrieval, connector: Connector):
        """ Get the default models for the index

        :param index: index to get the default models

        :return: List of default models
        """
        indexed_models = ["bm25"]
        for model in self.all_models:
            if connector.exist_index(ELASTICSEARCH_INDEX(input_object.index, model)):
                indexed_models.append(self.default_embedding_equivalences[model])
    
        # Get the model credentials
        return input_object.get_sent_models(indexed_models, self.available_pools, self.available_models,
                                                   self.models_credentials)

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
                input_object.models = self.get_default_models(input_object, connector)
            else:
                self.assert_correct_models(input_object.index, input_object.models, connector)

            # Check if the strategy selected can be done with the index passed
            chunking_method = self.STRATEGY_CHUNKING_METHOD_EQUIVALENCE[input_object.strategy]
            if len(input_object.models) == 1 and input_object.models[0]['alias'] == "bm25":
            # In the case that only bm25 is passed, the index with the model used must be searched
                _, model = self.get_bm25_vector_store(input_object.index, connector, es_client)
                models = [model]
            else:
                models = [model['embedding_model'] for model in input_object.models if model['alias'] != "bm25"]
            connector.assert_correct_chunking_method(input_object.index, chunking_method, models)

            retrievers_arguments = self.get_retrievers_arguments(input_object.models, input_object.index, es_client,
                                                                 connector, input_object.query)
            conf = {"strategy": input_object.strategy}
            if input_object.strategy == "recursive_genai_retrieval":
                # Needed to get the whole index for every retriever
                conf["connector"] = connector
            retrieval_strategy = ManagerRetrievalStrategies.get_retrieval_strategy(conf)
            sorted_documents = retrieval_strategy.do_retrieval_strategy(input_object, retrievers_arguments)

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

@app.route('/delete_documents', methods=['DELETE'])
def delete_documents() -> Tuple[str, int]:
    return delete_documents_handler(deploy, request)

@app.route('/delete_index', methods=['DELETE'])
def delete_index() -> Tuple[str, int]:
    return delete_index_handler(deploy, request)

@app.route('/healthcheck', methods=['GET'])
def healthcheck() -> Dict:
    return {"status": "Service available"}

@app.route('/retrieve_documents', methods=['POST'])
def retrieve_documents() -> Tuple[Dict, int]:
    return retrieve_documents_handler(deploy, request)

@app.route('/get_documents_filenames', methods=['GET'])
def get_documents_filenames() -> Tuple[Dict, int]:
    return get_documents_filenames_handler(deploy, request)

@app.route('/get_models', methods=['GET'])
def get_available_models() -> Tuple[Dict, int]:
    return get_models_handler(deploy)

@app.route('/list_indices', methods=['GET'])
def list_indices() -> Tuple[Dict, int]:
    return list_indices_handler(deploy)

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=False, port=8888, use_reloader=False)
