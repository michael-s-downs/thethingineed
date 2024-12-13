### This code is property of the GGAO ###


# Native imports
from abc import ABC
from typing import List

# Installed imports
from elasticsearch import Elasticsearch, RequestError

# Custom imports+
from common.utils import ELASTICSEARCH_INDEX
from common.errors.genaierrors import PrintableGenaiError

class Connector(ABC):
    MODEL_FORMAT = "Connector"

    def __init__(self, vector_storage: dict):
        self.host = vector_storage.get('vector_storage_host', '')
        self.username = vector_storage.get('vector_storage_username', '')
        self.password = vector_storage.get('vector_storage_password', '')
        self.scheme = vector_storage.get('vector_storage_scheme', 'https')
        self.port = vector_storage.get('vector_storage_port', 9200)
        self.connection = None

    def connect(self):
        """ Method to connect to the vector storage database

        """
        pass

    def assert_correct_index_metadata(self, index: str, docs: list, vector_storage_keys: list):
        """Raises an error if you try to change metadata for an already created index

        :param index: Index to check
        :param docs: Documents to check
        :param vector_storage_keys: Keys redundant
        """
        pass

    def assert_correct_index_conf(self, index: str, chunking_method: str, available_models: list, models: list):
        """Raises an error if you try to change the models for an already created index

        :param index: Index to check
        :param chunking_method: Chunking mode used
        :param available_models: Models available
        :param models: Models to check
        """
        pass

    def assert_correct_chunking_method(self, index: str, chunking_method: str, models: list):
        """Raises an error if you try to change the chunking method for an already created index

        :param index: Index to check
        :param chunking_method: Chunking mode used
        :param models: Models to check
        """
        pass

    def exist_index(self, index: str):
        """ Method to check if an index exists

        :param index: Index to check
        """
        pass

    def get_full_index(self, index: str, filters: list, offset: int = 0, size: int = 25) -> list:
        """ Method to get an index with all chunks

        :param index: Index to get
        :param offset: Documents starting point
        :param size: Size of documents

        return: List of documents from the index
        """
        pass

    def get_index_mapping(self, index: str):
        """ Method to get the index mapping

        :param index: Index to get configuration from
        """
        pass

    def get_documents(self, index: str, filters: dict, offset: int = 0, size: int = 25):
        """ Method to get document from index

        :param index: Index to get the document from
        :param filters: Dictionary of desired metadata to get documents
        :param offset: Documents starting point
        :param size: Size of documents
        """
        pass

    def delete_documents(self, index_name: str, filters: dict):
        """ Method to delete a document from an index

        :param index_name: Index to delete the document from
        :param filters: Dictionary of desired metadata to delete documents
        """
        pass

    def create_empty_index(self, index: str):
        """ Method to create an empty index

        :param index: Name of the index to create
        """
        pass

    def get_documents_filenames(self, index_name: str, size: int = 10000):
        """ Method to get the filenames from an index

        :param index_name: Index to get the filenames from
        :param size: Size of documents
        """
        pass

    def delete_index(self, index: str):
        """ Async method to create an empty index

        :param index: Name of the index to create
        """
        pass

    def close(self):
        """ Method to close the connection to the vector storage database

        """
        pass
    @classmethod
    def is_connector_type(cls, model_type):
        """Checks if a given model type is equel to the model format and thus it must be the one to use.
        """
        return model_type == cls.MODEL_FORMAT


class ElasticSearchConnector(Connector):
    MODEL_FORMAT = "elastic"

    def __init__(self, vector_storage: dict):
        super().__init__(vector_storage)

    def connect(self):
        """ Method to connect to the vector storage database

        """
        try:
            host = f"{self.scheme}://{self.host}:{self.port}"
            auth = (self.username, self.password)
            self.connection = Elasticsearch(hosts=host, http_auth=auth, verify_certs=False, timeout=30)

            result = self.connection.ping()
        except:
            raise PrintableGenaiError(400, f"Error connecting with '{self.scheme}://{self.host}:{self.port}'")
        if not result:
            raise PrintableGenaiError(400, f"Error connecting with '{self.scheme}://{self.host}:{self.port}'")

    def assert_correct_index_metadata(self, index: str, docs: list, vector_storage_keys: list):
        """Raises an error if you try to change metadata for an already created index

        :param index: Index to check
        :param docs: Documents to check
        :param vector_storage_keys: Keys redundant
        """
        extra_metadata = ["snippet_number", "snippet_id","_header_mapping", "_csv_path"] # meatadata added by us
        new_index = not self.exist_index(index)
        try:
            index_mapping = self.get_index_mapping(index)[index]['mappings']['properties']['metadata']['properties'] if not new_index else {}
        except KeyError:
            index_mapping = {}
            new_index = True

        index_meta = [key for key in index_mapping.keys() - extra_metadata if key not in vector_storage_keys]

        collection_meta = docs[0].metadata.keys() - extra_metadata
        for doc in docs:
            metadata = doc.metadata.keys() - extra_metadata
            if not all([key in collection_meta for key in metadata]):
                raise PrintableGenaiError(400,
                    f"Detected metadata discrepancies. Verify that all documents have consistent metadata keys.")
            if new_index:
                continue
            new_meta = [key for key in metadata if key not in index_meta and key not in vector_storage_keys]
            if new_meta:
                raise PrintableGenaiError(400,
                    f"Metadata keys {new_meta} do not match those in the existing index {index}. "
                    f"Check and align metadata keys. Index metadata: {list(index_meta)}")

    def assert_correct_index_conf(self, index: str, chunking_method: str, available_models: list, models: list):
        """Raises an error if you try to change the models for an already created index

        :param index: Index to check
        :param chunking_method: Chunking mode used
        :param available_models: Models available
        :param models: Models to check
        """
        # Assert correct models config
        models_used = []
        for model in available_models:
            if self.exist_index(ELASTICSEARCH_INDEX(index, model)):
                models_used.append(model)
        if len(models_used) == 0:
            return
        models_sent = [model.get('embedding_model') for model in models]
        if not all([model in models_sent for model in models_used]) or len(models_sent) != len(models_used):
            raise PrintableGenaiError(400, f"Error the models sent: '{models_sent}' must be equal to the models used in the first indexation '{models_used}'")
        # Just passed models because at this point, models used in first indexation are the same as the ones sent
        self.assert_correct_chunking_method(index, chunking_method, models_sent)

    def assert_correct_chunking_method(self, index: str, chunking_method: str, models: list):
        """Raises an error if you try to change the chunking method for an already created index

        :param index: Index to check
        :param chunking_method: Chunking mode used
        :param models: Models to check
        """
        for model in models:
            # All models sent does exist
            index_name = ELASTICSEARCH_INDEX(index, model)
            # Get the node_type used in first indexation
            try:
                result = self.connection.search(index=index_name,
                                                query={"bool": {"must": {"match_all": {}}}},
                                                size=1, from_=0)
                if len(result['hits']['hits']) == 0:
                    # The indices are empty so we can return
                    return
                node_type = result['hits']['hits'][0]['_source']['metadata']['_node_type']
            except RequestError as e:
                return "error", (f"Error: {e.info['error']['reason']} caused by: "
                                 f"{e.info['error']['caused_by']['reason']}"), 400
            # Get the metadata used in first indexation
            index_metadata = self.get_index_mapping(index_name)[index_name]['mappings']['properties']['metadata']['properties'].keys()
            if (chunking_method == "surrounding_context_window" and (node_type != "TextNode" or
                    not all(elem in index_metadata for elem in ['window', 'original_text']))):
                raise PrintableGenaiError(400, f"Error the index '{index_name}' was not indexed "
                                               f"with the chunking method '{chunking_method}' at first time.")
            else:
                if chunking_method == "simple" and (node_type != "TextNode" or any(elem in index_metadata for elem in ['window', 'original_text'])):
                    raise PrintableGenaiError(400, f"Error the index '{index_name}' was not indexed "
                                               f"with the chunking method '{chunking_method}' at first time.")
                if chunking_method == "recursive" and node_type != "IndexNode":
                    raise PrintableGenaiError(400, f"Error the index '{index_name}' was not indexed "
                                                   f"with the chunking method '{chunking_method}' at first time.")

    def exist_index(self, index: str):
        """ Method to check if an index exists"""
        if self.connection is None:
            raise PrintableGenaiError(400, f"Error the connection has not been established")
        return self.connection.indices.exists(index=index)

    def create_empty_index(self, index: str):
        """ Async method to create an empty index

        :param index: Name of the index to create
        """
        if self.connection is None:
            raise PrintableGenaiError(400, f"Error the connection has not been established")
        return self.connection.indices.create(index=index)

    def delete_index(self, index: str):
        """ Async method to create an empty index

        :param index: Name of the index to create
        """
        if self.connection is None:
            raise PrintableGenaiError(400, f"Error the connection has not been established")
        return self.connection.indices.delete(index=index)

    def get_full_index(self, index: str, filters: dict, offset: int = 0, size: int = 25) -> list:
        """ Method to get an index with all chunks

        :param index: Index to get
        :param offset: Documents starting point
        :param size: Size of documents

        return: List of documents from the index
        """
        chunks = []
        while True:
            result = self.connection.search(index=index,
                                            query={"bool": {"filter": self._generate_filters(filters), "must": {"match_all": {}}}},
                                            size=size, from_=offset)
            if len(result.get('hits', {}).get('hits', [])) == 0:
                break
            chunks.extend([chunk for chunk in result["hits"]["hits"]])
            offset += size
        if len(chunks) == 0:
            raise PrintableGenaiError(400, f"Error the index '{index}' is empty so retrieval cannot be done.")
        return chunks

    def get_index_mapping(self, index: str):
        """ Async method to get the index mapping

        :param index: Index to get configuration from
        """
        if self.connection is None:
            raise PrintableGenaiError(400, f"Error the connection has not been established")
        return self.connection.indices.get_mapping(index=index)

    def get_documents(self, index_name: str, filters: dict, offset: int = 0, size: int = 25):
        """ Method to get a document from an index

        :param index_name: Index to get the document from
        :param filters: Dictionary of desired metadata to retrieve documents
        :param offset: Documents starting point
        :param size: Size of documents
        """
        if self.connection is None:
            raise PrintableGenaiError(400, f"Error the connection has not been established")

        chunks = []
        while True:
            try:
                result = self.connection.search(index=index_name,
                                                query={"bool": {"filter": self._generate_filters(filters), "must": {"match_all": {}}}},
                                                size=size, from_=offset)
            except RequestError as e:
                return "error", (f"Error: {e.info['error']['reason']} caused by: "
                                 f"{e.info['error']['caused_by']['reason']}"), 400
            if len(result.get('hits', {}).get('hits', [])) == 0:
                break
            chunks.extend([chunk for chunk in result["hits"]["hits"]])
            offset += size
        if len(chunks) == 0:
            return "error", f"Document not found for filters: {filters}", 400

        parsed_chunks = self._parse_response(chunks)[1]

        chunks_per_file = {}
        for chunk in parsed_chunks:
            chunks_per_file[chunk.get('meta').get('filename')] = chunks_per_file.get(chunk.get('meta').get('filename'), []) + [chunk]

        for file, chunks in chunks_per_file.items():
            chunks_per_file[file] = sorted(chunks, key=lambda x: x.get('meta').get('snippet_number'))
        return "finished", chunks_per_file, 200

    def get_all_documents(self, index_name: str, offset: int = 0, size: int = 25):
        """ Method to get all documents from an index

        :param index_name: Index to get the document from
        :param offset: Documents starting point
        :param size: Size of documents

        return: List of documents from the index
        """
        chunks = []
        while True:
            try:
                result = self.connection.search(index=index_name,
                                                query={"bool": {"must": {"match_all":{}}}},
                                                size=size, from_=offset)
            except RequestError as e:
                return "error", (f"Error: {e.info['error']['reason']} caused by: "
                                 f"{e.info['error']['caused_by']['reason']}"), 400
            if len(result.get('hits', {}).get('hits', [])) == 0:
                break
            chunks.extend([chunk for chunk in result["hits"]["hits"]])
            offset += size
        if len(chunks) == 0:
            return "error", f"Index is empty", 400
        return self._parse_response(chunks)

    def delete_documents(self, index_name: str, filters: dict):
        """ Method to delete a document from an index

        :param index_name: Index to delete the document from
        :param filters: Dictionary of desired metadata to delete documents
        """
        if self.connection is None:
            raise PrintableGenaiError(400, f"Error the connection has not been established")

        body = {"query": self._generate_filters(filters)}
        return self.connection.delete_by_query(index=index_name, body=body)

    def close(self):
        """ Method to close the connection to the vector storage database
        """
        if self.connection:
            self.connection.close()

    def get_documents_filenames(self, index_name: str, size: int = 10000):
        filenames = []
        try:

            aggregation_body = {
                "size": 0,
                "aggs": {
                    "unique_documents": {
                        "terms": {
                            "field": "metadata.filename.keyword",
                            "size": size
                        }
                    }
                }
            }
            result = self.connection.search(index=index_name,
                                            body=aggregation_body)
        except RequestError as e:
            error_message = f"Error: {e.info['error']['reason']}"
            if 'caused_by' in e.info['error']:
                error_message += f" caused by: {e.info['error']['caused_by'].get('reason', '')}"
            return "error", error_message, 400

        buckets = result['aggregations']['unique_documents']['buckets']
        for bucket in buckets:
            filename = bucket['key']
            doc_count=bucket['doc_count']
            filenames.append({"filename": filename, "chunks": doc_count})

        return "finished", filenames, 200
    
    def list_indices(self):
        """Method to list all indices in the Elasticsearch database."""
        if self.connection is None:
            raise PrintableGenaiError(400, "Error: the connection has not been established")

        try:
            indices = self.connection.indices.get_alias(index="*")
            return list(indices.keys())
        except Exception as e:
            raise PrintableGenaiError(500, f"Error retrieving indices: {str(e)}")


    ############################################################################################################
    #                                                                                                          #
    #                                           PRIVATE METHODS                                                #
    #                                                                                                          #
    ############################################################################################################

    @staticmethod
    def _generate_filters(filters: dict):
        operands = []
        for key, value in filters.items():
            if isinstance(value, str):
                operands.append({"bool": {"should": {"term": {f"metadata.{key}.keyword": value}}}})
            elif isinstance(value, list) and all([isinstance(val, str) for val in value]):
                operands_should = []
                for subfilter in value:
                    operands_should.append({"term": {f"metadata.{key}.keyword": subfilter}})
                operands.append({"bool": {"should": operands_should}})
            else:
                raise PrintableGenaiError(400, f"Error the value '{value}' for the key '{key}' must be a string or a list containing strings.")
        return {"bool": {"must": operands}}

    @staticmethod
    def _parse_response(chunks: list):
        result = []
        for chunk in chunks:
            meta = {}
            for key, value in chunk.get('_source').get('metadata').items():
                if key not in ['_node_content', '_node_type', 'doc_id', 'ref_doc_id']:
                    meta[key] = value
            new_dict = {
                'id_': chunk.get('_id'),
                'meta': meta,
                'content': chunk.get('_source').get('content'),
                'score': chunk.get('_score')
            }

            result.append(new_dict)
        return "finished", result, 200


class ManagerConnector(object):
    MODEL_TYPES = [ElasticSearchConnector]

    @staticmethod
    def get_connector(conf: dict) -> Connector:
        """ Method to instantiate the document store class: [elasticsearch]

        :param conf: Model configuration. Example:  {"vector_storage_type":"elastic"}
        """
        for connector in ManagerConnector.MODEL_TYPES:
            connection_type = conf.get('vector_storage_type')
            if connector.is_connector_type(connection_type):
                return connector(conf)
        raise PrintableGenaiError(400, f"Platform type doesnt exist {conf}. "
                         f"Possible values: {ManagerConnector.get_possible_connectors()}")

    @staticmethod
    def get_possible_connectors() -> List:
        """ Method to list the endpoints: [azure, openai]

        :param conf: Model configuration. Example:  {"platform":"openai"}
        """
        return [store.MODEL_FORMAT for store in ManagerConnector.MODEL_TYPES]
