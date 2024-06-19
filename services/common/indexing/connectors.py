### This code is property of the GGAO ###


# Native imports
from abc import ABC
from typing import List

# Installed imports
from elasticsearch import Elasticsearch


class Connector(ABC):
    MODEL_FORMAT = "Connector"

    def __init__(self, vector_storage: dict):
        self.host = vector_storage.get('vector_storage_host', '')
        self.username = vector_storage.get('vector_storage_username', '')
        self.password = vector_storage.get('vector_storage_password', '')
        self.scheme = "https"
        self.connection = None

    def connect(self):
        """ Method to connect to the vector storage database

        """
        pass

    def exist_index(self, index: str):
        """ Method to check if an index exists

        :param index: Index to check
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

    def delete_document(self, index:str, filters: dict):
        """ Method to delete a document from an index

        :param index: Index to delete the document from
        :param filters: Dictionary of desired metadata to delete documents
        """
        pass

    def close(self):
        """ Method to close the connection to the vector storage database

        """
        pass
    @classmethod
    def is_platform_type(cls, model_type):
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
            host = f"{self.scheme}://{self.host}:9200"
            auth = (self.username, self.password)
            self.connection = Elasticsearch(hosts=host, http_auth=auth, verify_certs=False)
        except:
            raise ValueError(f"[Error] The credentials provided for {self.username}, {self.password}, {self.host}, "
                             f"{self.scheme} are wrong")

    def exist_index(self, index: str):
        """ Method to check if an index exists

        :param index: Index to check
        """
        if self.connection is None:
            raise ValueError(f"Error the connection has not been established")
        return self.connection.indices.exists(index=index)

    def get_index_mapping(self, index: str):
        """ Method to get the index mapping

        :param index: Index to get configuration from
        """
        if self.connection is None:
            raise ValueError(f"Error the connection has not been established")
        return self.connection.indices.get_mapping(index=index)

    def get_documents(self, index_name: str, filters: dict, offset: int = 0, size: int = 25):
        """ Method to get a document from an index

        :param index_name: Index to get the document from
        :param filters: Dictionary of desired metadata to retrieve documents
        :param offset: Documents starting point
        :param size: Size of documents
        """
        if self.connection is None:
            raise ValueError(f"Error the connection has not been established")
        filters_elastic = [{"term": {key: value}} for key, value in filters.items()]
        chunks = []
        while True:
            result = self.connection.search(index=index_name,
                                            query={"bool": {"filter": filters_elastic, "must": [{"match_all": {}}]}},
                                            size=size, from_=offset)
            if len(result.get('hits', {}).get('hits', [])) == 0:
                break
            chunks.extend([chunk for chunk in result["hits"]["hits"]])
            offset += size
        if len(chunks) == 0:
            return "error", f"Document not found for filters: {filters}", 400
        return self._parse_response(chunks)

    def delete_document(self, index_name: str, filters: dict):
        """ Method to delete a document from an index

        :param index_name: Index to delete the document from
        :param filters: Dictionary of desired metadata to delete documents
        """
        if self.connection is None:
            raise ValueError(f"Error the connection has not been established")
        result = self.connection.delete_by_query(index=index_name, body={'query': {'terms': filters}})
        return result

    @staticmethod
    def _parse_response(chunks: list):
        result = []
        for chunk in chunks:
            new_dict = {'content': chunk.get('_source').get('content'),
                        'content_type': chunk.get('_source').get('content_type'),
                        'score': chunk.get('_score'),
                        'meta': {key: value for key, value in chunk.get('_source').items() if key not in ['content', 'content_type']}}
            result.append(new_dict)
        return "finished", result, 200

    def close(self):
        pass


class ManagerConnector(object):
    MODEL_TYPES = [ElasticSearchConnector]

    @staticmethod
    def get_connector(conf: dict) -> Connector:
        """ Method to instantiate the document store class: [elasticsearch]

        :param conf: Model configuration. Example:  {"vector_storage_type":"elastic"}
        """
        for connector in ManagerConnector.MODEL_TYPES:
            connection_type = conf.get('vector_storage_type')
            if connector.is_platform_type(connection_type):
                return connector(conf)
        raise ValueError(f"Platform type doesnt exist {conf}. "
                         f"Possible values: {ManagerConnector.get_possible_platforms()}")

    @staticmethod
    def get_possible_platforms() -> List:
        """ Method to list the endpoints: [azure, openai]

        :param conf: Model configuration. Example:  {"platform":"openai"}
        """
        return [store.MODEL_FORMAT for store in ManagerConnector.MODEL_TYPES]
