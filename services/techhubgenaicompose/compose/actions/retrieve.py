### This code is property of the GGAO ###


import os
import json
import requests
from copy import deepcopy
from abc import abstractmethod, ABC
from typing import List, Dict, Union
from common.errors.genaierrors import PrintableGenaiError


class RetrieveMethod(ABC):
    """
    Abstract base class for retrieve methods.
    """

    TYPE: str

    def __init__(self, params: Union[List, Dict]) -> None:
        """
        Initialize the retrieve method with the given parameters.

        Args:
            params (Union[List, Dict]): The parameters for the retrieve method.
        """
        self.params = deepcopy(params)

    @abstractmethod
    def process(self) -> List:
        """
        Process the retrieve method.

        Returns:
            List: The processed result.
        """
        pass

    def get_example(self):
        """
        Get an example of the retrieve method.

        Returns:
            str: The example in JSON format.
        """
        return json.dumps(self._get_example())

    @abstractmethod
    def _get_example(self) -> Dict:
        """
        Get an example of the retrieve method.

        Returns:
            Dict: The example.
        """
        return {}


class DoNothing(RetrieveMethod):
    """
    Retrieve method that does nothing.
    """

    TYPE = "streamlist"

    def process(self):
        """
        Process the retrieve method.

        Returns:
            List: The processed result.
        """
        return self.params['streamlist']

    def _get_example(self) -> Dict:
        """
        Get an example of the retrieve method.

        Returns:
            Dict: The example.
        """
        return {
            "type": "streamlist",
            "params": [{
                "content": "example text",
                "meta": {
                    "field1": "value1"
                },
                "scores": {
                    "bm25": 1,
                    "sim-example": 0.9
                }
            }]
        }


class ChunksRetriever(RetrieveMethod):
    """
    Retrieve method that obtains the chunks.
    """

    TYPE = "get_chunks"

    URL = os.environ['URL_RETRIEVE']
    TEMPLATE = {
        "indexation_conf": {
            "task": "retrieve",
            "template_name": "system_query_and_context",
        }
    }

    HEADERS = {
        'Content-type': 'application/json'
    }

    def process(self):
        """
        Process the retrieve method.

        Returns:
            List: The processed result.
        """
        headers = deepcopy(self.HEADERS)
        template = deepcopy(self.TEMPLATE)

        template.update(self.params)
        headers.update(self.params.pop("headers_config", {}))

        try:
            if template['indexation_conf']['query'] == "":
                raise PrintableGenaiError(status_code=400, message="Query is empty, cannot retrieve")
        except KeyError:
            raise PrintableGenaiError(status_code=404, message="Query not found in the template, cannot retrieve")

        response = requests.post(self.URL, json=template, headers=headers, verify=True)
        if response.status_code != 200:
            raise PrintableGenaiError(status_code=response.status_code,
                                      message=f"Error from genai-inforetrieval: {response.content}")

        docs = response.json()['result']['docs']
        response_docs = [{
            "content": doc['content'],
            "meta": {key: value for key, value in doc['meta'].items() if
                     not (key.startswith("_") or key.endswith("--score"))},
            "scores": {key: doc['meta'][key] for key in doc['meta'] if key.endswith("--score")},
            "score": doc.get("score"),
            "answer": doc.get("answer")
        } for doc in docs]

        if response_docs == []:
            raise PrintableGenaiError(status_code=404, message="Error after calling retrieval. NO documents found")
        else:
            return response_docs

    def _get_example(self) -> Dict:
        """
        Get an example of the retrieve method.

        Returns:
            Dict: The example.
        """
        return {
            "type": "get_chunks",
            "params": self.TEMPLATE
        }


class DocumentsRetriever(RetrieveMethod):
    """
    Retrieve method that retrieves documents.
    """

    TYPE = "get_documents"

    URL = os.environ['URL_RETRIEVE'].replace("process", "retrieve_documents")

    HEADERS = {
        'Content-type': 'application/json'
    }

    def process(self):
        """
        Process the retrieve method by retrieving documents.

        Returns:
            List: The processed result.
        """
        headers = deepcopy(self.HEADERS)
        headers.update(self.params.get("headers_config", {}))

        response = requests.post(self.URL, json=self.params, headers=headers, verify=True)
        if response.status_code != 200:
            raise PrintableGenaiError(status_code=response.status_code,
                                      message=f"Error from Retrieval: {response.content}")

        docs = response.json()['result']['docs']
        result = []
        for doc in docs:
            common_pairs = docs[doc][0]['meta'].copy()
            text = docs[doc][0]['content']
            for d in docs[doc][1:]:
                common_pairs = {k: v for k, v in common_pairs.items() if k in d['meta'] and d['meta'][k] == v}
                text = text + d['content']
            result.append({'content': text, 'meta': common_pairs, 'scores': {}, 'answer': ""})

        return result

    def _get_example(self) -> Dict:
        """
        Get an example of the retrieve method.

        Returns:
            Dict: The example.
        """
        return {
            "type": "get_documents",
            "params": {
                "index": "example_index",
                "filters": {}
            }
        }


class RetrieverFactory:
    """
    Factory class for creating retrieve methods.
    """

    FILTERS = [DoNothing, ChunksRetriever, DocumentsRetriever]

    def __init__(self, filter_type: str) -> None:
        """
        Initialize the retriever factory with the given filter type.

        Args:
            filter_type (str): The type of the filter.
        """
        self.retrievermethod = None
        for retrievermethod in self.FILTERS:
            if retrievermethod.TYPE == filter_type:
                self.retrievermethod = retrievermethod
                break

        if self.retrievermethod is None:
            raise PrintableGenaiError(status_code=404,
                                      message=f"Provided retriever type does not match any of the possible ones: {', '.join(f.TYPE for f in self.FILTERS)}")

    def process(self, params: dict):
        """
        Process the retrieve method with the given parameters.

        Args:
            params (dict): The parameters for the retrieve method.

        Returns:
            List: The processed result.
        """
        return self.retrievermethod(params).process()
