### This code is property of the GGAO ###


import os
import json
import requests
from copy import deepcopy
import numpy as np
from typing import List, Dict
from abc import abstractmethod, ABC
from common.errors.genaierrors import PrintableGenaiError


class RescoreMethod(ABC):
    """
    Abstract base class for rescore methods.
    """

    TYPE: str 

    def __init__(self, streamlist: list) -> None:
        """
        Instantiate a RescoreMethod object.

        Args:
            streamlist (list): List of stream objects.
        """
        self.streamlist = streamlist

    def get_scores(self) -> np.array:
        """
        Get the scores from the streamlist.

        Returns:
            np.array: Array of scores.
        """
        scores = []
        for stream in self.streamlist:
            scores.append(list(zip(*stream.scores.items()))[1])
        return np.array(scores)

    @abstractmethod
    def process(self) -> List:
        """
        Abstract method to process the streamlist given the method.

        Returns:
            List: Processed streamlist.
        """
        pass

    def get_example(self):
        """
        Get an example of the rescore method.

        Returns:
            str: JSON string representing the example.
        """
        return json.dumps(self._get_example())

    @abstractmethod
    def _get_example(self) -> Dict:
        """
        Abstract method to return an example of the rescore method.

        Returns:
            Dict: Example of the rescore method.
        """
        pass


class AverageRescore(RescoreMethod):
    """
    Rescore method that calculates the mean score.
    """

    TYPE = "mean"

    def process(self, params: dict = None):
        """
        Process the streamlist by calculating the mean score.

        Args:
            params (dict, optional): Additional parameters. Defaults to None.

        Returns:
            List: Processed streamlist.
        """
        scores = self.get_scores()
        for stream, scores in zip(self.streamlist, scores):
            stream.scores = {"mean": scores.mean()}
        return self.streamlist

    def _get_example(self) -> Dict:
        """
        Get an example of the average rescore method.

        Returns:
            Dict: Example of the average rescore method.
        """
        return {
            "type": "mean",
            "params": None
        }


class GenaiRescorer(RescoreMethod):
    """
    Rescore method that calls with another sparse/dense model.
    """

    TYPE = "genai_rescorer"

    URL = os.environ['URL_RETRIEVE']
    TEMPLATE = {
        "generic": {
            "process_type": "ir_retrieve",
            "index_conf": {
                "task": "retrieve",
                "template_name": "system_query_and_context",
            }
        },
        "credentials": {},
        "specific": {'dataset': {'dataset_key': ''}}
    }

    HEADERS = {
        'Content-type': 'application/json'
    }

    def get_document_ids(self):
        """
        Get the document IDs from the streamlist.

        Yields:
            str: Document ID.
        """
        for sl in self.streamlist:
            if "snippet_id" not in sl.meta:
                raise PrintableGenaiError(status_code=404,
                                          message="Streamlist must have a 'snippet_id' key that identifies the passage on an index.")
            yield sl.meta['snippet_id']

    def process(self, params: dict = None):
        """
        Process the streamlist by calling with another model.

        Args:
            params (dict, optional): Additional parameters. Defaults to None.

        Returns:
            List: Processed streamlist.
        """
        headers = deepcopy(self.HEADERS)
        template = deepcopy(self.TEMPLATE)

        template.update(params)
        headers.update(params.pop("headers_config", {}))

        template['generic']['index_conf'].setdefault('filters', {})['snippet_id'] = list(self.get_document_ids())

        response = requests.post(self.URL, json=template, headers=headers, verify=True)
        if response.status_code != 200:
            raise PrintableGenaiError(status_code=response.status_code, message=str(response.content))

        docs = response.json()['result']['docs']
        return [{
            "content": doc['content'],
            "meta": {key: value for key, value in doc['meta'].items() if
                     not (key.startswith("_") or key.endswith("--score"))},
            "scores": {key: doc['meta'][key] for key in doc['meta'] if key.endswith("--score")},
            "answer": doc.get("answer")
        } for doc in docs]

    def _get_example(self) -> Dict:
        """
        Get an example of the Genai rescore method.

        Returns:
            Dict: Example of the Genai rescore method.
        """
        return {
            "type": "genai",
            "params": self.TEMPLATE
        }


class RescoreFactory:
    """
    Factory class for creating rescore methods.
    """

    RESCORERS = [AverageRescore, GenaiRescorer]

    def __init__(self, rescore_type: str) -> None:
        """
        Instantiate a RescoreFactory object.

        Args:
            rescore_type (str): Type of rescore method.
        """
        self.rescoremethod = None
        for rescoremethod in self.RESCORERS:
            if rescoremethod.TYPE == rescore_type:
                self.rescoremethod = rescoremethod
                break

        if self.rescoremethod is None:
            raise PrintableGenaiError(status_code=404,
                                      message=f"Provided rescore does not match any of the possible ones: {', '.join(f.TYPE for f in self.RESCORERS)}")

    def process(self, streamlist: list, params: dict):
        """
        Process the streamlist with the given method.

        Args:
            streamlist (list): List of stream objects.
            params (dict): Additional parameters.

        Returns:
            List: Processed streamlist.
        """
        return self.rescoremethod(streamlist).process(params)
