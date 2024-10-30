### This code is property of the GGAO ###


import os
import json
import asyncio
import aiohttp

from copy import deepcopy
import random
import requests
from typing import List, Dict
from abc import abstractmethod, ABC
from common.errors.LLM import LLMParser
from common.errors.genaierrors import PrintableGenaiError
from ..utils.defaults import FILTER_TEMPLATE
from common.genai_controllers import load_file, storage_containers

LLMP = LLMParser()
S3_QUERYFILTERSPATH = "src/compose/filter_templates"


class FilterResponseMethod(ABC):
    TYPE: str = None

    def __init__(self, streamlist: list) -> None:
        """Instantiate streamlist

        Args:
            streamlist (list): Streamlist
        """
        self.streamlist = streamlist
        self.response = None

    @abstractmethod
    def process(self) -> List:
        """Process the streamlist given the method
        """
        pass

    def get_example(self):
        return json.dumps(self._get_example())

    @abstractmethod
    def _get_example(self) -> Dict:
        """Return example
        """
        return {}


class FilterLLM(FilterResponseMethod):
    TYPE = "llm"
    TEMPLATE = FILTER_TEMPLATE
    URL = os.environ['URL_LLM']

    def load_filtertemplate(self, templatename):
        """Loads query filter templates from compose_conf json input.

        Args:
            compose_config (dict): Dictionary with compose configuration and params

        Returns:
            Parsed JSON data of the loaded template.
        """
        try:
            template = load_file(storage_containers['workspace'], f"{S3_QUERYFILTERSPATH}/{templatename}.json").decode()
            if not template:
                raise self.raise_PrintableGenaiError(404, f"S3 config file doesn't exists for name {templatename} in {S3_QUERYFILTERSPATH} S3 path")
        except ValueError as exc:
            raise self.raise_PrintableGenaiError(404, f"S3 config file doesn't exists for name {templatename} in {S3_QUERYFILTERSPATH} S3 path") from exc
        return json.loads(template)

    def process(self, params):
        """Process the streamlist given the method. This method filters the streamlist given the context
            of the text. It uses the LLMApi service to make the call and check if the text is related with
            the context.  If the output of the call is Yes, the streamlist is added to next phase.
        """
        template = self.TEMPLATE
        if len(self.streamlist) == 0  or self.streamlist is None:
            raise PrintableGenaiError(status_code=400, message="Streamlist is empty, cannot filter response")

        headers = params.pop("headers")
        templatename = params.get("template")
        query = params.pop("query")
        
        self.filter_template = self.load_filtertemplate(templatename)

        substitutions_template = self.filter_template.get("substitutions_template")
        substitutions = self.filter_template.get("substitutions")

        answer = self.streamlist[-1].answer
        
        context = " ".join([streamchunk.content for streamchunk in self.streamlist])
        filter_query_response = f"Query:{query}. Response:{answer}. Context:{context}"
        template['query_metadata']['query'] = substitutions_template + "\n" + filter_query_response
        r = requests.post(self.URL, json=template, headers=headers, verify=True)
        if r.status_code != 200:
            raise PrintableGenaiError(status_code=r.status_code, message=r.text)

        result = LLMP.parse_response(r)
        answer = result['answer']

        for substitution in substitutions:
            if substitution['from'] in answer:
                to = substitution['to']
                if not to:
                    break
                if "extra_words" in substitution:
                    extra_words = substitution['extra_words']
                    random.shuffle(extra_words)
                    to += " " + ", ".join(extra_words[0: substitution.get("randpick", -1)])
        
                self.response = to
                break

        if self.response is None:
            return self.streamlist

        template['query_metadata']['query'] = self.response
        r = requests.post(self.URL, json=template, headers=headers, verify=True)
        result = LLMP.parse_response(r)
        self.streamlist[-1].answer = result['answer']

        return self.streamlist 


    def get_example(self):
        return json.dumps(self._get_example())

    def _get_example(self) -> Dict:
        """Example to generate compose dict
        """
        return {
            'type': self.TYPE,
            'params': self.TEMPLATE
        }


class FilterResponseFactory:
    FILTERS = [FilterLLM]

    def __init__(self, filter_type: str) -> None:
        """Select the given filter

        Args:
            filter_type (str): one of the available filters
        """

        self.filtermethod: FilterResponseMethod = None
        for filtermethod in self.FILTERS:
            if filtermethod.TYPE == filter_type:
                self.filtermethod = filtermethod
                break

        if self.filtermethod is None:
            raise PrintableGenaiError(status_code=404,
                                      message=f"Provided filter does not match any of the possible ones: {', '.join(f.TYPE for f in self.FILTERS)}")

    def process(self, streamlist: list, params: dict):
        """Process the streamlist with the given method
        """
        filtered_streamlist = self.filtermethod(streamlist).process(params)
        if not filtered_streamlist:
            raise PrintableGenaiError(status_code=404, message="Error after filtering. NO documents passed the filters")
        else:
            return filtered_streamlist
