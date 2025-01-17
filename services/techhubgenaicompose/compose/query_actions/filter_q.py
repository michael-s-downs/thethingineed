### This code is property of the GGAO ###

import os
import json
import requests
import random
from typing import List
from abc import abstractmethod, ABC
from ..utils.defaults import FILTER_TEMPLATE, FILTERED_ACTIONS
from common.errors.genaierrors import PrintableGenaiError
from common.errors.LLM import LLMParser
from common.genai_controllers import load_file, storage_containers

LLMP = LLMParser()
S3_QUERYFILTERSPATH = "src/compose/filter_templates"

class FilterMethod(ABC):
    """Abstract base class for defining query filters.

    Attributes:
        query (string): The query string to be filtered.
        substitutions (list): List of substitution pairs to be applied to the query.
        substitutions_template (string): A template string for substitutions.
        headers (dict): HTTP headers for requests, if applicable.
    """

    def __init__(self, query) -> None:
        """Initializes the filter with query and substitution details.

        Args:
            query (string): The query string to be filtered.
            substitutions (list): List of substitution pairs to be applied to the query.
            substitutions_template (string): A template string for substitutions.
            headers (dict): HTTP headers for requests, if applicable.
        """
        self.query = query

    @abstractmethod
    def process(self) -> List:
        """Process the streamlist given the method
        """

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
                raise PrintableGenaiError(404, f"S3 config file doesn't exists for name {templatename} in {S3_QUERYFILTERSPATH} S3 path")
        except ValueError as exc:
            raise PrintableGenaiError(404, f"S3 config file doesn't exists for name {templatename} in {S3_QUERYFILTERSPATH} S3 path") from exc
        return json.loads(template)


class FilterExactMatch(FilterMethod):
    """Implements exact match filtering for queries.
    """
    TYPE = "exact_match"

    def process(self, params = None, actions_confs = None) -> str:
        """Processes the query for exact match substitutions.

        Returns:
            The substituted query and a boolean flag indicating substitution.
        """
        templatename = params.get("template")
        self.filter_template = self.load_filtertemplate(templatename)
        substitutions = self.filter_template.get("substitutions")
        headers = params.pop("headers")
        filtered = False
        for substitution in substitutions:
            if "from" not in substitution or "to" not in substitution:
                raise PrintableGenaiError(status_code=400,
                                          message=f"Substitutions must have a from and to key. Keys: {substitution.keys()}")
        for substitution in substitutions:
            if isinstance(substitution['from'], list):
                for f in substitution['from']:
                    if f in self.query:
                        filtered = True
                        self.query = substitution['to']
            elif isinstance(substitution['from'], str):
                if substitution['from'] in self.query:
                        filtered = True
                        self.query = substitution['to']
                
        if filtered:
            actions_confs.clear()
            actions_confs.extend(FILTERED_ACTIONS)
            actions_confs[-1]["action_params"]["params"]["query_metadata"]["query"] = self.query
            actions_confs[-1]["action_params"]["params"]["headers_config"] = headers


class FilterGPT(FilterMethod):
    """Implements GPT-based filtering for queries.
    """
    TYPE = "llm"

    URL = os.environ['URL_LLM']
    TEMPLATE = FILTER_TEMPLATE

    def process(self, params = None, actions_confs = None) -> str:
        """Processes the query using a GPT model for substitutions.

        Returns:
            The substituted query and a boolean flag indicating substitution.
        """
        template = self.TEMPLATE
        if self.query == "" or self.query is None:
            raise PrintableGenaiError(status_code=400, message="Query is empty, cannot filter")

        headers = params.pop("headers")
        templatename = params.get("template")
        
        self.filter_template = self.load_filtertemplate(templatename)

        substitutions_template = self.filter_template.get("substitutions_template")
        substitutions = self.filter_template.get("substitutions")
        
        template['query_metadata']['query'] = substitutions_template + "\n" + self.query
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
        
                self.query = to
                actions_confs.clear()
                actions_confs.extend(FILTERED_ACTIONS)
                actions_confs[-1]["action_params"]["params"]["query_metadata"]["query"] = self.query
                actions_confs[-1]["action_params"]["params"]["headers_config"] = headers
                return result


class FilterFactory:
    """
    Factory class for creating query filter methods.
    """

    FILTERS = [FilterGPT, FilterExactMatch]

    def __init__(self, filter_type: str) -> None:
        """
        Select the given query filter method.

        Args:
            filter_type (str): Type of the query filter method.
        """
        self.filtermethod = None
        for filtermethod in self.FILTERS:
            if filtermethod.TYPE == filter_type:
                self.filtermethod = filtermethod
                break

        if self.filtermethod is None:
            raise PrintableGenaiError(status_code=404, message=f"Provided query filter method does not match any of the possible ones: {', '.join(f.TYPE for f in self.FILTERS)}")


    def process(self, query, params, actions_confs):
        """
        Process the streamlist with the given parameters.

        Args:
            query (string)
            streamlist (list): List of chunks.
            params (dict): Actions parameters.

        Returns:
            query processed
        """
        return self.filtermethod(query).process(params, actions_confs)
