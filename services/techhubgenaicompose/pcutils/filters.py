### This code is property of the GGAO ###


import os
import random
import json
import requests
from typing import List, Tuple
from abc import ABC, abstractmethod

from compose.utils.defaults import FILTER_TEMPLATE
from common.errors.LLM import LLMParser
from common.errors.dolffiaerrors import DolffiaError
from common.genai_sdk_controllers import load_file, storage_containers
from basemanager import AbstractManager

S3_QUERYFILTERSPATH = "src/compose/queryfilters_templates"
LLMP = LLMParser()

class FilterManager(AbstractManager):
    """Manages and applies query filters based on provided configurations.

    Attributes:
        logger: Logger instance for logging activities.
        queryfilters: Stores the query filter configurations.
    """

    def __init__(self):
        """Initializes the FilterManager with a logger.

        Args:
            logger: Logger instance for logging activities.
        """
        self.queryfilters = None
        self.resultfilters = None
        self.defaults_dict = {
            "queryfilters": None,
            "resultfilters": None,
            "substitutions": [],
            "substitutions_template ": ""
        }

    def parse(self, compose_config):
        """Parses the configuration to set up query filters.

        Args:
            compose_config (dict): Configuration containing the template details.

        Returns:
            self, if successful; otherwise, None.
        """
        self.logger.debug("QueryFilters parse INIT")
        if "queryfilter_template" in compose_config:
            templatename = self.get_param(compose_config, "queryfilter_template", str)
            self.queryfilters = self.load_filtertemplate(templatename)
            self.logger.debug(f"Parsed queryfilters: {self.queryfilters}")

        if "resultfilter_template" in compose_config:
            templatename = self.get_param(compose_config, "resultfilter_template", str)
            self.resultfilters = self.load_filtertemplate(templatename)
            self.logger.debug(f"Parsed reulstfilters: {self.resultfilters}")

        self.logger.debug("QueryFilters parse END")
        return self

    def get_param(self, params:dict, param_name: str, param_type):
        """Gets the specified param from the dictionary, checks the type and sets 
        the default value if necessary.

        Args:
            params (dict): Dictionary with the params to load.
            param_name (str): Name of the param to load.
            param_type (_type_): Type of the param.

        Returns:
            param (param_type)
        """
        return super().get_param(params, param_name, param_type, self.defaults_dict)

    def load_filtertemplate(self, templatename):
        """Loads query filter templates from compose_conf json input.

        Args:
            compose_config (dict): Dictionary with compose configuration and params

        Returns:
            Parsed JSON data of the loaded template.
        """
        self.logger.debug("Queryfilter template name found")
        try:
            template = load_file(storage_containers['workspace'], f"{S3_QUERYFILTERSPATH}/{templatename}.json").decode()
            self.logger.info("Queryfilter template loaded")
            self.logger.info(f"Template {template}")
            if not template:
                raise self.raise_Dolffiaerror(404, f"S3 config file doesn't exists for name {templatename} in {S3_QUERYFILTERSPATH} S3 path")
        except ValueError as exc:
            raise self.raise_Dolffiaerror(404, f"S3 config file doesn't exists for name {templatename} in {S3_QUERYFILTERSPATH} S3 path") from exc
        return json.loads(template)

    def run(self, query, headers, output=False) -> dict:
        """Processes and applies query filters based on the configuration loaded.

        Args:
            template_params (dict): Parameters containing the query to be filtered.
            headers (dict): HTTP headers for any requests made during filtering.

        Returns:
            A tuple of filtered parameters and a boolean indicating if filtering was applied.
        """
        filtered = False
        query_old = query

        # Template loading
        self.logger.info("Queryfilters run INIT")
        if output == False:
            substitutions = self.get_param(self.queryfilters, "substitutions", list)
            substitutions_template = self.get_param(self.queryfilters, "substitutions_template", str)
        else:
            substitutions = self.get_param(self.resultfilters, "substitutions", list)
            substitutions_template = self.get_param(self.resultfilters, "substitutions_template", str)
            self.queryfilters = self.resultfilters

        self.logger.debug("Substitutions and template loaded")
        self.logger.debug(substitutions)
        self.logger.debug(substitutions_template)

        for ft in self.queryfilters['filter_types']:  #Iterate every filter type
            if ft == FilterExactMatch.TYPE: # Exact match filter
                query, aux_bool = FilterExactMatch(query, substitutions).process()
            elif ft == FilterGPT.TYPE: # GPT filter
                query, aux_bool = FilterGPT(query, substitutions, substitutions_template, headers).process()
            else:
                raise self.raise_Dolffiaerror(404, f"Provided filter does not match any of the possible ones: {', '.join(f.TYPE for f in self.FILTERS)}")
            filtered = filtered or aux_bool

        query = query.strip().lower()
        if query == 'all':
            return query_old

        if output:
            if not filtered:
                query_old = query_old.split("Response:")[1]
                query_old = query_old.split("Context:")[0]
                return query_old, filtered
            URL = os.environ['URL_LLM']
            template = FILTER_TEMPLATE
            template['query_metadata']['query'] = query
            r = requests.post(URL, json=template, headers=headers, verify=False)
            result  = LLMP.parse_response(r)
            query = result['answer']

        self.logger.info("Queryfilters run END")
        return query, filtered

class FilterQuery(ABC):
    """Abstract base class for defining query filters.

    Attributes:
        query (string): The query string to be filtered.
        substitutions (list): List of substitution pairs to be applied to the query.
        substitutions_template (string): A template string for substitutions.
        headers (dict): HTTP headers for requests, if applicable.
    """

    def __init__(self, query, substitutions: List[Tuple[str, str]], substitutions_template = "", headers: dict = {}) -> None:
        """Initializes the filter with query and substitution details.

        Args:
            query (string): The query string to be filtered.
            substitutions (list): List of substitution pairs to be applied to the query.
            substitutions_template (string): A template string for substitutions.
            headers (dict): HTTP headers for requests, if applicable.
        """
        self.query = query
        self.substitutions = substitutions
        self.substitutions_template = substitutions_template
        self.headers = headers

    @abstractmethod
    def process(self) -> List:
        """Process the streamlist given the method
        """

class FilterExactMatch(FilterQuery):
    """Implements exact match filtering for queries.
    """
    TYPE = "exact_match"

    def process(self) -> str:
        """Processes the query for exact match substitutions.

        Returns:
            The substituted query and a boolean flag indicating substitution.
        """
        for substitution in self.substitutions:
            if not "from" in substitution or not "to" in substitution:
                raise DolffiaError(status_code=400, message=f"Substitutions must have a from and to key. Keys: {substitution.keys()}")
        for substitution in self.substitutions:
            if isinstance(substitution['from'], list):
                for f in substitution['from']:
                    if f in self.query:
                        return substitution['to'], True
            elif isinstance(substitution['from'], str):
                if substitution['from'] in self.query:
                    return substitution['to'], True

        return self.query, False

class FilterGPT(FilterQuery):
    """Implements GPT-based filtering for queries.
    """
    TYPE = "GPT"

    URL = os.environ['URL_LLM']
    TEMPLATE = FILTER_TEMPLATE
    TEXT_KEY = "content"

    def process(self) -> str:
        """Processes the query using a GPT model for substitutions.

        Returns:
            The substituted query and a boolean flag indicating substitution.
        """
        template = self.TEMPLATE
        if self.query == "" or self.query is None:
            raise DolffiaError(status_code=400, message="Query is empty, cannot filter")
        template['query_metadata']['query'] = self.substitutions_template + "\n" + self.query
        r = requests.post(self.URL, json=template, headers=self.headers, verify=False)
        if r.status_code != 200:
            raise DolffiaError(status_code=r.status_code, message=r.text)

        result  = LLMP.parse_response(r)
        answer = result['answer']

        for substitution in self.substitutions:
            if substitution['from'] in answer:
                to = substitution['to']
                if not to:
                    break
                if "extra_words" in substitution:
                    extra_words = substitution['extra_words']
                    random.shuffle(extra_words)
                    to += " " +  ", ".join(extra_words[0: substitution.get("randpick", -1)])

                return to, True

        return self.query, False
