### This code is property of the GGAO ###

import os
import requests
from typing import List
from abc import abstractmethod, ABC
from compose.utils.defaults import REFORMULATE_TEMPLATE
from common.errors.genaierrors import PrintableGenaiError
from common.errors.LLM import LLMParser


LLMP = LLMParser()

class ReformulateMethod(ABC):
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


class MixQueries(ReformulateMethod):
    """Implements GPT-based filtering for queries.
    """
    TYPE = "mix_queries"

    URL = os.environ['URL_LLM']
    TEMPLATE = REFORMULATE_TEMPLATE

    def process(self, params = None, actions_confs = None) -> str:
        """Processes the query using a GPT model for substitutions.

        Returns:
            The substituted query and a boolean flag indicating substitution.
        """
        template = self.TEMPLATE
        if self.query == "" or self.query is None:
            raise PrintableGenaiError(status_code=400, message="Query is empty, cannot filter")

        headers = params.pop("headers")
        max_persistence = params.get("max_persistence", 5)
        template_name = params.get("template_name")
        lang = params.get("lang")
        save_mod_query = params.get("save_mod_query", False)
        session_id = params.get("session_id")
        PD = params.get("PD")
        
        session = PD.get_conversation(session_id)
        if session and len(session) > 0 and session[0]:
            session.update_last({"user": self.query})
            
            template['query_metadata']['query'] = "\\n".join([f"{s['user']}" for i, s in enumerate(session.get_n_last(max_persistence)) if s is not None])
            template['query_metadata']['template_name'] = template_name
            template['query_metadata']['lang'] = lang

            r = requests.post(self.URL, json=template, headers=headers, verify=True)
            result = LLMP.parse_response(r)
            query =  result['answer'].replace("\n", " ")
            if save_mod_query is False:
                PD.update_last({"user": query}, session_id)
            else:
                PD.update_last({"user":self.query, "assistant": query}, session_id)
                PD.add({"user": query}, session_id)

            for action in actions_confs:
                if action["action"] == "retrieve":
                    action["action_params"]["params"]["indexation_conf"]["query"] = query
                if action["action"] == "llm_action":
                    action["action_params"]["params"]["query_metadata"]["query"] = query
                

            return query


class ReformulateFactory:
    """
    Factory class for creating query filter methods.
    """

    REFORMULATES = [MixQueries]

    def __init__(self, reformulate_type: str) -> None:
        """
        Select the given query filter method.

        Args:
            reformulate_type (str): Type of the query filter method.
        """
        self.reformulatemethod = None
        for reformulatemethod in self.REFORMULATES:
            if reformulatemethod.TYPE == reformulate_type:
                self.reformulatemethod = reformulatemethod
                break

        if self.reformulatemethod is None:
            raise PrintableGenaiError(status_code=404, message=f"Provided reformulate method does not match any of the possible ones: {', '.join(f.TYPE for f in self.REFORMULATES)}")


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
        return self.reformulatemethod(query).process(params, actions_confs)
