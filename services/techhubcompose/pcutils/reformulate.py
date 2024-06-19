### This code is property of the GGAO ###


import os
import requests

from compose.utils.defaults import REFORMULATE_TEMPLATE
from common.errors.LLM import LLMParser
from basemanager import AbstractManager

LLMP = LLMParser()

class ReformulateManager(AbstractManager):
    """A manager class for query reformulation."""

    def __init__(self):
        """Initialize the ReformulateManager.

        Args:
            compose_config (dict): The compose configuration.
            apigw_params (dict): The API gateway parameters.
            session_id (str): The session ID.
            logger (Logger): The logger instance.
        """
        self.type = None
        self.params = None
        self.defaults_dict = {
            "max_persistence": 5,
            "save_mod_query": True,
            "template_name": "reformulate",
            "reformulate": None,
            "type": "mixqueries"
        }

    def parse(self, compose_config):
        """Detect elements to use in the query reformulation.

        Args:
            compose_config (dict): The compose configuration.

        Returns:
            ReformulateManager: The ReformulateManager instance.
        """
        self.logger.debug("Parse Reformulate INIT")
        conf = self.get_param(compose_config, "reformulate", dict)
        if conf is None:
            self.logger.debug("Reformulate is not in conf")
            return None

        self.type = self.get_param(conf, "type", str)
        self.params = self.get_param(conf, "params", dict)
        self.logger.debug("Parse Reformulate END")

        return self

    def get_param(self, params: dict, param_name: str, param_type):
        """Get a parameter from the given dictionary.

        Args:
            params (dict): The dictionary to get the parameter from.
            param_name (str): The name of the parameter.
            param_type (type): The type of the parameter.

        Returns:
            Any: The value of the parameter.
        """
        return super().get_param(params, param_name, param_type, self.defaults_dict)

    def run(self, query, session_id, headers, PD, lang):
        """Reformulate the query using persistence of previous queries and GPT3.

        Args:
            query (str): The query to be reformulated.
            session_id (str): The session ID.
            headers (dict): The headers of the call.
            PD (dict): The record of the conversations.
            lang (str): The language to use in the reformulation.

        Returns:
            tuple: A tuple containing the reformulated query and a boolean indicating if the query was reformulated.
        """
        reformulated = False
        if self.type != "mixqueries":
            self.logger.info(f"Reformulate type: {self.type} not valid, not reformulating")
            return query

        max_persistence = self.get_param(self.params, "max_persistence", int)
        template_name = self.get_param(self.params, "template_name", str)
        save_mod_query = self.get_param(self.params, "save_mod_query", bool)

        session = PD.get_conversation(session_id)
        if session and len(session) > 0:
            self.logger.debug("[Process ] Reformulating...")

            session.add({"user": query})
            query = GPTReformulate(session.get_n_last(max_persistence), lang, template_name, headers).process()
            reformulated = True
            if save_mod_query:
                self.logger.debug(f'[Process ] Saving Saving only reformulated')
                PD.update_last({"user": query}, session_id)
            else:
                self.logger.debug("[Process ] Saving reformulated and original query")
                PD.add({"user": query}, session_id)
            self.logger.info(f"Reformulated query: {query}")

        return query, reformulated

class GPTReformulate:
    TYPE = "GPT"

    URL = os.environ['URL_LLM']
    TEMPLATE = REFORMULATE_TEMPLATE

    def __init__(self, session, lang, template_name="reformulate", headers: dict = {}):
        """Initialize the GPTReformulate.

        Args:
            session (list): The session containing previous queries.
            lang (str): The language to use in the reformulation.
            template_name (str, optional): The name of the template. Defaults to "reformulate".
            headers (dict, optional): The headers for the request. Defaults to {}.
        """
        self.session = session
        self.template_name = template_name
        self.headers = headers
        self.lang = lang

    def process(self):
        """Reformulate the query with GPT, using the template and the provided language.

        Returns:
            str: The reformulated query.
        """
        template = self.TEMPLATE
        template['query_metadata']['query'] = "\\n".join([f"{s['user']}" for i, s in enumerate(self.session) if s is not None])
        template['query_metadata']['template_name'] = self.template_name
        template['query_metadata']['lang'] = self.lang

        r = requests.post(self.URL, json=template, headers=self.headers, verify=False)
        result = LLMP.parse_response(r)
        return result['answer'].replace("\n", " ")

