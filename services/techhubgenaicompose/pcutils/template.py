### This code is property of the GGAO ###


import random

from common.genai_controllers import load_file, storage_containers
from basemanager import AbstractManager

S3_TEMPLATEPATH = "src/compose/templates"


class TemplateManager(AbstractManager):
    """
    The TemplateManager class is responsible for managing templates used in the application.

    Attributes:
        params (dict): The parameters of the template.
        filter_template (str): The query filter template.
        name (str): The name of the template.
        template (str): The template itself.
        probs (list): The probabilities associated with each template name.
        top_k (int): The number of top results to retrieve.
        query (str): The query to be retrieved.
        defaults_dict (dict): The default values for the template parameters.

    Methods:
        parse(compose_config): Detects elements to use in the template.
        get_param(params, param_name, param_type, mandatory=False): Retrieves a parameter from the template parameters.
        default_template_params(): Sets default values for the template parameters.
        run(template_dict, template_params): Sets all the parameters to call the retrieve function.
        load_template(): Loads the template stored in S3 that's going to be used.
    """

    def __init__(self):
        self.params = None
        self.filter_template = None
        self.name = None
        self.template = None
        self.probs = None
        self.top_k = 5
        self.query = None
        self.defaults_dict = {
            "template": {},
            "name": None,
            "top_k": 5,
            "top_qa": 3,
            "reformulate": None,
            "queryfilter_template": {},
            "probs": None,
            "query": None,
            "query_type": "",
            "llm_action": []
        }

    def parse(self, compose_config):
        """
        Detects elements to use in the template.

        Args:
            compose_config (dict): The compose configuration.

        Returns:
            dict: The updated template.
        """
        conf = self.get_param(compose_config, "template", dict)
        if not conf:
            self.raise_PrintableGenaiError(404, "Template conf not found, trying compose_flow")

        self.name = conf.get("name")
        if not self.name:
            self.raise_PrintableGenaiError(404, "Mandatory param <name> not found in template")
        self.params = self.get_param(conf, "params", dict)
        self.default_template_params()
        self.top_k = self.get_param(self.params, "top_k", int)
        self.top_qa = self.get_param(self.params, "top_qa", int)
        self.filter_template = self.get_param(compose_config, "queryfilter_template", str)
        self.probs = self.get_param(compose_config, "probs", list)
        self.query = self.get_param(self.params, "query", str)
        self.query_type = self.get_param(self.params, "query_type", str)
        self.llm_action = self.get_param(self.params, "llm_action", list)
        self.logger.info("[Process ] Template parsed")

        if self.query == "":
            self.query = None

        return self

    def get_param(self, params: dict, param_name: str, param_type, mandatory=False):
        """
        Retrieves a parameter from the template parameters.

        Args:
            params (dict): The template parameters.
            param_name (str): The name of the parameter.
            param_type (type): The type of the parameter.
            mandatory (bool, optional): Whether the parameter is mandatory. Defaults to False.

        Returns:
            Any: The value of the parameter.
        """
        return super().get_param(params, param_name, param_type, self.defaults_dict, mandatory=mandatory)

    def default_template_params(self):
        """
        Sets default values for the template parameters.
        """
        default_param = {
            "filters": {},
            "top_k": 5,
        }
        for default_key, default_param in default_param.items():
            if default_key not in self.params:
                self.params[default_key] = default_param

    def set_params(self, template_params):
        """
        Sets the parameters for the template (query, query_type, top_qa).

        Args:
            template_params (dict): Template params dict.

        Returns:
            dict: The updated template params.
        """
        template_params['top_qa'] = self.top_qa
        template_params['query_type'] = self.query_type
        template_params['query'] = self.query
        return template_params

    def run(self, template_dict, template_params):
        """
        Sets all the parameters to call the retrieve function.

        Args:
            template_dict (dict): Contains all the template.
            template_params (dict): Template Params. Contains the query to be retrieved.

        Returns:
            dict: The updated template.
        """
        for _, step in enumerate(template_dict):
            if step['action'] == 'retrieve':
                self.logger.debug("[Process ] Retrieve action found")
                if "search_topic" in template_params:
                    self.logger.debug("[Process ] Search topic appears in template_params")

                    step['action_params']['params']['indexation_conf']['query'] = template_params['search_topic']
                    if 'top_k' not in step['action_params']['params']['indexation_conf']:
                        step['action_params']['params']['indexation_conf']['top_k'] = self.top_k

                    self.logger.debug(f"Busqueda para retrieval: {template_params['search_topic']}")
                query = step['action_params']['params']['indexation_conf']['query']
                if 'based on' in query:
                    self.logger.debug("[Process ] \"based on\" appears in the query")
                    search_topic = query.split('based on')[1].strip()
                    search_topic = search_topic.replace('"', '').replace("'", "").replace('.', '')
                    step['action_params']['params']['indexation_conf']['query'] = search_topic
                elif 'top_k' not in step['action_params']['params']['indexation_conf']:
                    step['action_params']['params']['indexation_conf']['top_k'] = self.top_k

                self.logger.info("Busqueda para retrieval: %s",
                                 step['action_params']['params']['indexation_conf']['query'])
                self.logger.info("Top_k para retrieval: %s",
                                 step['action_params']['params']['indexation_conf'].get('top_k', "Not necesary"))

        self.logger.info("[Process ] Template ready for retrieval")
        return template_dict

    def load_template(self):
        """
        Loads the template stored in cloud that's going to be used.
        """
        name = self.name
        if isinstance(self.name, list):
            try:
                name = random.choices(name, weights=self.probs)[0]
                self.logger.info(f"[Process ] Chosen template: {name}")
            except Exception:
                self.raise_PrintableGenaiError(400,
                                                 "If name field is a list, probs field must be defined as a list of same length. Ex: name: ['a', 'b'], probs: [1, 2]")
        self.logger.debug("Template name is not string so, uploading as string...")
        try:
            self.template = load_file(storage_containers['workspace'], f"{S3_TEMPLATEPATH}/{name}.json").decode()
            if not self.template:
                self.raise_PrintableGenaiError(404, "Compose template not found")
        except ValueError:
            self.raise_PrintableGenaiError(404, f"S3 config file doesn't exists for name {name}")
    
    def index_conf_retrocompatible(self, template):
        for action in template:
            if action["action"] == "retrieve":
                if "generic" in action["action_params"]["params"] and action["action_params"]["type"] != "streamlist":
                    action["action_params"]["params"] = action["action_params"]["params"]["generic"]
                    action["action_params"]["params"]["indexation_conf"] = action["action_params"]["params"]["index_conf"]
                    del action["action_params"]["params"]["index_conf"]
                    break

                if "generic" in action["action_params"]["params"] and action["action_params"]["type"] == "streamlist":
                    action["action_params"]["params"].update(action["action_params"]["params"]["generic"])
                    del action["action_params"]["params"]["generic"]
                    action["action_params"]["params"]["indexation_conf"] = action["action_params"]["params"]["index_conf"]
                    del action["action_params"]["params"]["index_conf"]
                    break
        
        return template
                
