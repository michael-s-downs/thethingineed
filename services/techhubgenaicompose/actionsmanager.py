### This code is property of the GGAO ###


from basemanager import AbstractManager
import json
import re
import copy
from string import Template

class ActionsManager(AbstractManager):
    def __init__(self, compose_confs, params):
        """Class to manage the actions set in the json input

        Args:
            compose_confs (dict): Dictionary with compose configuration and params
            params (dict): Dictionary with actions params
        """
        self.compose_confs = compose_confs
        self.params = params
        self.actions_confs = None
        self.query_actions_confs = []
        self.QUERY_ACTIONS = ["expansion", "filter_query", "reformulate_query"]

    def parse_input(self, clear_quotes):
        """Parses params in the configuration for each action, spliting retrieve from the other actions.

        """
        self.logger.debug("Actions parse INIT")

        self.actions_confs = []
        if len(self.params.get('retrieve', [])) > 0:
            for action in self.compose_confs:
                if action.get('action') == 'retrieve' and 'retrieve' not in [a.get('action') for a in
                                                                             self.actions_confs]:
                    for retrieve_params in self.params.get('retrieve', []):
                        retrieve_params = self.default_template_params(retrieve_params)
                        retrieve_dict = self.safe_substitute(action, retrieve_params, clear_quotes)
                        self.actions_confs.append(retrieve_dict)
                else:
                    actions_params = copy.deepcopy(self.params)
                    actions_params.pop('retrieve')
                    action_dict = self.safe_substitute(action, actions_params, clear_quotes)
                    self.actions_confs.append(action_dict)
        else:
            # Retrocompatibility mode
            self.actions_confs = [self.safe_substitute(action, self.params, clear_quotes) for action in
                                  self.compose_confs]

        self.check_llm_action_params()

        self.logger.debug(f"Actions parsed: {self.actions_confs}")
        if sum(1 for a in self.actions_confs if a.get('action') == 'retrieve') < 1:
            default_retrieve_action = {
                "action": "retrieve",
                "action_params": {
                    "type": "streamlist",
                    "params": {
                        "streamlist": [
                            {
                                "content": "",
                                "meta": {
                                    "field1": ""
                                }
                            }
                        ],
                        "headers_config": {}
                    }
                }
            }
            self.actions_confs.insert(0, default_retrieve_action)

        self.logger.debug("Actions parse END")

    def default_template_params(self, params):
        default_param = {
            "filters": {},
            "top_k": 5,
        }
        for default_key, default_param in default_param.items():
            if default_key not in params:
                params[default_key] = default_param
        return params

    def preprocess_query(self, template_dict, template_params):
        self.logger.debug("[Process ] Retrieve action found")
        if "search_topic" in template_params:
            self.logger.debug("[Process ] Search topic appears in template_params")

            template_dict['action_params']['params']['indexation_conf']['query'] = template_params['search_topic']

            self.logger.debug(f"Busqueda para retrieval: {template_params['search_topic']}")
        query = template_dict['action_params']['params']['indexation_conf']['query']
        if 'based on' in query:
            self.logger.debug("[Process ] \"based on\" appears in the query")
            search_topic = query.split('based on')[1].strip()
            search_topic = search_topic.replace('"', '').replace("'", "").replace('.', '')
            template_dict['action_params']['params']['indexation_conf']['query'] = search_topic

        self.logger.info("Busqueda para retrieval: %s",
                         template_dict['action_params']['params']['indexation_conf']['query'])
        self.logger.info("Top_k para retrieval: %s",
                         template_dict['action_params']['params']['indexation_conf']['top_k'])

        self.logger.info("[Process ] Template ready for retrieval")
        return template_dict


    def check_llm_action_params(self):
        for action in self.actions_confs:
            if action['action'] == "llm_action":
                action_params = action['action_params']
                if action_params.get("params") is not None:
                    template = action_params['params'].get('query_metadata').get('template')
                    if template:
                        try:
                            template_dict = eval(template)
                        except SyntaxError:
                            self.raise_PrintableGenaiError(400,
                                                            "Template is not well formed, must be a dict {} structure")
                        if "$query" not in template_dict.get("user"):
                            self.raise_PrintableGenaiError(400, "Template must contain $query to be replaced")


    def safe_substitute(self, template, template_params, clear_quotes):
        """Replaces the placeholders with its param value

        Args:
            template (dict): Template with the params to replace
            template_params (dict): Params to replace in the template
            clear_quotes (bool)

        Returns:
            template_dict (dict): Template with placeholders replaced
        """
        template_params = self.assert_json_serializable(template_params, clear_quotes)
        template_str = json.dumps(template)
        template_str = re.sub(r'"\$([^"]+)"', r'$\1', template_str)
        t = Template(template_str)

        for param, value in template_params.items():
            if param == "filters":
                continue
            if param == "query":
                if value is None:
                    template_params[param] = '""'
                else:
                    template_params[param] = f'"{value}"'
                continue
            if isinstance(value, bool):
                template_params[param] = str(value).lower()
                continue
            if isinstance(value, dict):
                if value:
                    value = str(value)
                    template_params[param] = f'"{value}"'
                continue

            value = str(value)
            if value.startswith("{") and value.endswith("}") and len(value) > 2:
                template_params[param] = f'"{value}"'
            if value.startswith("{") == False and value.endswith("}") == False and value.isdigit() == False:
                template_params[param] = f'"{value}"'
            if value.startswith("[") and value.endswith("]"):
                template_params[param] = value

        template_substituted = t.safe_substitute(**template_params)
        if "'" in template_substituted:
            self.logger.warning(
                "Template contains ' character. Single quotes are not allowed in the template. Please use double quotes")
        if "$" in template_substituted:
            self.logger.warning("$ Params not substituted")

        try:
            template_dict = json.loads(template_substituted)

        except json.decoder.JSONDecodeError as ex:
            error_param = []
            idx = int(str(ex).split("char ")[1].replace(")", ""))
            for i in range(idx, len(template_substituted)):
                if template_substituted[i] == "," or template_substituted[i] == "}" or template_substituted[i] == "]" or \
                        template_substituted[i] == "\\" or template_substituted[i] == " ":
                    break
                error_param.append(template_substituted[i])
            error_param = "".join(error_param)
            raise self.raise_PrintableGenaiError(400,
                                                 f"After substitution template is not json serializable please check near param: <{error_param}>. Template: {template_substituted}")

        return template_dict


    def assert_json_serializable(self, params, clear_quotes):
        """ Params are substituted into the template and then must be loaded as a json. 
        This function asserts that after substitution the template is json serializable.

        Args: 
            params (dict): Dictionary to be asserted

        Returns:
            dict: params
        """
        try:
            if clear_quotes:
                params = {k: re.sub(r"['\"]", "", v) if isinstance(v, (str)) else v for k, v in
                          params.items()}  # Assert final json is json serializable

            params = {k: json.dumps(v)[1:-1] if isinstance(v, (str)) else v for k, v in
                      params.items()}  # Assert final json is json serializable
            params = {k: json.dumps(v) if isinstance(v, (dict, list)) else v for k, v in params.items()}
        except Exception as _:
            raise self.raise_PrintableGenaiError(400,
                                                 f"Params field must be a dictionary with json serializable values. Please check params field. Params: {params}")

        return params
    
    def get_and_drop_query_actions(self):
        actions_temp = []
        for action_conf in self.actions_confs:
            if action_conf["action"] in self.QUERY_ACTIONS:
                self.query_actions_confs.append(action_conf)
            else:
                actions_temp.append(action_conf)
        
        self.actions_confs = actions_temp

        