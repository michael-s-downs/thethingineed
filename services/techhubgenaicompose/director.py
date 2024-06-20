### This code is property of the GGAO ###


from copy import deepcopy
import json
import re
from string import Template
from basemanager import AbstractManager
from compose.streambatch import StreamBatch
from pcutils.persist import PersistDict
from confmanager import ConfManager
from actionsmanager import ActionsManager
from outputmanager import OutputManager
from common.utils import get_error_word_from_exception


class Director(AbstractManager):

    def __init__(self, compose_conf, apigw_params) -> None:
        """
            Inicializate all objects that can be accessed by the Director
        """
        self.sb = StreamBatch()
        self.apigw_params = apigw_params
        self.compose_conf = compose_conf
        self.conf_manager : ConfManager = None
        self.actions_manager : ActionsManager = None
        self.output_manager : OutputManager = None
        self.PD = PersistDict()
        self.logger.debug("Director created")


    def get_output(self):
        """Gets the output from the API call
        Returns:
            dict: output
        """
        output = {
            'session_id': self.conf_manager.session_id,
            'streambatch': self.sb.to_list_serializable(),
        }
        self.output_manager.get_answer(output, self.sb)
        self.output_manager.get_scores(output, self.sb)
        self.output_manager.get_lang(output, self.conf_manager.lang)
        self.output_manager.get_n_conversation(output, self.PD.get_conversation(self.conf_manager.session_id))
        self.output_manager.get_n_retrieval(output, self.sb)
        return output


    def run(self):
        """Runs the director process, with all classes 
        Returns:
            dict: output
        """
        self.conf_manager = ConfManager(self.compose_conf, self.apigw_params)
        if self.conf_manager.persist_m:
            self.PD.get_from_redis(self.conf_manager.session_id, self.conf_manager.headers['x-tenant'])
        compose_confs = self.run_conf_manager_actions()

        self.actions_manager = ActionsManager(compose_confs, self.conf_manager.template_m.params)
        self.actions_manager.parse_input(self.conf_manager.clear_quotes)
        self.run_actions()
        self.filter_result()
        if self.conf_manager.persist_m:
            self.PD.save_to_redis(self.conf_manager.session_id, self.conf_manager.headers['x-tenant'])
        self.output_manager = OutputManager(self.compose_conf)
        return self.get_output()

    
    def filter_result(self):
        filtered = False
        if self.conf_manager.filter_m is None:
            return
        if self.conf_manager.filter_m.resultfilters:
            for sl in self.sb:
                for sc in sl:
                    if sc.answer:
                        filter_query_response = f"Query:{self.conf_manager.template_m.query}. Response:{sc.answer}. Context:{sl.join_get_content()}"
                        sc.answer, filtered = self.conf_manager.filter_m.run(filter_query_response, self.conf_manager.headers, output=True)


    def run_actions(self):
        """Runs the actions process, depending if they have been passed by API call
                IF action is summarize, uses different input parameters
        """
        for actions_conf in self.actions_manager.actions_confs:
            self.logger.info(f"Action: {actions_conf}")

            action = actions_conf['action']
            action_params = actions_conf['action_params']
            function_map = {
                "retrieve": self.sb.retrieve,
                "filter": self.sb.filter,
                "merge": self.sb.merge,
                "rescore": self.sb.rescore,
                "llm_action": self.sb.llm_action,
                "batchmerge": self.sb.batchmerge,
                "batchcombine": self.sb.batchcombine,
                "batchsplit": self.sb.batchsplit,
                "sort": self.sb.sort,
                "batchsort": self.sb.batchsort,
                "groupby": self.sb.groupby
            }

            action_function = function_map.get(action)

            if not action_function:
                raise self.raise_Dolffiaerror(404, "Action not found, choose one between \"filter\", \"merge\", \"rescore\", \"summarize\", \"sort\",\"batchmerge\", \"batchcombine\" & \"batchsplit\"")
            ap = action_params.get('params', {})

            if action_function == self.sb.llm_action:
                ap["PD"] = self.PD
                ap["top_qa"] = self.conf_manager.template_m.top_qa
                ap["query_type"] = self.conf_manager.template_m.query_type
                ap["llm_action"] = self.conf_manager.template_m.llm_action

            action_function(action_params['type'], ap if ap else {})
            self.logger.info(f"Action: {action} executed")

    def run_conf_manager_actions(self):
        """Runs the configurations process, depending if they´ve been passed by API call
        """
        if "compose_flow" in self.compose_conf or "template" in self.compose_conf:
            template = self.get_compose_flow()
        else:
            raise self.raise_Dolffiaerror(404, "Compose config must have whether compose_conf or template arguments")

        # Set lang
        self.logger.debug(f"Setting lang: {self.conf_manager.lang} in the template")
        for t in template:
            if t['action'] == 'summarize' or t['action'] == 'llm_action':
                t['action_params']['params']['query_metadata']['lang'] = self.conf_manager.lang
        
        # Add headers to the template
        self.logger.debug("Adding headers in the template")
        if self.apigw_params:
            for t in template:
                if t['action'] in ['retrieve', 'llm_action', 'filter', 'summarize']:
                    t['action_params']['params']['headers_config'] = self.conf_manager.headers
                elif t['action'] in ['rescore']:
                    if t['action_params']['type'] in ['dolffia_rescorer']:
                        t['action_params']['params']['headers_config'] = self.conf_manager.headers

        return template
    
    def fix_merge(self, s):
        # Replace $ with "$" but avoid affecting the template parameter
        result = ""
        in_template = False
        in_merge = False
        for line in s.split('\n'):
            if '"template":' in line:
                in_template = True
            if "merge" in line:
                in_merge = True
            if in_template and in_merge:
                line = re.sub(r'"\$(\w+)"', r'$\1', line)
                in_template = False
                in_merge = False
            result += line + '\n'
        return result

    def get_compose_flow(self):
        """
        Executes the flow of the conf_manager (filter, reformulate, persist), 
            replacing the query and updating the template dict

            Returns:
               dict: template 
        """
        template_params = self.conf_manager.template_m.params
        
        if self.conf_manager.template_m.template is None:
            self.conf_manager.template_m.load_template()
        template = re.sub(r'"\$([^"]+)"', r'$\1', self.conf_manager.template_m.template)
        template = re.sub(r'\$([A-Za-z0-9_]+)', r'"$\1"', template)
        template = self.fix_merge(template)
        try:
            template = json.loads(template)
        except json.decoder.JSONDecodeError as ex:
            error_param = get_error_word_from_exception(ex, template)
            raise self.raise_Dolffiaerror(500, f"Template is not json serializable please check near param: <{error_param}>. Template: {template}")
        except Exception as ex:
            raise self.raise_Dolffiaerror(500, ex)

        filtered = False
        reformulated = False
        if self.conf_manager.filter_m and self.conf_manager.filter_m.queryfilters:
            self.conf_manager.template_m.query, filtered = self.conf_manager.filter_m.run(self.conf_manager.template_m.query, self.conf_manager.headers )
            if ("search_topic" in template_params) and (len(template_params['search_topic'])==0):
                template_params['search_topic'] = self.conf_manager.template_m.query
        
        if not filtered and self.conf_manager.reformulate_m:
            self.conf_manager.template_m.query, reformulated = self.conf_manager.reformulate_m.run(self.conf_manager.template_m.query, self.conf_manager.session_id, self.conf_manager.headers, self.PD, self.conf_manager.lang)
        
        template_params = self.conf_manager.template_m.set_params(template_params)

        try:
            if self.conf_manager.persist_m:
                template = self.conf_manager.persist_m.run(template, self.conf_manager.session_id, self.PD, reformulated)

            return template

        except json.decoder.JSONDecodeError as ex:
            error_param = get_error_word_from_exception(ex, template)
            raise self.raise_Dolffiaerror(500, f"After substitution template is not json serializable please check near param: <{error_param}>. Template: {template}")
        except Exception as ex:
            raise self.raise_Dolffiaerror(500, ex)