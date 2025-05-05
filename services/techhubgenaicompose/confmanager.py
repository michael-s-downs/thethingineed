### This code is property of the GGAO ###


import os
import random
import string
import re
from datetime import datetime
from copy import deepcopy

from basemanager import AbstractManager
from pcutils.persist import PersistManager
from pcutils.template import TemplateManager
from langfusemanager import LangFuseManager
from common.genai_controllers import load_file, storage_containers

from lingua import Language, LanguageDetectorBuilder

IRStorage_TEMPLATEPATH = "src/compose/templates"


class ConfManager(AbstractManager):

    def __init__(self, compose_config, apigw_params, langfuse_m) -> None:
        """Class to manage and read params/actions set in the compose_conf input json.

        Args:
            compose_config (dict): Dictionary with compose configuration and params
            apigw_params (dict): Dictionary with the API Gateway params
            logger (app.log): Refernce to the application log
        """
        self.logger.debug("Created ConfManager with:")
        self.logger.info(f"Config: {compose_config}")
        self.logger.debug(f"APIgw params: {apigw_params}")
        self.apigw_params = apigw_params
        self.department = self.apigw_params.get("x-department", "main")
        self.langfuse_m = langfuse_m.parse(compose_config)
        self.session_id = self.parse_session(compose_config)
        self.langfuse_m.create_trace(self.session_id)
        self.clear_quotes = False
        self.template_m = None
        self.persist_m = None
        self.filter_m = None
        self.lang = None
        self.reformulate_m = None
        self.update_model_from_defaults()
        self.set_detector()
        self.parse_conf_actions(compose_config)

    def parse_conf_actions(self, compose_config):
        """Parses params from compose_conf and instanciates each manager

        Args:
            compose_config (dict): Dictionary with compose configuration and params
        """
        self.logger.debug("Compose_conf parse INIT")
        self.headers = compose_config['headers_config'] if "headers_config" in compose_config else deepcopy(
            self.apigw_params)
        self.clear_quotes = compose_config.get("clear_quotes", self.clear_quotes)
        self.template_m = TemplateManager().parse(compose_config, self.langfuse_m)
        self.persist_m = PersistManager().parse(compose_config)
        self.langfuse_m.update_metadata(compose_config)
        if self.template_m.query is not None:
            self.lang = self.parse_lang(compose_config, self.template_m.query)
        self.logger.debug("Compose_conf parse END")

    def parse_session(self, compose_config) -> string:
        """Given compose_conf from input json, parses the session
        and if not exists generates a new one with date and randoms

        Args:
            compose_config (dict): Dictionary with params for compose

        Returns:
            session_id (string)
        """
        session_id = compose_config.get("session_id")
        if isinstance(session_id, str):
            session_id = session_id.strip()

        model_request = compose_config.get('template').get('params').get('model')
        name = compose_config.get('template').get('name')
        model_template = None
        if not model_request:
            try:
                template = self.langfuse_m.load_template(name)
                template = template.prompt
                #template = load_file(storage_containers['workspace'], f"{IRStorage_TEMPLATEPATH}/{name}.json").decode()
                if not template:
                    self.raise_PrintableGenaiError(404, "Compose template not found")
                pattern_llm_action = r'("action":\s*"summarize"|"action":\s*"llm_action")'
                match = re.search(pattern_llm_action, template)
                if match:
                    template = template[match.end():]
                    pattern_model = r'"model":\s*"([^"]+)"'
                    models_in_template = re.findall(pattern_model, template)
                    model_template = models_in_template[0]
                else:
                    model_template = ""
            except Exception:
                if name:
                    self.raise_PrintableGenaiError(404, f"Template file doesn't exists for name {name}")
                else:
                    self.raise_PrintableGenaiError(400, "Mandatory param <name> not found in template.")

            if isinstance(model_template, str) and len(model_template) > 0:
                if model_template[0] != '$':
                    model = model_template
                else:
                    if isinstance(model_request, str):
                        model = model_request
                    else:
                        self.raise_PrintableGenaiError(400, "There is no model defined in the request or template")
            else:
                model = model_template
        else:
            model = model_request

        model = self.clean_model(model)
        if session_id is None or not session_id:
            session_id = f"{self.department}/session_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}_{''.join([random.choice(string.ascii_lowercase + string.digits) for _ in range(6)])}/{model}"
            self.logger.debug(f"Generated session: {session_id}")
        else:
            session_id = session_id + "/" + model
            self.logger.debug(f"Parsed session: {session_id}")

        return session_id

    def parse_lang(self, compose_config, query) -> string:
        """Given compose_conf from input json, detects or retrieves the language.

        Args:
            compose_config (dict): Dictionary with params for compose

        Returns:
            lang (string)
        """
        query = str.lower(query)
        lang = ""
        if "lang" not in compose_config:
            self.logger.info("No lang provided, detecting default languages")
            if query.startswith("http") or query.startswith("data:image"):
                return ""
            
            lang = self.detector.detect_language_of(query)
            return str.lower(lang.iso_code_639_1.name)

        else:
            lang = compose_config['lang']
            if isinstance(lang, str):
                lang = lang.lower()
                self.logger.debug(f"Parsed lang: <{lang}>")

            else:
                self.logger.error(f"Lang <{lang}> found not valid")
                return ""

        return lang

    def clean_model(self, model: str):
        remove_strings = ["genai-", "-pool", "-america", "-europe", "-world", "-australia", "-japan", "-uk", "-ew",
                          "-NorthVirginiaEast", "-FranckfurtCentral", "-ParisWest", "-AustraliaEast", "AustraliaEast",
                          "-CanadaEast", "CanadaEast", "-EastUs", "EastUs", "-EastUs2", "EastUs2", "Francia",
                          "JapanEast", "-NorthCentralUs", "NorthCentralUS", "-sweden", "-SouthCentralUs", "-france",
                          "-westeurope", "WestUS", "-UKSouth", "UKSouth", "-SwitzerlandNorth", "SwitzerlandNorth"]
        for substring in remove_strings:
            model = model.replace(substring, "")
        return model


    def update_model_from_defaults(self):
        """Updates all default templates with the model set in the environ
        """
        default_model = os.getenv("DEFAULT_LLM_MODEL")
        if default_model is None:
            return
        
        from compose.utils.defaults import SUM_TEMPLATE, FILTER_TEMPLATE, REFORMULATE_TEMPLATE, TRANSLATE_TEMPLATE, FILTERED_ACTIONS, STEP_TEMPLATE

        SUM_TEMPLATE['llm_metadata']['model'] = default_model
        FILTER_TEMPLATE['llm_metadata']['model'] = default_model
        REFORMULATE_TEMPLATE['llm_metadata']['model'] = default_model
        TRANSLATE_TEMPLATE['llm_metadata']['model'] = default_model
        STEP_TEMPLATE['llm_metadata']['model'] = default_model
        FILTERED_ACTIONS[1]['action_params']['params']['llm_metadata']['model'] = default_model
    
    def set_detector(self):
       langs = os.environ.get("DEFAULT_LANGS") 
       if langs and isinstance(langs, list):
            langs_map = {
                "en": Language.ENGLISH,
                "es": Language.SPANISH,
                "ja": Language.JAPANESE,
                "fr": Language.FRENCH,
                "de": Language.GERMAN,
                "it": Language.ITALIAN,
                "pt": Language.PORTUGUESE,
                "ca": Language.CATALAN,
                "ko": Language.KOREAN,
                "zh": Language.CHINESE
            }
            langs = [*map(langs_map.get, langs)]
            self.detector = LanguageDetectorBuilder.from_languages(langs).with_preloaded_language_models().build()

       else:
            self.detector = LanguageDetectorBuilder.from_all_languages().with_preloaded_language_models().build()
        
        

            