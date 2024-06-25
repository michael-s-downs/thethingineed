### This code is property of the GGAO ###


import random
import string
from datetime import datetime
from copy import deepcopy

from basemanager import AbstractManager
from pcutils.filters import FilterManager
from pcutils.reformulate import ReformulateManager
from pcutils.persist import PersistManager
from pcutils.template import TemplateManager
from langfusemanager import LangFuseManager
from common.preprocess.preprocess_utils import get_language


class ConfManager(AbstractManager):

    def __init__(self, compose_config, apigw_params) -> None:
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
        self.session_id = self.parse_session(compose_config)
        self.clear_quotes = False
        self.template_m = None
        self.persist_m = None
        self.filter_m = None
        self.lang = None
        self.reformulate_m = None
        self.langfuse_m = None
        self.parse_conf_actions(compose_config)


    def parse_conf_actions(self, compose_config):
        """Parses params from compose_conf and instanciates each manager

        Args:
            compose_config (dict): Dictionary with compose configuration and params
        """
        self.logger.debug("Compose_conf parse INIT")
        self.headers = compose_config['headers_config'] if "headers_config" in compose_config else deepcopy(self.apigw_params)
        self.clear_quotes = compose_config.get("clear_quotes", self.clear_quotes)
        self.template_m = TemplateManager().parse(compose_config)
        self.filter_m = FilterManager().parse(compose_config)
        self.reformulate_m = ReformulateManager().parse(compose_config)
        self.persist_m = PersistManager().parse(compose_config)
        if self.template_m.query is not None:
            self.lang = self.parse_lang(compose_config, self.template_m.query)
        self.langfuse_m = LangFuseManager().parse(compose_config, self.session_id)
        self.langfuse_m.update_metadata(compose_config)
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

        if session_id is None or not session_id:
            session_id = f"{self.department}/session_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}_{''.join([random.choice(string.ascii_lowercase + string.digits) for i in range(6)])}"
            self.logger.debug(f"Generated session: {session_id}")

        else:
            self.logger.debug(f"Parsed session: {session_id}")

        return session_id


    def parse_lang(self, compose_config, query) -> string:
        """Given compose_conf from input json, detects or retrieves the language.

        Args:
            compose_config (dict): Dictionary with params for compose

        Returns:
            lang (string)
        """
        lang = ""
        if "lang" not in compose_config:
            self.logger.info("No lang provided, detecting default languages")
            if query.startswith("http") or query.startswith("data:image"):
                return ""
            lang, prob = get_language(query, return_acc=True, possible_langs=["es", "en", "ja"])
            self.logger.debug(f"Detected lang: <{lang}>")
            if prob > 0.8 and lang in ["es", "en", "ja"]:
                return lang
            self.logger.debug(f"Detected <{lang}>, not enough prob")
            return ""

        else:
            lang = compose_config['lang']
            if isinstance(lang, str):
                lang = lang.lower()
                self.logger.debug(f"Parsed lang: <{lang}>")

            elif isinstance(lang, list):
                if query.startswith("http") or query.startswith("data:image"):
                    return ""
                return get_language(query, return_acc=False, possible_langs=lang)
                
            else:
                self.logger.error(f"Lang <{lang}> found not valid")
                return ""

        return lang
