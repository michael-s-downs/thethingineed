### This code is property of the GGAO ###


import os
from abc import abstractmethod, ABC
from typing import List
from common.errors.genaierrors import PrintableGenaiError
from common.errors.LLM import LLMParser
from ..utils.defaults import TRANSLATE_TEMPLATE
import asyncio
import aiohttp
from copy import deepcopy

LLMP = LLMParser()

class ExpansionMethod(ABC):
    """
    Abstract base class for query expansion methods.
    """

    TYPE: str = ""

    def __init__(self, query) -> None:
        """
        Instantiate ExpansionMethod.

        Args:
            query (str): Query to be expanded.
        """
        self.query = query

    @abstractmethod
    def process(self) -> List:
        """
        Process the query given the expansion method.

        """


class LangExpansion(ExpansionMethod):
    """
    Language based expansion method for the query
    """

    URL = os.environ['URL_LLM']
    HEADERS = {'Content-type': 'application/json'}
    TEMPLATE = TRANSLATE_TEMPLATE
    TYPE = "lang"
    LANG_MAP = {
        "ja": "japanese",
        "es": "spanish",
        "en": "english",
        "fr": "french",
        "de": "german",
        "zh": "chinese",
        "it": "italian",
        "ko": "korean",
        "pt": "portuguese",
        "ru": "russian",
        "ar": "arabic",
        "hi": "hindi",
        "tr": "turkish",
        "nl": "dutch",
        "sv": "swedish",
        "pl": "polish",
        "el": "greek",
        "he": "hebrew",
        "vi": "vietnamese",
        "th": "thai",
        "ca": "catalan"
        }

    async def async_call_llm(self, template, headers, session):
        """Async call to llm

        Args:
            template (dict): Json to call the service
            headers (dict): Headers params
            session (aiohttp.session): Session that mimics requests but allows async concurrent calls
        Returns:
            dict: In this case the output will be the response of LLM
        """
        async with session.post(self.URL, json=template, headers=headers, verify_ssl=True) as response:
            LLMP.control_errors(response, async_bool=True)
            return (await response.json(content_type='text/html'))['result']

    async def parallel_calls(self, templates, headers):
        """Async function that makes parallel calls using async_call_llm

        Args:
            template (list): List of jsons to call the service
            headers (dict): Headers params

        Returns:
            list: A ordered list depending on template order with llmapi response
        """
        async with aiohttp.ClientSession() as session:
            tasks = []
            for template in templates:
                task = asyncio.ensure_future(self.async_call_llm(template, headers, session))
                tasks.append(task)
            responses = await asyncio.gather(*tasks)
            return responses

    def parse_lang(self, lang):
        full_lang_name = self.LANG_MAP.get(lang)
        if full_lang_name is not None:
            return full_lang_name

        return lang
        

    def process(self, params, actions_confs):
        """Translates the query to the received langs.

        Args:
            params (_type_): _description_
            actions_confs (_type_): _description_

        Raises:
            PrintableGenaiError: _description_
            PrintableGenaiError: _description_
            PrintableGenaiError: _description_
        """
        headers = params.pop("headers")
        
        retrieve_action = None
        for action in actions_confs:
            if action["action"] == "retrieve":
                retrieve_action = action
        
        if retrieve_action is None:
            raise PrintableGenaiError(404, "Action retrieve not found for query expansion")

        langs = params.get("langs")
        if langs is None or len(langs) == 0:
            langs = ["en", "es"]
        
        if not isinstance(langs, list):
            raise PrintableGenaiError(500, "Param <langs> is not a list")

        templates = []
        for lang in langs:
            lang = self.parse_lang(lang)
            TRANSLATE_QUERY = f"Sentence: {self.query} \n Language: {lang}"
            TRANSLATE_TEMPLATE["query_metadata"]["query"] = TRANSLATE_QUERY
            templates.append(deepcopy(TRANSLATE_TEMPLATE))

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(self.parallel_calls(templates, headers))
        loop.close()

        queries = []
        for r in result:
            translated_query = r["answer"]
            queries.append(translated_query)
            retrieve_action["action_params"]["params"]["generic"]["index_conf"]["query"] = translated_query
            actions_confs.insert(0, deepcopy(retrieve_action))
        
        return queries
        
class ExpansionFactory:
    """
    Factory class for creating query expansion methods.
    """

    EXPANSIONS = [LangExpansion]

    def __init__(self, expansion_type: str) -> None:
        """
        Select the given query expansion method.

        Args:
            expansion_type (str): Type of the query expansion method.
        """
        self.expansionmethod: ExpansionMethod = None
        for expansionmethod in self.EXPANSIONS:
            if expansionmethod.TYPE == expansion_type:
                self.expansionmethod = expansionmethod
                break

        if self.expansionmethod is None:
            raise PrintableGenaiError(status_code=404, message=f"Provided query expansion method does not match any of the possible ones: {', '.join(f.TYPE for f in self.EXPANSIONS)}")


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
        return self.expansionmethod(query).process(params, actions_confs)
