### This code is property of the GGAO ###


import os
import requests
from abc import ABC
from common.errors.genaierrors import PrintableGenaiError
from common.errors.LLM import LLMParser
from ..utils.defaults import TRANSLATE_TEMPLATE, STEP_TEMPLATE
import asyncio
import aiohttp
from copy import deepcopy

LLMP = LLMParser()

class ExpansionMethod(ABC):
    """
    Abstract base class for query expansion methods.
    """

    TYPE: str = ""
    URL = os.environ['URL_LLM']
    HEADERS = {'Content-type': 'application/json'}

    def __init__(self, query) -> None:
        """
        Instantiate ExpansionMethod.

        Args:
            query (str): Query to be expanded.
        """
        self.query = query


    def get_retrieve_action(self, actions_confs):
        retrieve_action = []
        for action in actions_confs:
            if action['action'] == "retrieve":
                retrieve_action.append(deepcopy(action))
        
        if len(retrieve_action) == 0:
            raise PrintableGenaiError(404, "Action retrieve not found for query expansion")
        
        return retrieve_action

class StepSplitExpansion(ExpansionMethod):

    TYPE = "steps"

    def call_llm(self, template, headers):
        """Given the template and headers, call the LLM service.

        Args:
            template (dict): The JSON to call the LLM service.
            headers (dict): The verification headers to make the call.

        Returns:
            dict: The LLM response.
        """
        try:
            r = requests.post(self.URL, json=template, headers=headers, verify=True)
        except Exception as ex:
            raise PrintableGenaiError(
                status_code=500, message=f"Error calling GENAI-LLMAPI: {ex}"
            )

        if r.status_code != 200:
            raise PrintableGenaiError(
                status_code=r.status_code, message=f"Error from GENAI-LLMAPI: {r.text}"
            )

        return LLMP.parse_response(r)

    def process(self, params: dict, actions_confs: dict):
        headers = params.pop("headers")
        template_name = params.get("template_name")
        template = deepcopy(STEP_TEMPLATE)
        if template_name:
            template["query_metadata"].pop("template")
            template["query_metadata"]["prompt_template_name"] = template_name
        
        retrieve_action = self.get_retrieve_action(actions_confs)

        k_steps = params.get("k_steps")
        if k_steps is None or not isinstance(k_steps, int):
            raise PrintableGenaiError(404, "K_steps not provided or not valid")
        
        model = params.get("model")
        context = params.get("context")

        retriever_result = []
        for retriever in retrieve_action:
            query = retriever['action_params']['params']['indexation_conf']['query']
            STEP_QUERY = f"Input Query: {query}, K queries or lower: {k_steps} \n Response: [List of simpler queries here]"
            if context:
                STEP_QUERY = f"Context: {context}, {STEP_QUERY}"
            template['query_metadata']['query'] = STEP_QUERY
            if model is not None:
                template['llm_metadata']['model'] = model
            
            result = self.call_llm(template, headers)
            retriever_result.append(result)

        queries = []
        for retriever, result in zip(retrieve_action, retriever_result):
            n_q = 0
            for r in result['answer'].split("\n"):
                n_q += 1
                if n_q > 10:
                    break
                queries.append(r)
                retriever['action_params']['params']['indexation_conf']['query'] = r
                actions_confs.insert(0, deepcopy(retriever))
        
        return queries
        

class LangExpansion(ExpansionMethod):
    """
    Language based expansion method for the query
    """

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
            return (await response.json())['result']

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
            params (dict): Params
            actions_confs (list): List with the actions 

        Raises:
            PrintableGenaiError
        """
        headers = params.pop("headers")
        
        retrieve_action = self.get_retrieve_action(actions_confs)

        langs = params.get("langs")
        if langs is None or len(langs) == 0:
            raise PrintableGenaiError(404, "Langs to expand not provided")
        
        translate_model = params.get("model")
        
        if not isinstance(langs, list):
            raise PrintableGenaiError(400, "Param <langs> is not a list")

        retriever_result = []
        for retriever in retrieve_action:
            templates = []
            query = retriever['action_params']['params']['indexation_conf']['query']
            for lang in langs:
                lang = self.parse_lang(lang)
                TRANSLATE_QUERY = f"Sentence: <{query}> \n Language: {lang}"
                TRANSLATE_TEMPLATE['query_metadata']['query'] = TRANSLATE_QUERY
                if translate_model is not None:
                    TRANSLATE_TEMPLATE['llm_metadata']['model'] = translate_model
                templates.append(deepcopy(TRANSLATE_TEMPLATE))

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result =  loop.run_until_complete(self.parallel_calls(templates, headers))
            loop.close()
            retriever_result.append(result)

        queries = []
        for retriever, result in zip(retrieve_action, retriever_result):
            for r in result:
                translated_query = r['answer']
                queries.append(translated_query)
                retriever['action_params']['params']['indexation_conf']['query'] = translated_query
                actions_confs.insert(0, deepcopy(retriever))
        
        return queries
        
class ExpansionFactory:
    """
    Factory class for creating query expansion methods.
    """

    EXPANSIONS = [LangExpansion, StepSplitExpansion]

    def __init__(self, expansion_type: str) -> None:
        """
        Select the given query expansion method.

        Args:
            expansion_type (str): Type of the query expansion method.
        """
        self.expansionmethod = None
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
