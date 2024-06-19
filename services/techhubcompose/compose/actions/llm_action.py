### This code is property of the GGAO ###


import os
import json
import logging
import asyncio
import aiohttp
import requests
from pathlib import Path
from copy import deepcopy
from typing import List, Dict
from abc import abstractmethod, ABC

from ..utils.defaults import SUM_TEMPLATE
from ..streamchunk import StreamChunk
from common.errors.LLM import LLMParser
from common.errors.dolffiaerrors import DolffiaError

LLMP = LLMParser()
logger = logging.getLogger("Summarize")


class LLMMethod(ABC):
    """
    Abstract base class for LLM methods.

    Attributes:
        TYPE (str): The type of the LLM method.
        URL (str): The URL to call the LLM service.
        TEMPLATE (dict): The template for the LLM method.
        HEADERS (dict): The headers for the LLM method.

    Args:
        streamlist (list): The streamlist to process.

    Raises:
        DolffiaError: If there is an error in the LLM method.

    Returns:
        list: The processed streamlist.
    """

    TYPE: str = None
    URL: str = None
    TEMPLATE: dict = None
    HEADERS = {'Content-type': 'application/json'}

    def __init__(self, streamlist: list) -> None:
        """Instantiate the LLMMethod object.

        Args:
            streamlist (list): The streamlist to process.
        """
        self.streamlist = streamlist

    @abstractmethod
    def process(self, param) -> list:
        """Process the streamlist given the method.

        Args:
            param: The parameter for the LLM method.

        Returns:
            list: The processed streamlist.
        """

    def update_params(self, params):
        """Updates the template and the headers in order to make the call.

        Args:
            params (dict): The parameters to make the LLM call.

        Returns:
            tuple: A tuple containing the updated headers, template, and session ID.
        """
        headers = deepcopy(self.HEADERS)
        template = deepcopy(self.TEMPLATE)

        headers.update(params.get("headers_config", {}))
        session_id = params.get("session_id", None)

        template.update(params)
        return headers, template, session_id

    def call_llm(self, template, headers):
        """Given the template and headers, call the LLM service.

        Args:
            template (dict): The JSON to call the LLM service.
            headers (dict): The verification headers to make the call.

        Returns:
            dict: The LLM response.
        """
        r = requests.post(self.URL, json=template, headers=headers, verify=False)
        if r.status_code != 200:
            raise DolffiaError(status_code=r.status_code, message=f"Error from GENAI-LLMAPI: {r.text}")

        return LLMP.parse_response(r)

    def get_example(self):
        """Get an example of the LLM method.

        Returns:
            str: The example of the LLM method.
        """
        return json.dumps(self._get_example())

    def _get_example(self) -> dict:
        """Get an example dictionary to generate compose dict.

        Returns:
            dict: The example dictionary.
        """
        return {
            'type': self.TYPE, 
            'params': self.TEMPLATE
        }
    
    def adapt_query_for_model(self, llm_action, query_type, template):
        query = []
        if len(llm_action) > 0:
            for action in llm_action:
                if action['query_type'] == "image_url":
                    query.append({
                        "type": "image_url",
                        "image": {
                            "url": action['query'],
                            "detail": "high"
                        }
                    })

                if action['query_type'] == "image_b64":
                    query.append({
                        "type": "image_b64",
                        "image": {
                            "base64": action['query'],
                            "detail": "high"
                        }
                    })
                
                if action['query_type'] == "text":
                    query.append({
                        "type": "text",
                        "text": action['query']
                    })

        if query_type in ["image_url"]:
            query.append({
                "type": query_type,
                "image": {
                    "url": template['query_metadata']['query'],
                    "detail": "high"
                }
            })

        if query_type == "image_b64":
            query.append({
                "type": "image_b64",
                "image": {
                    "base64": template['query_metadata']['query'],
                    "detail": "high"
                }
            })
        
        if query_type == "text":
            query.append({
                "type": query_type,
                "text": template['query_metadata']['query']
            })

        if query != []:
            template['query_metadata']['query'] = query
    
        return template


class LLMSummarize(LLMMethod):
    """
    This class represents the LLMSummarize action, which is used to make a summary of the full text.
    """

    TYPE = "llm"

    URL = os.environ['URL_LLM']
    TEMPLATE = SUM_TEMPLATE
    TEXT_KEY = "content"

    def clear_output(self):
        """Deletes all the answers used to make a global summary. Not needed in this case
        """

    def add_hystoric(self, session_id, template, PD):
        """ Adds persistence to compose summaries 
        """
        hystoric = PD.get_conversation(session_id)
        persistence = []
        for h in hystoric:
            if isinstance(h, dict):
                if 'user' in h:
                    persistence.append({'role': "user", 'content': h['user']})
                if 'system' in h:
                    persistence.append({'role': "system", 'content': h['system']})
                if 'assistant' in h:
                    persistence.append({'role': "assistant", 'content': h['assistant']})

        if persistence:
            template['query_metadata']['persistence'] = [(persistence[i], persistence[i+1]) for i in range(0, len(persistence) - 1, 2)]
        return template

    def process(self, params):
        """Main function to process each streamlist, make a summary of the full text. 
            Take into account that some text can be ignored by llm to make the summary
        """
        headers, template, session_id = self.update_params(params)
        PD = template.pop('PD')
        query_type = template.pop('query_type')
        llm_action = template.pop('llm_action')

        texts = []
        for sl in self.streamlist:
            meta = sl.meta
            title = meta.get("title", meta.get("filename"))
            if title and isinstance(title, str):
                texts.append(Path(Path(title).name).stem.upper())

            content = sl.content
            if content is not None:
                texts.append(sl.content)

        if texts:
            context = "\n".join(texts)
            template['query_metadata']['context'] = context
            if session_id:
                template = self.add_hystoric(session_id, template, PD)
            
            template = self.adapt_query_for_model(llm_action, query_type, template)

            result = self.call_llm(template, headers)

            if session_id:
                persistence = {"user": template['query_metadata']['query']}
                if result['answer']:
                    persistence.update({"assistant": result['answer']})
                    if isinstance(result['query_tokens'], list):
                        for i, q in enumerate(persistence['user']):
                            q['n_tokens'] = result['query_tokens'][i]
                    else:
                        persistence['n_tokens'] = result['query_tokens'] 

                    persistence['input_tokens'] = result['input_tokens']
                    persistence['output_tokens'] = result['output_tokens']

                PD.update_last(persistence, session_id)
                PD.update_context(session_id, context)
                

            self.clear_output()
            self.streamlist.append(StreamChunk({"content":"", "meta": {"title": "Summary"}, "scores":1, "answer": result['answer']}))

        return self.streamlist


class LLMSummarizeContent(LLMSummarize):
    TYPE = "llm_content"
    TEXT_KEY = "content"


class LLMSummarizeAnswer(LLMSummarize):
    TYPE = "llm_answer"
    TEXT_KEY = "answer"

    def clear_output(self):
        """Deletes all the answers used to make a global summary
        """
        for sl in self.streamlist:
            sl.pop("answer", None)


class LLMSummarizeSegments(LLMMethod):
    """
    Class for summarizing segments using LLM (Language Model Microservice).
    """

    TYPE = "llm_segments"

    URL = os.environ['URL_LLM']
    TEMPLATE = SUM_TEMPLATE

    def parse_streamlists(self):
        """
        Function to modify streamlists in case it is wanted in the future.
        Currently, it does nothing.
        """

    async def async_call_llm(self, template, headers, session):
        """
        Async function to call LLM service.

        Args:
            template (dict): JSON to call the service.
            headers (dict): Headers parameters.
            session (aiohttp.session): Session that mimics requests but allows async concurrent calls.

        Returns:
            dict: The response from LLM.
        """
        async with session.post(self.URL, json=template, headers=headers, verify_ssl=False) as response:
            LLMP.control_errors(response, async_bool=True)
            return (await response.json(content_type='text/html'))['result']

    async def parallel_calls(self, templates, headers):
        """
        Async function that makes parallel calls using async_call_llm.

        Args:
            templates (list): List of JSONs to call the service.
            headers (dict): Headers parameters.

        Returns:
            list: An ordered list depending on the template order with LLM API responses.
        """
        async with aiohttp.ClientSession() as session:
            tasks = []
            for template in templates:
                task = asyncio.ensure_future(self.async_call_llm(template, headers, session))
                tasks.append(task)
            responses = await asyncio.gather(*tasks)
            return responses

    def process(self, params):
        """
        Main function to process each streamlist, make a summary of them, and then return a global summary.
        Take into account that some text can be ignored by LLM to make the summary.

        Args:
            params (dict): Parameters for processing.

        Returns:
            list: The processed streamlist.
        """
        self.parse_streamlists()
        headers, template, session_id = self.update_params(params)
        template.pop("PD")

        if session_id:
            logger.warning("llm_segments does not have a chat-like persistence.")

        top_qa = template.pop("top_qa")
        llm_action = template.pop("llm_action")
        query_type = template.pop("query_type")

        templates = []
        for sl in self.streamlist[:top_qa]:
            template['query_metadata']['context'] = sl.content
            template = self.adapt_query_for_model(llm_action, query_type, template)
            templates.append(deepcopy(template))

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(self.parallel_calls(templates, headers))
        loop.close()

        for i, r in enumerate(result):
            if "answer" in r:
                self.streamlist[i].answer = r['answer']

        return self.streamlist


class LLMFactory:

    SUMMARIES = [LLMSummarizeContent, LLMSummarizeSegments, LLMSummarizeAnswer]

    def __init__(self, llm_type: str) -> None:
        """Select the given summarize

        Args:
            summarize_type (str): one of the available summarizes
        """

        self.llm_method: LLMMethod = None
        for llm_method in self.SUMMARIES:
            if llm_method.TYPE == llm_type:
                self.llm_method = llm_method
                break

        if self.llm_method is None:
            raise DolffiaError(status_code=404, message=f"Provided llm_action does not match any of the possible ones: {', '.join(f.TYPE for f in self.SUMMARIES)}")

    def process(self, streamlist: list, params):
        """Process the streamlist with the given method
        """
        return self.llm_method(streamlist).process(params)
