### This code is property of the GGAO ###


import os
import json
import asyncio
import aiohttp

from copy import deepcopy
from typing import List, Dict
from abc import abstractmethod, ABC
from common.errors.LLM import LLMParser
from common.errors.genaierrors import PrintableGenaiError
from ..utils.defaults import FILTER_TEMPLATE
from dateutil.parser import parse

LLMP = LLMParser()


class FilterMethod(ABC):
    TYPE: str

    def __init__(self, streamlist: list) -> None:
        """Instantiate streamlist

        Args:
            streamlist (list): Streamlist
        """
        self.streamlist = streamlist

    @abstractmethod
    def process(self) -> List:
        """Process the streamlist given the method
        """
        pass

    def get_example(self):
        return json.dumps(self._get_example())

    @abstractmethod
    def _get_example(self) -> Dict:
        """Return example
        """
        return {}


class TopKFilter(FilterMethod):
    """Filter the streamlist by the first top k elements

    Args:
        FilterMethod : Class

    Returns:
        streamlist: Streamlist with the top k elements
    """
    TYPE = "top_k"

    def process(self, params: dict = {}):
        """Process the streamlist given the method
        """
        top_k = params.get('top_k', None)
        return self.streamlist[:int(top_k)]

    def _get_example(self) -> Dict:
        return {
            "type": "top_k",
            "params": {
                "top_k": 5
            }
        }


class PermissionFilter(FilterMethod):
    TYPE = "permission"
    TEMPLATE = FILTER_TEMPLATE
    URL = os.environ.get('URL_ALLOWED_DOCUMENTS_KNOWLER')
    HEADERS = {'Content-type': 'application/json'}
    BODY = {}

    def process(self, params = {}):
        """Process the streamlist and filter unauthorized chunks. 
        """
        headers, _ = self.update_params(params)
        # Get input documents IDs
        document_ids = []
        for sc in self.streamlist:
            knowler_id = sc.meta["knowler_id"]
            document_ids.append(knowler_id)

        self.BODY["internalIds"] = list(set(document_ids))  # IDs must be unique
        self.HEADERS["Authorization"] = headers.get("user-token", "")
        self.HEADERS["delegate-token"] = headers.get("delegate-token", "")
        url = self.URL
        self.URL = params.get("url_allowed_documents", url)
        if self.URL is None:
            raise PrintableGenaiError(404, "Variable URL_ALLOWED_DOCUMENT not found")
        # Check permissions for documents
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(self.send_post_request(self.URL, self.BODY, self.HEADERS))
        loop.close()
        permitted_docs = [k for k, v in result["allowed"].items() if v]

        # Filter non permitted streamchunks
        output_sl = []
        for sc in self.streamlist:
            knowler_id = sc.meta["knowler_id"]
            if knowler_id in permitted_docs:
                output_sl.append(sc)

        return output_sl

    async def send_post_request(self, url: str, body: Dict, headers: Dict) -> Dict:
        """Async function that makes calls to a given URL

        Args:
            url (str): GetUserAllowedDocuments service URL
            body (Dict): Query with documents ids
            headers (Dict): User identification

        Returns:
            response (Dict): GetUserAllowedDocuments service response
        """

        async with aiohttp.ClientSession() as session:
            async with session.post(url=url, data=json.dumps(body), headers=headers) as response:
                # Check if the request was successful (status code 2xx)
                if response.status // 100 == 2:
                    return await response.json()
                else:
                    # If the request was not successful, raise an exception
                    response_text = await response.text()
                    raise PrintableGenaiError(status_code=response.status, message=response_text)

    def update_params(self, params):
        """Updates the template and the headers in order to make the call

        Args:
            params (dict): params to make GetUserAllowedDocuments call
        """
        headers = deepcopy(self.HEADERS)
        template = deepcopy(self.TEMPLATE)

        headers.update(params.pop("headers_config", {}))
        template.update(params)
        return headers, template

    def get_example(self):
        return json.dumps(self._get_example())

    def _get_example(self) -> Dict:
        """Example to generate compose dict
        """
        return {
            'type': self.TYPE,
            'params': self.TEMPLATE
        }


class RelatedToFilter(FilterMethod):
    TYPE = "related_to"
    TEMPLATE = FILTER_TEMPLATE
    URL = os.environ['URL_LLM']
    TEXT_KEY = "content"
    HEADERS = {'Content-type': 'application/json'}

    def process(self, params = {}):
        """Process the streamlist given the method. This method filters the streamlist given the context
            of the text. It uses the LLMApi service to make the call and check if the text is related with
            the context.  If the output of the call is Yes, the streamlist is added to next phase.
        """
        self.parse_streamlists()
        headers, template = self.update_params(params)

        templates = []
        for sc in self.streamlist:
            template['query_metadata']['context'] = sc.content
            templates.append(deepcopy(template))

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(self.parallel_calls(templates, headers))
        loop.close()

        output_sl = []
        for sl, r in zip(self.streamlist, result):
            # Test if the response of the llmapi is Yes
            if "answer" in r and r['answer'].count('Yes') == 1:
                sl.answer = r['answer']
                output_sl.append(sl)
        return output_sl

    def update_params(self, params):
        """Updates the template and the headers in order to make the call

        Args:
            params (dict): params to make llm call
        """
        headers = deepcopy(self.HEADERS)
        template = deepcopy(self.TEMPLATE)

        headers.update(params.pop("headers_config", {}))
        template.update(params)
        return headers, template

    def parse_streamlists(self):
        """Function to modify streamlists in case in the future is wanted, nowadays does nothing
        """

    async def async_call_llm(self, template, headers, session):
        """Async call to llm

        Args:
            template (dict): Json to call the service
            headers (dict): Headers params
            session (aiohttp.session): Session that mimics requests but allows async concurrent calls
        Returns:
            dict: In this case the output will be the response of LLM
        """
        async with session.post(self.URL, json=template, headers=headers, verify_ssl=False) as response:
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

    def get_example(self):
        return json.dumps(self._get_example())

    def _get_example(self) -> Dict:
        """Example to generate compose dict
        """
        return {
            'type': self.TYPE,
            'params': self.TEMPLATE
        }


class MetadataFilter(FilterMethod):
    """Filter the streamlist by different metadata conditions.

    Args:
        FilterMethod (type): The base class for filter methods.

    Raises:
        GenaiError: If the metadata key does not exist.
        GenaiError: If the dictionary has an invalid key.
        GenaiError: If the date format is not valid.

    Returns:
        bool: True if the metadata passes the filter conditions, False otherwise.
    """
    TYPE = "metadata"

    def apply_subfilter(self, metadata, sub_filter):
        """Apply a subfilter to the metadata.

        Args:
            metadata (dict): The metadata dictionary.
            sub_filter (dict): The subfilter dictionary.

        Raises:
            GenaiError: If the metadata key does not exist.
            GenaiError: If the dictionary has an invalid key.

        Returns:
            bool: True if the metadata passes the subfilter conditions, False otherwise.
        """
        for key, value in sub_filter.items():
            if value[0] not in metadata:
                raise PrintableGenaiError(status_code=404, message=f"Metadata key {value[0]} does not exist")

            if key == "eq":
                if metadata.get(value[0]) == value[1]:
                    return True
            elif key == "gt":
                float_ = float(metadata.get(value[0]))
                if float_ > value[1]:
                    return True
            elif key == "lt":
                float_ = float(metadata.get(value[0]))
                if float_ < value[1]:
                    return True
            elif key == "in" or key == "metaintext":
                if metadata.get(value[0]) in value[1]:
                    return True
            elif key == "textinmeta":
                if value[1] in metadata.get(value[0]):
                    return True
            elif key == "eq_date":
                metadata_date = self.metadata_to_datetime(metadata.get(value[0]))
                received_date = self.metadata_to_datetime(value[1])
                if metadata_date == received_date:
                    return True
            elif key == "gt_date":
                metadata_date = self.metadata_to_datetime(metadata.get(value[0]))
                received_date = self.metadata_to_datetime(value[1])
                if metadata_date > received_date:
                    return True
            elif key == "lt_date":
                metadata_date = self.metadata_to_datetime(metadata.get(value[0]))
                received_date = self.metadata_to_datetime(value[1])
                if metadata_date < received_date:
                    return True
            else:
                raise PrintableGenaiError(status_code=400,
                                          message=f"Dictionary must have only eq, gt, lt, in, metaintext or textinmeta in keys. See example {self._get_example()}")
            return False

    def metadata_to_datetime(self, value):
        """Convert a metadata value to a datetime object.

        Args:
            value (str): The metadata value.

        Raises:
            GenaiError: If the date format is not valid.

        Returns:
            datetime.datetime: The datetime object.
        """
        try:
            return parse(value)
        except Exception as ex:
            raise PrintableGenaiError(status_code=500, message=f" Error: {ex}.\n Date format not valid.")

    def operator(self, a, b, operator):
        """Apply the operator to the two boolean values.

        Args:
            a (bool): The first boolean value.
            b (bool): The second boolean value.
            operator (str): The operator to apply.

        Raises:
            GenaiError: If the operator is invalid.

        Returns:
            bool: The result of applying the operator to the two boolean values.
        """
        if operator == "and":
            return a and b
        elif operator == "or":
            return a or b
        else:
            raise PrintableGenaiError(status_code=500,
                                      message=f"Operator must be 'and' or 'or'. See example {self._get_example()}")

    def apply_filter(self, metadata, filter_dict, operator):
        """Apply the filter to the metadata.

        Args:
            metadata (dict): The metadata dictionary.
            filter_dict (dict): The filter dictionary.
            operator (str): The operator to combine the filters.

        Raises:
            GenaiError: If the dictionary has an invalid key.

        Returns:
            bool: True if the metadata passes the filter conditions, False otherwise.
        """
        boolean = False
        if "and" in filter_dict:
            and_subfilter = all(self.apply_subfilter(metadata, sub_filter) for sub_filter in filter_dict["and"])
            boolean = and_subfilter
            if "or" in filter_dict:
                or_subfilter = any(self.apply_subfilter(metadata, sub_filter) for sub_filter in filter_dict["or"])
                boolean = self.operator(and_subfilter, or_subfilter, operator)

        elif "or" in filter_dict:
            or_subfilter = any(self.apply_subfilter(metadata, sub_filter) for sub_filter in filter_dict["or"])
            boolean = or_subfilter
        else:
            raise PrintableGenaiError(status_code=400,
                                      message=f"Only 'and' or 'or' keys must be defined. See example {self._get_example()}")
        return boolean

    def filter_data(self, filter_dict, operator):
        """Filter the streamlist based on the filter conditions.

        Args:
            filter_dict (dict): The filter conditions.
            operator (str): The operator to combine the filters.

        Returns:
            list: The filtered data.
        """
        filtered_data = []
        for item in self.streamlist:
            if self.apply_filter(item.meta, filter_dict, operator):
                filtered_data.append(item)
        return filtered_data

    def process(self, params: dict = {}):
        """Process the streamlist given the method.

        Args:
            params (dict): The parameters for the filter method.

        Returns:
            list: The filtered data.
        """
        operator = params.get('operator', 'and')
        assert operator in ['and', 'or'], f"Operator must be 'and' or 'or'. See example {self._get_example()}"

        return self.filter_data(params['filter_conditions'], operator)

    def _get_example(self) -> dict:
        """Get an example of the filter conditions.

        Returns:
            dict: An example of the filter conditions.
        """
        return {
            "type": "top_k",
            "params": {
                "filter_conditions": {
                    "or": [
                        {"eq": ("city", "New York")},  # Checks if a metadata is equal to a value.
                        {"in": ("city", ["New York", "London"])},  # Checks if a metadata is in a list of values.
                        {"textinmeta": ("city", "New Yor")},  # Checks if a string is contained in the metadata.
                        {"metaintext": ("city", "The city of New York is known as the big apple.")}
                        # Checks if a metadata is contained in a string.
                    ],
                    "and": [
                        {"gt": ("age", 30)},  # Checks if a metadata is greater than a value.
                        {"lt": ("age", 40)}  # Checks if a metadata is lower than a value.
                    ]
                },
                "operator": "and"  # Operator to combine the filters. It can be 'and' or 'or'.
            }
        }


class FilterFactory:
    FILTERS = [TopKFilter, RelatedToFilter, MetadataFilter, PermissionFilter]

    def __init__(self, filter_type: str) -> None:
        """Select the given filter

        Args:
            filter_type (str): one of the available filters
        """

        self.filtermethod = None
        for filtermethod in self.FILTERS:
            if filtermethod.TYPE == filter_type:
                self.filtermethod = filtermethod
                break

        if self.filtermethod is None:
            raise PrintableGenaiError(status_code=404,
                                      message=f"Provided filter does not match any of the possible ones: {', '.join(f.TYPE for f in self.FILTERS)}")

    def process(self, streamlist: list, params: dict):
        """Process the streamlist with the given method
        """
        filtered_streamlist = self.filtermethod(streamlist).process(params)
        if not filtered_streamlist:
            raise PrintableGenaiError(status_code=404, message="Error after filtering. NO documents passed the filters")
        else:
            return filtered_streamlist
