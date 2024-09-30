### This code is property of the GGAO ###


# Native imports
from typing import List
from string import Template
from abc import ABC, abstractmethod
import os

# Local imports
from common.services import GENAI_LLM_MESSAGES
from common.logging_handler import LoggerHandler
from common.errors.genaierrors import PrintableGenaiError
from adapters import ManagerAdapters


class Message(ABC):
    MODEL_FORMAT = "Message"

    def __init__(self):
        """Message object"""
        logger_handler = LoggerHandler(GENAI_LLM_MESSAGES, level=os.environ.get('LOG_LEVEL', "INFO"))
        self.logger = logger_handler.logger

    @staticmethod
    def _is_query_ok(query: [str, list], is_vision: bool = True) -> [str, list]:
        """Given a query it will check if the format is correct.

        :param query: Question made.
        :param is_vision: Boolean to check if the query is a vision model or not.
        :return: Query if its format is correct
        """
        if isinstance(query, str):
            return query
        if not isinstance(query, list) and is_vision:
            raise PrintableGenaiError(400, "query must be a list")
        if not is_vision:
            raise PrintableGenaiError(400, "query and persistence user content must be a string for non vision models")
        for el in query:
            if isinstance(el, dict):
                additional_keys = set(el.keys()) - {'type', 'text', 'image', 'n_tokens'}
                if additional_keys:
                    raise PrintableGenaiError(400, f"Incorrect keys: {additional_keys}")
                type = el.get('type')
                if type not in ['text', 'image_url', 'image_b64']:
                    raise PrintableGenaiError(400, "Type must be one in ['text', 'image_url', 'image_b64']")
                if type == 'text':
                    if not el.get('text') or not isinstance(el.get('text'), str):
                        raise PrintableGenaiError(400, "For type 'text' there must be a key 'text' containing a string")
                elif type in ['image_url', 'image_b64']:
                    image = el.get('image')
                    if not image or not isinstance(image, dict):
                        raise PrintableGenaiError(400, "'image' param must be a dict")
                    if not isinstance(image.get('url'), str) and not isinstance(image.get('base64'), str):
                        raise PrintableGenaiError(400, "Type 'image' must contain a 'url' key with a string or a 'base64' key with "
                                         "a string")
                    if image.get('detail') and image.get('detail') not in ["high", "low", "auto"]:
                        raise PrintableGenaiError(400, "Detail parameter must be one in ['high', 'low', 'auto']")
                    additional_keys = set(image.keys()) - {'url', 'detail', 'base64'}
                    if additional_keys:
                        raise PrintableGenaiError(400, f"Incorrect keys: {additional_keys}")
                else:
                    raise PrintableGenaiError(400, "Key must be 'type' and its value must be one in ['text', 'image']")
            else:
                raise PrintableGenaiError(400, "Elements of the content must be dict {}")
        return query

    def _is_persistence_ok(self, persistence: list, is_vision: bool = False) -> List:
        """Given a persistence it will check if the format is correct.

        :param persistence: Persistence list containing the conversation history
        :param is_vision: Boolean to check if the query is a vision model or not.
        :return: Persistence if its format is correct
        """
        for pair in persistence:
            if not isinstance(pair, list):
                raise PrintableGenaiError(400, "Persistence must be a list containing lists")
            if len(pair) != 2:
                raise PrintableGenaiError(400, "Content must contain pairs of ['user', 'assistant']")
            additional_keys = [key for el in pair for key in el.keys() if key not in {'role', 'content', 'n_tokens'}]
            if additional_keys:
                raise PrintableGenaiError(400, f"Incorrect keys: {additional_keys}. Accepted keys: {'role', 'content', 'n_tokens'}")
            roles = [el.get('role') for el in pair]
            if roles[0] != "user" or roles[1] != "assistant":
                raise PrintableGenaiError(400, "In persistence, first role must be 'user' and second role must be 'assistant'")
            for el in pair:
                if el.get('role') == "user":
                    if not el.get('content'):
                        raise PrintableGenaiError(400, "'User' role must have a content key")
                    if not isinstance(el.get('content'), str) and not isinstance(el.get('content'), list):
                        raise PrintableGenaiError(400, "'User' role content must be a string for non-vision models or a list for "
                                         "vision models")
                    self._is_query_ok(el.get('content'), is_vision)
                if el.get('role') == "assistant":
                    if not el.get('content') or not isinstance(el.get('content'), str):
                        raise PrintableGenaiError(400, "'assistant' role must have a content key containing a string")
        return persistence

    @staticmethod
    def _get_user_query_tokens(query: list) -> [int, List]:
        """Given a query it will return the number of tokens

        :param query: Query containing the user content
        :return: Number of tokens
        """
        n_tokens = []
        for message in query:
            if message.get('role') == "user":
                if isinstance(message['content'], str):
                    n_tokens.append(message.get('n_tokens'))
                elif isinstance(message['content'], list):
                    for item in message['content']:
                        n_tokens.append(item.get('n_tokens'))

        return n_tokens[0] if len(n_tokens) == 1 else n_tokens

    def unitary_persistence(self) -> List:
        """Given a persistence it will return a list with the messages in a unitary format.

        :return: List with the messages in a unitary format
        """
        unitary_persistence = []
        for pair in self.persistence:
            for message in pair:
                unitary_persistence.append(message)
        return unitary_persistence

    @abstractmethod
    def preprocess(self):
        """Given a query and a context it will return the text in the GPT model format. 
        """
        pass

    @classmethod
    def is_message_type(cls, message_type: str):
        """It will chech if a model_type coincides with the model format
        """
        return message_type == cls.MODEL_FORMAT

    def __repr__(self):
        return f'{{query:{self.query}, ' \
               f'context:{self.context}, ' \
               f'template_name:{self.template_name}, ' \
               f'template:{self.template}}}'


class PromptGPT3Message(Message):
    MODEL_FORMAT = "promptGPT"

    def __init__(self, query: str, template: dict, template_name: str = "system_query", context: str = "",
                 system: str = ""):
        """Prompt object. It is used for text-only models such as gpt3-turbo instuct

        :param query: Question made.
        :param template: Template used to build the prompt.
        :param template_name: Template Name used to build the prompt.
        :param context: Context used to answer the question.
        :param system: Not used in this version

        """
        super().__init__()
        if isinstance(query, list):
            raise PrintableGenaiError(400, "query must be a string for non vision models")
        self.query = query
        self.context = context
        self.template_name = template_name
        self.template = template
        self.multiprompt = False

    def preprocess(self) -> str:
        """Given a query and a context it will return the text in the GPT model format.

        :return: List with the messages in the correct format for the model
        """

        user_prompt = Template(self.template["user"])
        return user_prompt.safe_substitute(query=self.query, context=self.context)


class DalleMessage(Message):
    MODEL_FORMAT = "dalle"

    def __init__(self, query: str, template: dict, system: str = "", context: str = "", user: str = "",
                 template_name: str = "", persistence=()):
        """Dalle message object.
        :param query: A text description of the desired image(s). The maximum length is 1000 characters for dall-e-2 and 4000 characters for dall-e-3
        :param model: The model to use for image generation.
        :param n: The number of images to generate. Must be between 1 and 10. For dall-e-3, only n=1 is supported.
        :param quality: The quality of the image that will be generated. hd creates images with finer details and greater consistency across the image. This param is only supported for dall-e-3.
        :param response_format: The format in which the generated images are returned. Must be one of url or b64_json.
        :param size: The size of the generated images. Must be one of 256x256, 512x512, or 1024x1024 for dall-e-2. Must be one of 1024x1024, 1792x1024, or 1024x1792 for dall-e-3 models.
        :param style: The style of the generated images. Must be one of vivid or natural. Vivid causes the model to lean towards generating hyper-real and dramatic images. Natural causes the model to produce more natural, less hyper-real looking images. This param is only supported for dall-e-3.
        :param user: A unique identifier representing your end-user, which can help OpenAI to monitor and detect abuse.
        """
        super().__init__()
        self.query = self._is_query_ok(query, False)
        self.template = template
        self.template_name = template_name
        self.persistence = self._is_persistence_ok(persistence) if isinstance(persistence, list) else []
        self.multiprompt = bool(self.persistence)
        self.system = system
        self.context = context
        self.substituted_query = []
        self.adapter = ManagerAdapters.get_adapter({'adapter': "dalle", 'message': self})
        self.adapter.adapt_query_and_persistence()
        self.user_query_tokens = self._get_user_query_tokens(self.substituted_query)

    def get_query_tokens(self, query: list) -> [int, List]:
        """Given a query it will return the number of tokens

        :param query: Query containing the user content
        :return: Number of tokens
        """
        n_tokens = 0
        for message in query:
            n_tokens += len(self.adapter.encoding.encode(message['content']))

        return n_tokens

    def preprocess(self) -> List:
        """Given a query and a context it will return the text in the GPT model format.

        :return: List with the messages in the correct format for the model
        """
        user_prompt = Template(self.template["user"])
        if "system" in self.template:
            system_prompt = Template(self.template["system"])
            system_content = system_prompt.safe_substitute(system=self.system)
        else:
            system_content = self.system

        user_content = user_prompt.safe_substitute(query=self.query, context=self.context)
        return [{'role': 'system', 'content': system_content}] + self.unitary_persistence() + [{'role': 'user', 'content': user_content}]

    def __repr__(self):
        """Representation of the object"""
        return f'{{query:{self.query}, ' \
               f'template_name:{self.template_name}, ' \
               f'template:{self.template}}}'


class ChatGPTMessage(Message):
    MODEL_FORMAT = "chatGPT"

    def __init__(self, query: str, template: dict, template_name: str = "system_query", context: str = "",
                 system='You are a helpful assistant', functions=None, function_call: str = "none", persistence=()):
        """Chat object. It is used for models that admit persitance as an input such as gpt3.5 or gpt4.

        :param query: Question made.
        :param template: Template used to build the prompt.
        :param template_name: Template Name used to build the prompt.
        :param context: Context used to answer the question.
        :param system:  openai model system configuration
        :param functions: List of functions to be used in the model
        :param function_call: Function call to be used in the model
        :param persistence: List containing the conversation history

        """
        super().__init__()
        self.query = self._is_query_ok(query, False)
        self.context = context
        if functions is None:
            functions = []
        self.template_name = template_name
        self.template = template
        self.system = system
        self.functions = functions
        self.function_call = function_call
        self.persistence = self._is_persistence_ok(persistence) if isinstance(persistence, list) else []
        self.multiprompt = bool(self.persistence)
        self.substituted_query = []
        adapter = ManagerAdapters.get_adapter({'adapter': "base", 'message': self})
        adapter.adapt_query_and_persistence()
        self.user_query_tokens = self._get_user_query_tokens(self.substituted_query)

    def preprocess(self) -> List:
        """Given a query and a context it will return the text in the GPT model format.

        :return: List with the messages in the correct format for the model
        """
        user_prompt = Template(self.template["user"])
        if "system" in self.template:
            system_prompt = Template(self.template["system"])
            system_content = system_prompt.safe_substitute(system=self.system)
        else:
            system_content = self.system

        user_content = user_prompt.safe_substitute(query=self.query, context=self.context)

        return [{'role': 'system', 'content': system_content}] + self.unitary_persistence() + [{'role': 'user', 'content': user_content}]


class ChatGPTvMessage(Message):
    MODEL_FORMAT = "chatGPT-v"

    def __init__(self, query: list, template: dict, template_name: str = "system_query", context: str = "",
                 system='You are a helpful assistant', functions=None, function_call: str = "none", persistence=()):
        """Chat object. It is used for models that admit persitance as an input such as gpt3.5 or gpt4.

        :param query: Question made.
        :param template: Template used to build the prompt.
        :param template_name: Template Name used to build the prompt.
        :param context: Context used to answer the question.
        :param system:  openai model system configuration
        :param functions: List of functions to be used in the model
        :param function_call: Function call to be used in the model
        :param persistence: List containing the conversation history
        """
        super().__init__()
        self.query = super()._is_query_ok(query)
        if context != "" and isinstance(self.query, list):
            raise PrintableGenaiError(400, "Context param not allowed in vision models")
        self.context = context
        if functions is None:
            functions = []
        self.template_name = template_name
        self.template = template
        self.system = system
        self.functions = functions
        self.function_call = function_call
        self.persistence = self._is_persistence_ok(persistence, True) if isinstance(persistence, list) else []
        self.multiprompt = bool(self.persistence)
        self.substituted_query = []
        adapter = ManagerAdapters.get_adapter({'adapter': "gpt4v", 'message': self})
        adapter.adapt_query_and_persistence()
        self.user_query_tokens = self._get_user_query_tokens(self.substituted_query)

    def preprocess(self) -> List:
        """Given a query and a context it will return the text in the GPT model format.

        :return: List with the messages in the correct format for the model
        """
        if "system" in self.template:
            system_prompt = Template(self.template["system"])
            system_content = system_prompt.safe_substitute(system=self.system)
        else:
            system_content = self.system

        user_content = []
        user_prompt = Template(self.template["user"])
        if isinstance(self.template["user"], str):
            user_content = user_prompt.safe_substitute(query=self.query, context=self.context)
        else:
            for e in self.template["user"]:
                if e == "$query":
                    user_content.extend(self.query)
                else:
                    user_content.append(e)

        return [{'role': 'system', 'content': system_content}] + self.unitary_persistence() + [{'role': 'user', 'content': user_content}]


class ClaudeMessage(Message):
    MODEL_FORMAT = "chatClaude"

    def __init__(self, query: str, template: dict, template_name: str = "system_query", context: str = "",
                 system='You are a helpful assistant', persistence=()):
        """Chat object. It is used for models that admit persitance as an input such as gpt3.5 or gpt4.

        :param query: Question made.
        :param template: Template used to build the prompt.
        :param template_name: Template Name used to build the prompt.
        :param context: Context used to answer the question.
        :param system:  openai model system configuration
        :param persistence: List containing the conversation history
        """
        super().__init__()
        self.query = self._is_query_ok(query, False)
        self.context = context
        self.template_name = template_name
        self.template = template
        self.system = system
        self.persistence = self._is_persistence_ok(persistence) if isinstance(persistence, list) else []
        self.multiprompt = bool(self.persistence)
        self.substituted_query = []
        adapter = ManagerAdapters.get_adapter({'adapter': "claude", 'message': self})
        adapter.adapt_query_and_persistence()
        self.user_query_tokens = self._get_user_query_tokens(self.substituted_query)

    def preprocess(self):
        """Given a query and a context it will return the text in the GPT model format.

        :return: List with the messages in the correct format for the model
        """
        user_prompt = Template(self.template["user"])
        if "system" in self.template:
            system_prompt = Template(self.template["system"])
            system_content = system_prompt.safe_substitute(system=self.system)
        else:
            system_content = self.system

        user_content = user_prompt.safe_substitute(query=self.query, context=self.context)
        return self.unitary_persistence() + [
            {'role': 'assistant', 'content': system_content},
            {'role': 'user', 'content': user_content}]


class Claude3Message(Message):
    MODEL_FORMAT = "chatClaude3"

    def __init__(self, query: str, template: dict, template_name: str = "system_query_v", context: str = "",
                 system='I am a helpful assistant', persistence=()):
        """Chat object. It is used for models that admit persitance as an input such as gpt3.5 or gpt4.

        :param query: Question made.
        :param template: Template used to build the prompt.
        :param template_name: Template Name used to build the prompt.
        :param context: Context used to answer the question.
        :param system:  openai model system configuration
        :param persistence: List containing the conversation history
        """
        super().__init__()
        self.query = self._is_vision_query_ok(query)
        if context != "" and isinstance(self.query, list):
            raise PrintableGenaiError(400, "Context param not allowed in vision models")
        self.context = context
        self.template_name = template_name
        self.template = template
        self.system = system
        self.persistence = self._is_persistence_ok(persistence, True) if isinstance(persistence, list) else []
        self.multiprompt = bool(self.persistence)
        self.substituted_query = []
        adapter = ManagerAdapters.get_adapter({'adapter': "claude", 'message': self})
        adapter.adapt_query_and_persistence()
        self.user_query_tokens = self._get_user_query_tokens(self.substituted_query)

    def _is_vision_query_ok(self, query):
        """Given a query it will check if the format is correct.

        :param query: Question made.
        :return: Query if its format is correct
        """
        super()._is_query_ok(query)
        if isinstance(query, list):
            for el in query:
                if el.get('type') == 'image':
                    if el.get('detail'):
                        raise PrintableGenaiError(400, "Detail parameter not allowed in Claude vision models")
        return query

    def preprocess(self):
        """Given a query and a context it will return the text in the Bedrock model format.

        :return: List with the messages in the correct format for the model
        """
        if "system" in self.template:
            system_prompt = Template(self.template["system"])
            system_content = system_prompt.safe_substitute(system=self.system)
        else:
            system_content = self.system

        user_content = []
        user_prompt = Template(self.template["user"])
        if isinstance(self.template["user"], str):
            user_content = user_prompt.safe_substitute(query=self.query, context=self.context)
        else:
            for e in self.template["user"]:
                if e == "$query":
                    user_content.extend(self.query)
                else:
                    user_content.append(e)

        return self.unitary_persistence() + [
            {'role': 'assistant', 'content': system_content},
            {'role': 'user', 'content': user_content}]


class Llama3Message(Message):
    MODEL_FORMAT = "chatLlama3"

    def __init__(self, query: str, template: dict, template_name: str = "system_query", context: str = "",
                 system='You are a helpful assistant', functions=None, function_call: str = "none", persistence=()):
        """Chat object. It is used for models that admit persitance as an input such as gpt3.5 or gpt4.

        :param query: Question made.
        :param template: Template used to build the prompt.
        :param template_name: Template Name used to build the prompt.
        :param context: Context used to answer the question.
        :param system:  openai model system configuration
        :param functions: List of functions to be used in the model
        :param function_call: Function call to be used in the model
        :param persistence: List containing the conversation history

        """
        super().__init__()
        self.query = self._is_query_ok(query, False)
        self.context = context
        if functions is None:
            functions = []
        self.template_name = template_name
        self.template = template
        self.system = system
        self.functions = functions
        self.function_call = function_call
        self.persistence = self._is_persistence_ok(persistence) if isinstance(persistence, list) else []
        self.multiprompt = bool(self.persistence)
        self.substituted_query = []
        adapter = ManagerAdapters.get_adapter({'adapter': "base", 'message': self})
        adapter.adapt_query_and_persistence()
        self.user_query_tokens = self._get_user_query_tokens(self.substituted_query)

    def preprocess(self) -> List:
        """Given a query and a context it will return the text in the GPT model format.

        :return: List with the messages in the correct format for the model
        """
        user_prompt = Template(self.template["user"])
        if "system" in self.template:
            system_prompt = Template(self.template["system"])
            system_content = system_prompt.safe_substitute(system=self.system)
        else:
            system_content = self.system

        user_content = user_prompt.safe_substitute(query=self.query, context=self.context)

        return [{'role': 'system', 'content': system_content}] + self.unitary_persistence() + [{'role': 'user', 'content': user_content}]


class ManagerMessages(object):
    MESSAGE_TYPES = [PromptGPT3Message, ChatGPTMessage, ClaudeMessage, DalleMessage, Claude3Message, ChatGPTvMessage, Llama3Message]

    @staticmethod
    def get_message(conf: dict) -> Message:
        """ Method to instantiate the message class: [promptGPT, chatGPT, chatClaude, dalle, chatClaude3, chatGPT-v]

        :param conf: Message configuration. Example:  {"message":"chatClaude3"}
        :return: message object
        """
        for message in ManagerMessages.MESSAGE_TYPES:
            message_type = conf.get('message')
            if message.is_message_type(message_type):
                conf.pop('message')
                return message(**conf)
        raise PrintableGenaiError(400, f"Message type doesnt exist {conf}. "
                         f"Possible values: {ManagerMessages.get_possible_platforms()}")

    @staticmethod
    def get_possible_platforms() -> List:
        """ Method to list the messages: [promptGPT, chatGPT, chatClaude, dalle, chatClaude3, chatGPT-v]

        :param conf: Message configuration. Example:  {"message":"chatClaude3"}
        :return: available messages
        """
        return [message.MODEL_FORMAT for message in ManagerMessages.MESSAGE_TYPES]
