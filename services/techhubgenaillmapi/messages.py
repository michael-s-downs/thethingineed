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

DEFAULT_SYSTEM_MSG = 'You are a helpful assistant'


class Message(ABC):
    MODEL_FORMAT = "Message"

    def __init__(self):
        """Message object"""
        logger_handler = LoggerHandler(GENAI_LLM_MESSAGES, level=os.environ.get('LOG_LEVEL', "INFO"))
        self.logger = logger_handler.logger

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
        self.query = query
        self.template = template
        self.template_name = template_name
        self.persistence = persistence if isinstance(persistence, list) else []
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
                 system=DEFAULT_SYSTEM_MSG, functions=None, function_call: str = "none", persistence=()):
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
        self.query = query
        self.context = context
        if functions is None:
            functions = []
        self.template_name = template_name
        self.template = template
        self.system = system
        self.functions = functions
        self.function_call = function_call
        self.persistence = persistence if isinstance(persistence, list) else []
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
                 system=DEFAULT_SYSTEM_MSG, functions=None, function_call: str = "none", persistence=(), max_img_size_mb=20.00):
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
        self.query = query
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
        self.persistence = persistence if isinstance(persistence, list) else []
        self.multiprompt = bool(self.persistence)
        self.substituted_query = []
        adapter = ManagerAdapters.get_adapter({'adapter': "gpt4v", 'message': self, 'max_img_size_mb': max_img_size_mb})
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
                 system=DEFAULT_SYSTEM_MSG, persistence=()):
        """Chat object. It is used for models that admit persitance as an input such as gpt3.5 or gpt4.

        :param query: Question made.
        :param template: Template used to build the prompt.
        :param template_name: Template Name used to build the prompt.
        :param context: Context used to answer the question.
        :param system:  openai model system configuration
        :param persistence: List containing the conversation history
        """
        super().__init__()
        self.query = query
        self.context = context
        self.template_name = template_name
        self.template = template
        self.system = system
        self.persistence = persistence if isinstance(persistence, list) else []
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
    MODEL_FORMAT = "chatClaude-v"

    def __init__(self, query: str, template: dict, template_name: str = "system_query_v", context: str = "",
                 system='I am a helpful assistant', persistence=(), max_img_size_mb=5.00):
        """Chat object. It is used for models that admit persitance as an input such as gpt3.5 or gpt4.

        :param query: Question made.
        :param template: Template used to build the prompt.
        :param template_name: Template Name used to build the prompt.
        :param context: Context used to answer the question.
        :param system:  openai model system configuration
        :param persistence: List containing the conversation history
        """
        super().__init__()
        self.query = query
        if context != "" and isinstance(self.query, list):
            raise PrintableGenaiError(400, "Context param not allowed in vision models")
        self.context = context
        self.template_name = template_name
        self.template = template
        self.system = system
        self.persistence = persistence if isinstance(persistence, list) else []
        self.multiprompt = bool(self.persistence)
        self.substituted_query = []
        adapter = ManagerAdapters.get_adapter({'adapter': "claude", 'message': self, 'max_img_size_mb': max_img_size_mb})
        adapter.adapt_query_and_persistence()
        self.user_query_tokens = self._get_user_query_tokens(self.substituted_query)

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


class NovaMessage(Message):
    MODEL_FORMAT = "chatNova"

    def __init__(self, query: str, template: dict, template_name: str = "system_query", context: str = "",
                 system=DEFAULT_SYSTEM_MSG, persistence=()):
        """Chat object. It is used for models that admit persitance as an input such as gpt3.5 or gpt4.

        :param query: Question made.
        :param template: Template used to build the prompt.
        :param template_name: Template Name used to build the prompt.
        :param context: Context used to answer the question.
        :param system:  openai model system configuration
        :param persistence: List containing the conversation history
        """
        super().__init__()   
        self.query = query
        self.context = context
        self.template_name = template_name
        self.template = template
        self.system = system
        self.persistence = persistence if isinstance(persistence, list) else []
        self.multiprompt = bool(self.persistence)
        self.substituted_query = []
        adapter = ManagerAdapters.get_adapter({'adapter': "nova", 'message': self})
        adapter.adapt_query_and_persistence()
        self.user_query_tokens = self._get_user_query_tokens(self.substituted_query)

    def unitary_persistence(self) -> List:
        """Given a persistence it will return a list with the messages in a unitary format.

        :return: List with the messages in a unitary format
        """
        unitary_persistence = []
        for pair in self.persistence:
            for message in pair:
                if isinstance(message['content'], str):
                    message['content'] = [{"text": message['content']}]
                unitary_persistence.append(message)
        return unitary_persistence


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
            {'role': 'assistant', 'content': [{"text": system_content}]},
            {'role': 'user', 'content': [{"text": user_content}]}]

class NovaVMessage(Message):
    MODEL_FORMAT = "chatNova-v"

    def __init__(self, query: str, template: dict, template_name: str = "system_query_v", context: str = "",
                 system='I am a helpful assistant', persistence=(), max_img_size_mb=20.00):
        """Chat object. It is used for models that admit persitance as an input such as gpt3.5 or gpt4.

        :param query: Question made.
        :param template: Template used to build the prompt.
        :param template_name: Template Name used to build the prompt.
        :param context: Context used to answer the question.
        :param system:  openai model system configuration
        :param persistence: List containing the conversation history
        """
        super().__init__() # super to Message to avoid claude3 format query and persistence adaptation  
        self.query = query
        if context != "" and isinstance(self.query, list):
            raise PrintableGenaiError(400, "Context param not allowed in vision models")
        self.context = context
        self.template_name = template_name
        self.template = template
        self.system = system
        self.persistence = persistence if isinstance(persistence, list) else []
        self.multiprompt = bool(self.persistence)
        self.substituted_query = []
        adapter = ManagerAdapters.get_adapter({'adapter': "nova", 'message': self, 'max_img_size_mb': max_img_size_mb})
        adapter.adapt_query_and_persistence()
        self.user_query_tokens = self._get_user_query_tokens(self.substituted_query)
    
    def unitary_persistence(self) -> List:
        """Given a persistence it will return a list with the messages in a unitary format.

        :return: List with the messages in a unitary format
        """
        unitary_persistence = []
        for pair in self.persistence:
            for message in pair:
                if isinstance(message['content'], str):
                    message['content'] = [{"text": message['content']}]
                unitary_persistence.append(message)
        return unitary_persistence


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

        if isinstance(user_content, str):
            user_content = [{"text": user_content}]

        return self.unitary_persistence() + [
            {'role': 'assistant', 'content': [{"text": system_content}]},
            {'role': 'user', 'content': user_content}]
        
class Llama3Message(Message):
    MODEL_FORMAT = "chatLlama3"

    def __init__(self, query: str, template: dict, template_name: str = "system_query", context: str = "",
                 system=DEFAULT_SYSTEM_MSG, functions=None, function_call: str = "none", persistence=()):
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
        self.query = query
        self.context = context
        if functions is None:
            functions = []
        self.template_name = template_name
        self.template = template
        self.system = system
        self.functions = functions
        self.function_call = function_call
        self.persistence = persistence if isinstance(persistence, list) else []
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
    MESSAGE_TYPES = [ChatGPTMessage, ClaudeMessage, DalleMessage, Claude3Message, ChatGPTvMessage, Llama3Message, NovaMessage, NovaVMessage]

    @staticmethod
    def get_message(conf: dict) -> Message:
        """ Method to instantiate the message class: [chatGPT, chatClaude, dalle, chatClaude-v, chatGPT-v, chatNova, chatNova-v]

        :param conf: Message configuration. Example:  {"message":"chatClaude-v"}
        :return: message object
        """
        for message in ManagerMessages.MESSAGE_TYPES:
            message_type = conf.get('message')
            if message.is_message_type(message_type):
                conf.pop('message')
                return message(**conf)
        raise PrintableGenaiError(400, f"Message type doesnt exist {conf}. "
                         f"Possible values: {ManagerMessages.get_possible_messages()}")

    @staticmethod
    def get_possible_messages() -> List:
        """ Method to list the messages: [chatGPT, chatClaude, dalle, chatClaude-v, chatGPT-v, chatNova, chatNova-v]

        :param conf: Message configuration. Example:  {"message":"chatClaude-v"}
        :return: available messages
        """
        return [message.MODEL_FORMAT for message in ManagerMessages.MESSAGE_TYPES]
