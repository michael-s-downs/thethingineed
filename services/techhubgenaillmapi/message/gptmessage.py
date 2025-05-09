
# Native imports
from typing import List
from string import Template

# Local imports
from messages import Message
from adapters import ManagerAdapters
from common.errors.genaierrors import PrintableGenaiError

DEFAULT_SYSTEM_MSG = 'You are a helpful assistant'
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

class ChatGPTOMiniMessage(Message):
    MODEL_FORMAT = "chatGPT-o1-mini"

    def __init__(self, query: list, template: dict, template_name: str = "system_query", context: str = "",
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

        return [{'role': 'assistant', 'content': system_content}] + self.unitary_persistence() + [{'role': 'user', 'content': user_content}]