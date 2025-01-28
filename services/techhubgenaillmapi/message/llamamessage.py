
# Native imports
from typing import List
from string import Template

# Local imports
from messages import Message
from adapters import ManagerAdapters

DEFAULT_SYSTEM_MSG = 'You are a helpful assistant'

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