
# Native imports
from string import Template

# Local imports
from messages import Message
from adapters import ManagerAdapters
from common.errors.genaierrors import PrintableGenaiError

DEFAULT_SYSTEM_MSG = 'You are a helpful assistant'
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