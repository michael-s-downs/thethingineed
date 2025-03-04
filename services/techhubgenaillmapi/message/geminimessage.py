# Native imports
from typing import List
from string import Template

# Local imports
from messages import Message
from adapters import ManagerAdapters
from common.errors.genaierrors import PrintableGenaiError

DEFAULT_SYSTEM_MSG = 'You are a helpful assistant'
class GeminiMessage(Message):
    MODEL_FORMAT = "chatGemini"

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
        adapter = ManagerAdapters.get_adapter({'adapter': "gemini", 'message': self})
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


class GeminiVMessage(Message):
    MODEL_FORMAT = "chatGemini-v"

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
        super().__init__()  # super to Message to avoid claude3 format query and persistence adaptation
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
        adapter = ManagerAdapters.get_adapter({'adapter': "gemini", 'message': self, 'max_img_size_mb': max_img_size_mb})
        adapter.adapt_query_and_persistence()
        self.user_query_tokens = self._get_user_query_tokens(self.substituted_query)

    @staticmethod
    def _get_user_query_tokens(query: list) -> [int, List]:
        """Given a query it will return the number of tokens

        :param query: Query containing the user content
        :return: Number of tokens
        """
        n_tokens = []
        for message in query:
            if message.get("role") == "user":
                for part in message.get("parts", []):
                    if "text" in part and isinstance(part["text"], dict):
                        n_tokens.append(part["text"].get("n_tokens", 0))
                    elif "inlineData" in part and isinstance(part["inlineData"], dict):
                        n_tokens.append(part["inlineData"].get("n_tokens", 0))

        return n_tokens[0] if len(n_tokens) == 1 else n_tokens

    def unitary_persistence(self) -> List:
        """Given a persistence it will return a list with the messages in a unitary format.

        :return: List with the messages in a unitary format
        """
        unitary_persistence = []
        for pair in self.persistence:
            for message in pair:
                if isinstance(message['content'], str):
                    message['parts'] = [{"text": message['content']}]
                    message.pop('content')
                elif isinstance(message['content'], list):
                    message['parts'] = message['content']
                    message.pop('content')
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
            {'role': 'model', 'parts': [{"text": system_content}]},
            {'role': 'user', 'parts': user_content}]

'''    @staticmethod
    def _get_user_query_tokens(query: list) -> [int, List]:
        """Given a query it will return the number of tokens

        :param query: Query containing the user content
        :return: Number of tokens
        """
        n_tokens = []

        for message in query:
            if message.get("role") == "user":
                for part in message.get("parts", []):
                    # Extract tokens from "text"
                    if "text" in part and isinstance(part["text"], dict):
                        n_tokens.append(part["text"].get("n_tokens", 0))

                    # Extract tokens from "inlineData" if present
                    if "inlineData" in part and isinstance(part["inlineData"], dict):
                        n_tokens.append(part["inlineData"].get("n_tokens", 0))

        return n_tokens[0] if len(n_tokens) == 1 else n_tokens

    def unitary_persistence(self) -> List:
        """Transforms persistence messages to the Gemini format with 'role' and 'parts'."""
        unitary_persistence = []
        for pair in self.persistence:
            for message in pair:
                role = message.get('role', 'user')
                if role == 'assistant':
                    role = 'model'  # Convert 'system' role to 'model' for Gemini compliance
                content = message['content']
                if isinstance(content, str):
                    parts = [{'text': content}]
                else:
                    parts = []
                    for item in content:
                        if item.get('type', "") == 'text':
                            parts.append({'text': item.get('text', "")})
                        elif item.get('type', "") in ['image_url', 'image_b64']:
                            if item['type'] == 'image_url':
                                image_data = {"data": item['image']['url'], "mimeType": "image/png", "type": "image_url"}
                                container = "inlineData"
                            else:
                                image_data = {"data": item['image']['base64'], "mimeType": "image/png", "type": "image_b64"}
                                container = "inlineData"

                            if parts and 'text' in parts[-1]:
                                parts[-1][container] = image_data
                            else:
                                parts.append({container: image_data})
                unitary_persistence.append({
                    'role': role,
                    'parts': parts
                })
                
        return unitary_persistence

    def preprocess(self):
        """Prepares the message in the Gemini API format."""
        contents = []

        if 'system' in self.template:
            system_prompt = Template(self.template['system'])
            system_content = system_prompt.safe_substitute(system=self.system)
            contents.append({
                'role': 'model',
                'parts': [{'text': system_content}]
            })

        user_prompt = Template(self.template['user'])
        if isinstance(self.template['user'], str):
            user_content = user_prompt.safe_substitute(query=self.query, context=self.context)
            parts = [{'text': user_content}]
        else:
            parts = []
            for e in self.template['user']:
                if e == "$query":
                    for q in self.query:
                        if q.get('type', "") == 'text':
                            parts.append({'text': q.get('text', "")})
                        elif q.get('type', "") in ['image_url', 'image_b64']:
                            if q['type'] == 'image_url':
                                image_data = {"data": q['image']['url'], "mimeType": "image/png", "type": "image_url"}
                                container = "inlineData"
                            else:
                                image_data = {"data": q['image']['base64'], "mimeType": "image/png", "type": "image_b64"}
                                container = "inlineData"

                            if parts and 'text' in parts[-1]:
                                parts[-1][container] = image_data
                            else:
                                parts.append({container: image_data})
                else:
                    parts.append({'text': e})

        contents.append({
            'role': 'user',
            'parts': parts
        })

        result = self.unitary_persistence() + contents
        self.substituted_query = result[-2:]
        return result
'''

