
# Native imports
from typing import List

# Local imports
from common.errors.genaierrors import PrintableGenaiError
from messages import Message
from message.llamamessage import Llama3Message
from message.novamessage import NovaVMessage, NovaMessage
from message.claudemessage import ClaudeMessage, Claude3Message
from message.gptmessage import ChatGPTvMessage, ChatGPTMessage, DalleMessage, ChatGPTOMiniMessage
class ManagerMessages(object):
    MESSAGE_TYPES = [ChatGPTMessage, ClaudeMessage, DalleMessage, Claude3Message, ChatGPTvMessage, ChatGPTOMiniMessage, Llama3Message, NovaMessage, NovaVMessage]

    @staticmethod
    def get_message(conf: dict) -> Message:
        """ Method to instantiate the message class: [chatGPT, chatClaude, dalle, chatClaude-v, chatGPT-v, chatNova, chatNova-v, chatGPT-o]

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
        """ Method to list the messages: [chatGPT, chatClaude, dalle, chatClaude-v, chatGPT-v, chatNova, chatNova-v, chatGPT-o]

        :param conf: Message configuration. Example:  {"message":"chatClaude-v"}
        :return: available messages
        """
        return [message.MODEL_FORMAT for message in ManagerMessages.MESSAGE_TYPES]