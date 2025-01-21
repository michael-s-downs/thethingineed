### This code is property of the GGAO ###


# Native imports
import json
import logging
import uuid
import boto3
import os
from typing import Tuple
from abc import ABCMeta, abstractmethod

# Installed imports
from azure.storage.queue import QueueClient
from azure.servicebus import ServiceBusClient, ServiceBusMessage


timeout_connector = os.getenv('QUEUE_TIMEOUT_CONECTOR', 1)  # seconds
timeout_operation = int(os.getenv('QUEUE_TIMEOUT_OPERATION', 0)) or None  # seconds

class SingletonABCMeta(ABCMeta):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances.keys():
            cls._instances[cls] = super(SingletonABCMeta, cls).__call__(*args, **kwargs)

        return cls._instances[cls]


class BaseQueueService(metaclass=SingletonABCMeta):
    __metaclass__ = SingletonABCMeta
    ORIGIN_TYPES = []

    @abstractmethod
    def get_num_in_queue(self, origin):
        """ Get the number of messages in queue

        :param origin: (str)  the name of the queue
        :return: (int) Number of messages in queue
        """
        pass

    @abstractmethod
    def read(self, origin, max_num=-1, delete=True):
        """ Read messages from the queue

        :param origin: (str)  the name of the queue
        :param max_num: Max num of messages to read
        :param delete: If True the messages will be deleted from the queue
        :return: (dict) Data of the message
        """
        pass

    @abstractmethod
    def write(self, origin, data):
        """ Write a message to the queue

        :param origin: (str)  the name of the queue
        :param data: Data to write
        :return: (response) Response of the method
        """
        pass

    @abstractmethod
    def delete_messages(self, origin, entries):
        """ Delete messages from the queue

        :param origin: (str)  the name of the queue
        :param entries: Ids of the messages to delete
        :return: (bool) True if all messages have been deleted successfully
        """
        pass

    @abstractmethod
    def purge(self, origin):
        """ Purge the queue, delete all messages

        :param origin: (str)  the name of the queue
        """
        pass

    @classmethod
    def check_origin(cls, origin_type):
        """ Check if it is a valid origin for the service

        :param origin_type: (str) Type of the queue to get the service of.
        :return: (bool) True if the origin is valid
        """
        return origin_type in cls.ORIGIN_TYPES


class AWSQueueService(BaseQueueService):

    ORIGIN_TYPES = ["aws", "aws_queue"]
    clients = {}
    credentials = {}
    secret_path = os.path.join(os.getenv('SECRETS_PATH', '/secrets'), "aws", "aws.json")
    env_vars = ["AWS_ACCESS_KEY", "AWS_SECRET_KEY", "AWS_REGION_NAME"]

    def __init__(self):
        """ Initializes the service """
        self.logger = logging.getLogger(__name__)

    def set_credentials(self, origin: str, url: str, credentials: dict):
        """ Set the credentials for the service

        :param origin: (str) type of the queue and the name of the queue
        :param url: (str) URL of the queue
        :param credentials: (dict) Credentials to connect to the service
        """
        if origin and origin not in self.credentials:
            if not credentials:
                if os.path.exists(self.secret_path):
                    with open(self.secret_path, "r") as file:
                        credentials = json.load(file)
                elif os.getenv(self.env_vars[0], ""):
                    credentials = {
                        'access_key': os.getenv(self.env_vars[0]),
                        'secret_key': os.getenv(self.env_vars[1]),
                        'region_name': os.getenv(self.env_vars[2])
                    }
                elif eval(os.getenv("AWS_ROLE", "False")):
                    credentials = {}
                else:
                    raise Exception("Credentials not found")

            credentials['url'] = url
            self.credentials[origin] = credentials

    def get_session(self, origin: str):
        """ Get the session of the client

        :param origin: (str) with the type of the queue and the name of the queue
        :return: (boto3.client) Client of the service
        """
        if origin and origin in self.clients:
            return self.clients[origin]

        self.logger.debug("Connection created")
        region_name = self.credentials[origin].get('region_name', "eu-west-1")

        if eval(os.getenv("AWS_ROLE", "False")):
            session = boto3.Session(region_name=region_name)
        else:
            session = boto3.Session(aws_access_key_id=self.credentials[origin]['access_key'], aws_secret_access_key=self.credentials[origin]['secret_key'], region_name=region_name)

        sqs_client = session.client("sqs")

        self.clients[origin] = sqs_client

        return sqs_client

    def get_num_in_queue(self, origin: str) -> int:
        """ Get the number of messages in queue

        :param origin: (str)  the name of the queue
        :return: (int) Number of messages in queue
        """
        url = self.credentials[origin]['url']
        sqs_client = self.get_session(origin)
        return int(sqs_client.get_queue_attributes(QueueUrl=url, AttributeNames=['ApproximateNumberOfMessages'])['Attributes']['ApproximateNumberOfMessages'])

    def read(self, origin: str, max_num: int = 1, delete: bool = True) -> Tuple[list, list]:
        """ Read messages from the queue

        :param origin: (str)  the name of the queue
        :param max_num: (int) Max num of messages to read
        :param delete: (bool) If True the messages will be deleted from the queue
        :return: (tuple) Data of the message
        """
        url = self.credentials[origin]['url']
        sqs_client = self.get_session(origin)

        self.logger.debug("Reading messages from queue")

        if max_num <= 0:
            max_num = 1

        if max_num > 10:
            max_num = 10
            self.logger.debug("Number of messages in queue greater than 10. Setting 10 as max")

        data = []
        entries = None
        self.logger.debug("Receiving messages")
        resp = sqs_client.receive_message(QueueUrl=url, AttributeNames=['All'], MaxNumberOfMessages=max_num, WaitTimeSeconds=20)
        if resp.get("Messages"):
            for msg in resp["Messages"]:
                data.append(json.loads(msg["Body"]))

            entries = [{"Id": msg["MessageId"], "ReceiptHandle": msg["ReceiptHandle"]} for msg in resp["Messages"]]

            if delete:
                self.delete_messages(origin, entries)

        if len(data) == 0:
            data = None

        return data, entries

    def write(self, origin: str, data: str, group_id: str = "grp1") -> dict:
        """ Write a message to the queue

        :param origin: (str)  the name of the queue
        :param data: (str) Data to write
        :param group_id: (str) ID to group messages
        :return: (dict) Response of the method
        """
        url = self.credentials[origin]['url']
        sqs_client = self.get_session(origin)

        self.logger.debug(f"Writing in queue: {data}")
        response = sqs_client.send_message(QueueUrl=url, MessageDeduplicationId=str(uuid.uuid4()), MessageGroupId=group_id, MessageBody=json.dumps(data))

        return response

    def delete_messages(self, origin: str, entries: list) -> bool:
        """ Delete messages from the queue

        :param origin: (str)  the name of the queue
        :param entries: (list) Ids of the messages to delete
        :return: (bool) True if all messages have been deleted successfully
        """
        url = self.credentials[origin]['url']
        sqs_client = self.get_session(origin)

        self.logger.debug(f"Deleting messages from queue: {entries}")
        resp = sqs_client.delete_message_batch(QueueUrl=url, Entries=entries)

        if "Successful" in resp:
            if len(resp['Successful']) != len(entries):
                raise RuntimeError(f"Failed to delete messages: entries={entries!r} resp={resp!r}")

            return True
        else:
            return False

    def purge(self, origin: str):
        """ Purge the queue, delete all messages

        :param origin: (str)  the name of the queue
        """
        url = self.credentials[origin]['url']
        sqs_client = self.get_session(origin)

        self.logger.debug("Purging queue...")
        _ = sqs_client.purge_queue(QueueUrl=url)


class AzureServiceBusService(BaseQueueService):
    ORIGIN_TYPES = ["azure", "azure_bus"]
    clients = {}
    credentials = {}
    receiver = {}
    secret_path = os.path.join(os.getenv('SECRETS_PATH', '/secrets'), "azure", "azure.json")
    env_vars = ["AZ_CONN_STR_QUEUE"]

    def __init__(self):
        """ Initializes the service """
        self.logger = logging.getLogger(__name__)

    def set_credentials(self, origin: str, url: str, credentials: dict):
        """ Set the credentials for the service

        :param origin: (str) type of the queue and the name of the queue
        :param url: (str) URL of the queue
        :param credentials: (dict) Credentials to connect to the service
        """
        if origin and origin not in self.credentials:
            if not credentials:
                if os.path.exists(self.secret_path):
                    with open(self.secret_path, "r") as file:
                        conn_str = json.load(file).get('conn_str_queue', "")
                        credentials = {'conn_str': conn_str}
                elif os.getenv(self.env_vars[0], ""):
                    credentials = {'conn_str': os.getenv(self.env_vars[0])}
                else:
                    raise Exception("Credentials not found")

            self.credentials[origin] = credentials

    def get_session(self, origin: str):
        """ Get the session of the client

        :param origin: (str) with the type of the queue and the name of the queue
        :return: Client of the service
        """
        self.logger.debug("Connection created")

        queue_client = ServiceBusClient.from_connection_string(conn_str=self.credentials[origin]['conn_str'], logging_enable=True)

        return queue_client

    def get_num_in_queue(self, origin: str) -> int:
        """ Get the number of messages in queue

        :param origin: (str) Name of the queue.
        :return: (int) Number of messages in queue
        """
        raise RuntimeError("This method is not implemented")

    def read(self, origin: str, max_num: int = 1, delete: bool = True) -> Tuple[list, list]:
        """ Read messages from the queue

        :param origin: (str) Name of the queue
        :param max_num: (int) Max num of messages to read
        :param delete: (bool) If True the messages will be deleted from the queue
        :return: (tuple) Data of the message
        """
        queue_client = self.get_session(origin)
        receiver = queue_client.get_queue_receiver(origin, socket_timeout=timeout_connector)
        self.receiver[origin] = receiver

        self.logger.debug("Reading messages from queue")

        if max_num <= 0:
            max_num = 1

        if max_num > 10:
            max_num = 10
            self.logger.debug("Number of messages in queue greater than 10. Setting 10 as max")

        data = []

        self.logger.debug("Receiving messages")
        resp = receiver.receive_messages(max_message_count=max_num, max_wait_time=timeout_operation)
        if len(resp) != 0:
            for msg in resp:
                data.append(json.loads(format(msg)))
                if delete:
                    receiver.complete_message(msg)
        if len(data) == 0:
            data = None

        receiver.close()
        queue_client.close()

        return data, resp

    def write(self, origin: str, data: str, group_id: str = "grp1") -> bool:
        """ Write a message to the queue

        :param origin: (str) Name of the queue
        :param data: (str) Data to write
        :param group_id: (str) useless in that case
        :return: (bool) Response of the method
        """
        queue_client = self.get_session(origin)
        sender = queue_client.get_queue_sender(queue_name=origin, socket_timeout=timeout_connector)

        self.logger.debug(f"Writing in queue: {data}")
        try:
            message = ServiceBusMessage(json.dumps(data))
            sender.send_messages(message, timeout=timeout_operation)
        except Exception as ex:
            raise ex
        else:
            return True
        finally:
            sender.close()
            queue_client.close()

    def delete_messages(self, origin: str, entries: list) -> bool:
        """ Delete messages from the queue

        :param origin: (str) Name of the queue
        :param entries: (list) List of ServiceBusReceivedMessage
        :return: (bool) True if all messages have been deleted successfully
        """
        queue_client = self.get_session(origin)
        receiver = queue_client.get_queue_receiver(origin)
        response = True

        try:
            for msg in entries:
                receiver.complete_message(msg)
        except Exception:
            raise RuntimeError("Failed to delete some messages")
        finally:
            receiver.close()

        return response

    def purge(self, origin: str):
        """ Purge the queue, delete all messages

        :param origin: (str) Name of the queue
        """
        raise RuntimeError("This method is not implemented")


class AzureStorageQueueService(BaseQueueService):

    ORIGIN_TYPES = ["azure_storage"]

    clients = {}
    credentials = {}
    secret_path = os.path.join(os.getenv('SECRETS_PATH', '/secrets'), "azure", "azure.json")
    env_vars = ["AZ_CONN_STR_STORAGE"]

    def __init__(self):
        """ Initializes the service """
        self.logger = logging.getLogger(__name__)

    def set_credentials(self, origin: str, url: str, credentials: dict):
        """ Set the credentials for the service

        :param origin: (str) type of the queue and the name of the queue
        :param url: (str) URL of the queue
        :param credentials: (dict) Credentials to connect to the service
        """
        if origin and origin not in self.credentials:
            if not credentials:
                if os.path.exists(self.secret_path):
                    with open(self.secret_path, "r") as file:
                        conn_str = json.load(file).get('conn_str_storage', "")
                        credentials = {'conn_str': conn_str, 'queue_name': url}
                elif os.getenv(self.env_vars[0], ""):
                    credentials = {'conn_str': os.getenv(self.env_vars[0]), 'queue_name': url}
                else:
                    raise Exception("Credentials not found")

            self.credentials[origin] = credentials

    def get_session(self, origin: str):
        """ Get the session of the client

        :param origin: (str) with the type of the queue and the name of the queue
        :return: Client of the service
        """

        self.logger.debug("Connection created")

        queue_client = QueueClient.from_connection_string(conn_str=self.credentials[origin]['conn_str'], queue_name=self.credentials[origin]['queue_name'])

        return queue_client

    def get_num_in_queue(self, origin: str) -> int:
        """ Get the number of messages in queue

        :param origin: (str) Name of the queue
        :return: (int) Number of messages in queue
        """
        queue_client = self.get_session(origin)
        properties = queue_client.get_queue_properties()
        count = properties.approximate_message_count
        queue_client.close()
        return count

    def read(self, origin: str, max_num: int = 1, delete: bool = True) -> Tuple[list, list]:
        """ Read messages from the queue

        :param origin: (str) Name of the queue
        :param max_num: (int) Max num of messages to read
        :param delete: (bool) If True the messages will be deleted from the queue
        :return: (tuple) Data of the message
        """
        queue_client = self.get_session(origin)

        self.logger.debug("Reading messages from queue")

        if max_num <= 0:
            max_num = 1

        if max_num > 10:
            max_num = 10
            self.logger.debug("Number of messages in queue greater than 10. Setting 10 as max")

        data = []
        entries = []
        self.logger.debug("Receiving messages")
        resp = queue_client.receive_messages(max_messages=max_num, visibility_timeout=5, timeout=timeout_operation)

        for msg in resp:
            data.append(json.loads(msg.content))
            entries.append({'Id': msg['id'], 'pop_receipt': msg['pop_receipt']})

        if delete:
            self.delete_messages(origin, entries)

        if len(data) == 0:
            data = None

        queue_client.close()

        return data, entries

    def write(self, origin: str, data: str, group_id: str = "grp1") -> bool:
        """ Write a message to the queue

        :param origin: (str) Name of the queue
        :param data: (str) Data to write
        :param group_id: (str) useless in that case
        :return: (bool) True if the message has been sent
        """
        queue_client = self.get_session(origin)

        self.logger.debug(f"Writing in queue: {data}")
        try:
            queue_client.send_message(content=json.dumps(data), time_to_live=-1, timeout=timeout_operation)
        except Exception as ex:
            raise ex
        else:
            return True
        finally:
            queue_client.close()



    def delete_messages(self, origin: str, entries: list) -> bool:
        """ Delete messages from the queue

        :param origin: (str) Name of the queue.
        :param entries: (list) Ids of the messages to delete
        :return: (bool) True if all messages have been deleted successfully
        """

        queue_client = self.get_session(origin)

        self.logger.debug(f"Deleting messages from queue: {entries}")

        try:
            num_before = self.get_num_in_queue(origin)

            for msg in entries:
                queue_client.delete_message(message=msg['Id'], pop_receipt=msg['pop_receipt'])

            num_after = self.get_num_in_queue(origin)

            if num_before != num_after:
                if num_before-num_after != len(entries):
                    raise RuntimeError("Failed to delete some messages")
                response = True
            else:
                response = False
        except Exception as ex:
            raise RuntimeError("Failed to delete some messages")
        finally:
            queue_client.close()

        return response

    def purge(self, origin: str):
        """ Purge the queue, delete all messages

        :param origin: (str) Name of the queue
        """
        queue_client = self.get_session(origin)

        self.logger.debug("Purging queue...")
        queue_client.clear_messages()
        queue_client.close()