### This code is property of the GGAO ###


# Native imports
import os
import importlib
import logging
from typing import Tuple

# Custom imports
from genai_sdk_services.resources.import_user_functions import import_user_functions
from genai_sdk_services.services.queue_service import AWSQueueService, BaseQueueService, AzureServiceBusService, AzureStorageQueueService


class QueueController(object):

    services = [AWSQueueService, AzureStorageQueueService, AzureServiceBusService]
    user_functions_services = []
    origins = {}

    def __init__(self, config: dict = None):
        """ Init the controller

        :param config: (dict) Configuration of the controller
        """
        self.logger = logging.getLogger(__name__)

        if config:
            user_functions = config.get("user_functions", None)
            if user_functions is None or type(user_functions) != list:
                user_functions = []

            for to_be_imported in import_user_functions():
                module = to_be_imported.__module__
                class_ = to_be_imported.__name__
                if class_ in user_functions:
                    globals()[f"{class_}"] = getattr(importlib.import_module(f"{module}"), class_)

            for function in user_functions:
                self.user_functions_services.append(globals()[function])

    def set_credentials(self, origin: Tuple[str, str], url: str = "", credentials: dict = None):
        """ Set credentials to access the queue origins

        :param origin: tuple(str, str) Origin of the data, tuple with the type of the origin (i.e. aws, azure_storage, azure_bus )
                      and the origin (i.e. the name of the queue).
        :param url: (str) URL of the queue only necessary for SQS
        :param credentials: (dict) Credentials of the queue.
        """
        if origin[0] not in self.origins:
            self.origins[origin[0]] = self._get_origin(origin[0])
        self.origins[origin[0]].set_credentials(origin[1], url, credentials)

    def _get_origin(self, origin_type: str) -> BaseQueueService:
        """ Get the service of the origin of the data

        :param origin_type: Type of the queue to get the service of.
        :return: (BaseQueueService) Service to be used
        """
        if (eval(os.getenv('STORAGE_QUEUE', "False")) and origin_type == "azure"):
            origin_type = origin_type + "_storage"

        for origin in self.user_functions_services:
            if origin.check_origin(origin_type):
                return origin()

        for origin in self.services:
            if origin.check_origin(origin_type):
                return origin()

        raise ValueError("Type not supported")

    def read(self, queue: tuple, max_num: int = -1, delete: bool = True) -> Tuple[list, list]:
        """ Read messages from the queue

        :param queue: tuple(str, str)  Queue to read from.
        :param max_num: Max num of messages to read
        :param delete: If True the messages will be deleted from the queue
        :return: (Tuple) Data and entries of the queue
        """
        try:
            if queue[0] not in self.origins:
                self.origins[queue[0]] = self._get_origin(queue[0])
            data = self.origins[queue[0]].read(queue[1], max_num, delete)

            self.logger.debug(f"Controller - Read: {data}")
        except Exception as ex:
            self.logger.exception("Error while reading from queue")
            raise ex

        return data

    def write(self, queue: Tuple[str, str], data: str, group_id: str = "grp1") -> bool:
        """ Write data in queue

        :param queue: tuple(str, str) Queue to write data into.
        :param data: (str) Data to write
        :param group_id: (str) Group Id of the message
        :return: (bool) True or false if the data was written
        """
        self.logger.debug(f"Controller - Writing: {data}")
        try:
            if queue[0] not in self.origins:
                self.origins[queue[0]] = self._get_origin(queue[0])
            response = self.origins[queue[0]].write(queue[1], data, group_id)
        except Exception as ex:
            self.logger.exception("Error while writing in queue")
            raise ex

        return response

    def get_num_in_queue(self, queue: Tuple[str, str]) -> int:
        """ Get the number of messages in queue

        :param queue: tuple(str, str) Queue to get number of messages from.
        :return: (int) Number of messages in queue
        """
        try:
            self.logger.debug("Controller - Getting number of messages in queue")

            if queue[0] not in self.origins:
                self.origins[queue[0]] = self._get_origin(queue[0])
        except Exception as ex:
            self.logger.exception("Error while getting num in queue")
            raise ex

        return self.origins[queue[0]].get_num_in_queue(queue[1])

    def delete_messages(self, queue: Tuple[str, str], entries: list) -> bool:
        """ Delete messages from the queue

        :param queue: <tuple(str, str)>  Queue to delete messages from.
        :param entries: (list) Ids of the messages to delete
        :return: (bool) True or false if the messages were deleted
        """

        try:
            self.logger.debug(f"Controller - Deleting messages from queue. {entries}")
            if queue[0] not in self.origins:
                self.origins[queue[0]] = self._get_origin(queue[0])
            response = self.origins[queue[0]].delete_messages(queue[1], entries)
        except Exception as ex:
            self.logger.exception("Error while deleting messages from queue")
            raise ex

        return response

    def purge(self, queue: Tuple[str, str]):
        """ Purge the queue, delete all messages

        :param queue: tuple(str, str)  Queue to purge (aws)
        """
        try:
            self.logger.debug("Controller - Purging queue...")
            if queue[0] not in self.origins:
                self.origins[queue[0]] = self._get_origin(queue[0])
            self.origins[queue[0]].purge(queue[1])
        except Exception as ex:
            self.logger.exception("Error while purging queue")
            raise ex
