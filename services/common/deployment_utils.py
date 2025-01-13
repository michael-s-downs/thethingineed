### This code is property of the GGAO ###


# Native imports
import os
import re
import json
import time
import requests
import warnings
from typing import Any
from datetime import datetime
from abc import ABC, abstractmethod

# Custom imports
from common.genai_controllers import set_queue, write_to_queue, read_from_queue, delete_from_queue, provider
from common.genai_json_parser import *
from common.graceful_killer import GracefulKiller
from common.logging_handler import LoggerHandler
from common.utils import convert_service_to_queue
from common.errors.genaierrors import PrintableGenaiError, GenaiError


warnings.simplefilter('ignore')


class BaseDeployment(ABC):
    def __init__(self):
        """ Creates the deployment"""
        if os.getenv('TENANT', "") in os.getenv('DEBUG_TENANTS', "develop, test").replace(",", " ").split():
            os.environ['LOG_LEVEL'] = "DEBUG"

        logger_handler = LoggerHandler(self.service_name, level=os.environ.get('LOG_LEVEL', "INFO"))
        self.logger = logger_handler.logger
        self.logger.info(f"---- Launching service ({os.getenv('LOG_LEVEL')} MODE)")
        self.Q_IN = (provider, convert_service_to_queue(self.service_name, provider))
        self.killer = GracefulKiller()

        self.senders = []

    @property
    def must_continue(self) -> bool:
        """ True if the output should be sent to next step """
        return False

    @property
    @abstractmethod
    def service_name(self):
        """ Service name.
        The name must be the same as the AWS SQS queue name without the Q_ identifier.
        Example: Q_TRAIN_GPU - train_gpu
        It can be in lowercase but must have the same chars.
        The endpoint for sync deployments will be the same as the service name. """
        pass

    @property
    @abstractmethod
    def max_num_queue(self):
        """ Max number of messages to read from queue at once """
        return 1

    @abstractmethod
    def process(self, json_input: GenaiInput) -> Tuple[bool, dict, str]:
        """ Main function """
        raise NotImplementedError(f"Process not implemented yet for {self.service_name}.")

    def send_any_message(self, url: str, message: dict) -> bool:
        """ Send message via queue or API

        :param url: URL to send message
        :param message: JSON message to send
        :return: True or False if send ok
        """
        valid = True

        # Detect if URL is a queue: if not start with http or https, or start with https://sqs, or contain --q- or --Q-
        is_queue = re.match(r"(^(?!.*https?://))|https://sqs.*|.*--[qQ]-.*", url)
        self.logger.debug(f"Is queue: '{bool(is_queue)}'.")

        if is_queue:
            try:
                queue = (provider, url)

                if queue not in self.senders:
                    set_queue(queue)
                    self.senders.append(queue)
                    self.logger.debug(f"List queues: {self.senders}.")

                self.logger.debug(f"Sending message via queue: {message}.")
                write_to_queue(queue, message)
                self.logger.info(f"Message sent via queue to: '{url}'.")
            except:
                valid = False
                self.logger.error(f"Unable to send message via queue to: '{url}'.", exc_info=get_exc_info())
        else:
            try:
                self.logger.debug(f"Sending message via API: {message}.")
                requests.post(url, json=message)
                self.logger.info(f"Message sent via API to: '{url}'.")
            except:
                valid = False
                self.logger.error(f"Unable to send message via API to: '{url}'.", exc_info=get_exc_info())

        return valid

    @staticmethod
    def generate_tracking_message(request_json: dict, service_name: str, tracking_type: str) -> dict:
        """ Add tracking step to pipeline

        :param request_json: Request JSON with all information
        :param service_name: Service name to add step to pipeline
        :param tracking_type: Tracking type INPUT or OUTPUT
        :return: Request JSON with all information
        """
        tracking_request = request_json.setdefault('tracking', {})
        pipeline = tracking_request.setdefault('pipeline', [])

        # Avoid to repeat step
        last_step = pipeline[-1] if pipeline else {}
        new_step = {'ts': round(datetime.now().timestamp(), 3), 'step': service_name.upper(), 'type': tracking_type}

        # Update ts if already inserted (some services do it inside process)
        if last_step and last_step['step'] == new_step['step'] and last_step['type'] == new_step['type']:
            last_step.update(new_step)
        else:
            pipeline.append(new_step)

        return request_json

    def send_tracking_message(self, request_json: dict, service_name: str, tracking_type: str) -> dict:
        """ Add tracking step to pipeline and send tracking message if enabled

        :param request_json: Request JSON with all information
        :param service_name: Service name to add step to pipeline
        :param tracking_type: Tracking type INPUT or OUTPUT
        :return: Request JSON with all information
        """
        url = os.getenv(f'TRACKING_{tracking_type}_URL', "")
        request_json = self.generate_tracking_message(request_json, service_name, tracking_type)

        if url:
            self.logger.info(f"Sending tracking {tracking_type} message.")
            self.send_any_message(url, request_json['tracking'])

        return request_json

    def report_api(self, count: int, dataset_status_key: str, url: str, resource: str, process_id: str, reporting_type: str = "PAGS"):
        """ Report number of pages to Genai API Gateway

        :param count: Number of pages to report
        :param dataset_status_key: Id of process
        :param url: Url to report
        :param resource: Resource to API
        :param process_id: Id of process
        :param reporting_type: Type of report
        """
        try:
            report_json = {
                'Resource': resource,
                'Count': count
            }
            requests.post(url, json=report_json)
            self.logger.info(f"{process_id} REPORTED {count} {reporting_type} TO API.")
        except Exception:
            self.logger.debug("Error reporting to API.", exc_info=get_exc_info())
            self.logger.error(f"[Process {dataset_status_key}] Error reporting to API.", exc_info=get_exc_info())

    def propagate_queue_message_input(self, raw_input: dict) -> Tuple[dict, dict]:
        """ Get request JSON from bigger message if is inside a defined key

        :param raw_input: Request JSON or request JSON inside bigger message
        :return: Raw input and request JSON with all information
        """
        input_key = os.getenv('JSON_KEY_INPUT', "")

        if input_key:
            self.logger.debug(f"Extracting input from key '{input_key}'")
            json_input = raw_input.pop(input_key, raw_input)
        else:
            json_input = raw_input

        return raw_input, json_input

    def propagate_queue_message_output(self, raw_input: dict, json_output: dict) -> dict:
        """ Return result JSON in bigger message if must be inside a defined key

        :param raw_input: Request JSON or request JSON inside bigger message
        :param json_output: Result JSON
        :return: Result JSON or result JSON inside bigger message
        """
        output_key = os.getenv('JSON_KEY_OUTPUT', "")

        if output_key:
            self.logger.debug(f"Inserting output in key '{output_key}'")
            raw_input[output_key] = json_output
            raw_output = raw_input
        else:
            raw_output = json_output
            raw_output['tracking'] = raw_input.get('tracking', {})

        return raw_output

    def async_deployment(self):
        """ Deploy service in async way. Configure queue. Must exist queue with service name."""
        while not self.killer.kill_now:
            try:
                # Reading from queue
                data, entries = read_from_queue(self.Q_IN, max_num=self.max_num_queue, delete=eval(os.getenv('QUEUE_DELETE_ON_READ', "False")))
                if data is not None and entries is not None:
                    for dat, entry in zip(data, entries):
                        try:
                            s_time = time.time()

                            dat = self.send_tracking_message(dat, self.service_name, "INPUT")
                            raw_input, dat = self.propagate_queue_message_input(dat)

                            dataset_status_key = get_dataset_status_key(json_input=dat)
                            self.logger.info(f"[Process {dataset_status_key}] Request received")

                            must_continue, output, next_service = self.process(dat)  # Process data

                            output = self.propagate_queue_message_output(raw_input, output)
                            output = self.send_tracking_message(output, self.service_name, "OUTPUT")

                            if must_continue:
                                # Async mode - Convert next_service to queue
                                next_queue_name = convert_service_to_queue(next_service, provider)
                                next_queue = (provider, next_queue_name)
                                set_queue(next_queue)
                                write_to_queue(next_queue, output)

                            self.logger.info(f"[Process {dataset_status_key}] Request finished")

                            # Delete message from queue
                            if not eval(os.getenv('QUEUE_DELETE_ON_READ', "False")):
                                delete_from_queue(self.Q_IN, entries)

                            document = get_document(dat)
                            file = document.get('filename', "No filename")

                            # Track time taken to extract
                            self.logger.info(f"Document: {file} Time: {time.time() - s_time}.")
                        except Exception:
                            self.logger.exception(f"Exception for {dat}.", exc_info=get_exc_info())
                            if not eval(os.getenv('QUEUE_DELETE_ON_READ', "False")):
                                delete_from_queue(self.Q_IN, entries)
            except TypeError:
                self.logger.debug("Waiting messages.", exc_info=get_exc_info())

    def sync_deployment(self, dat: GenaiInput) -> Tuple[str, Union[int, Any]]:
        """ Deploy service in a sync way. """
        s_time = time.time()

        dataset_status_key = get_dataset_status_key(json_input=dat)
        self.logger.info(f"[Process {dataset_status_key}] Request received.")

        # Process data
        output = ""
        error_message = ""
        try:
            #dat = self.send_tracking_message(dat, self.service_name, "INPUT")
            must_continue, output, next_service = self.process(dat)  # Process data
            #output = self.send_tracking_message(output, self.service_name, "OUTPUT")

            if output.get('status_code', 200) != 200:
                # For deployments with personalized errors
                status_code = output.get('status_code', 500)
                error_message = output.get('error_message', "Error processing.")
                output = {}
            else:
                status_code = 200
        except PrintableGenaiError as ex:
            self.logger.error(ex.message)
            self.logger.error(f"[Process {dataset_status_key}] Error while processing.", exc_info=get_exc_info())
            error_message = str(ex)
            status_code = PrintableGenaiError(ex.status_code, ex.message).status_code
        except GenaiError as ex:
            self.logger.error(f"[Process {dataset_status_key}] Error while processing.", exc_info=get_exc_info())
            error_message = str(ex)
            status_code = PrintableGenaiError(ex.status_code, ex.message).status_code
        except KeyError:
            self.logger.error(f"[Process {dataset_status_key}] Error while processing. Error parsing input JSON.", exc_info=get_exc_info())
            error_message = "Error parsing input JSON."
            status_code = 400
        except Exception as ex:
            self.logger.error(f"[Process {dataset_status_key}] Error while processing.", exc_info=get_exc_info())
            error_message = str(ex)
            status_code = 500

        # Delete message from queue
        self.logger.info(f"[Process {dataset_status_key}] Request finished.")
        try:
            document = get_document(dat)
            file = document['filename']
        except KeyError:
            file = "None"
        # Track time
        self.logger.info(f"Document: {file} Time: {time.time() - s_time}.")

        response = {
            'status': "finished" if status_code == 200 else "error",
            'result': output,
            'status_code': status_code
        }
        if error_message:
            response['error_message'] = error_message
        return json.dumps(response), status_code

    def cron_deployment(self, time_sleep: int = 1):
        """ Deploy service in a cron way.

        :param time_sleep: Time to wait between each process
        """
        while not self.killer.kill_now:
            try:
                self.process({})
                time.sleep(time_sleep*60)
            except Exception:
                self.logger.exception("Error while processing.", exc_info=get_exc_info())

    def call_back_deployment(self, response: dict) -> Tuple[str, Union[int, Any]]:
        """ Deploy service in a call back way.

        :param response: Response from previous service
        """
        self.logger.info("Request received")

        try:
            output = self.process(response)

            self.logger.debug(f"CALLBACK:\n {json.dumps(output, indent=4)}")
            status_code = 200
        except Exception:
            self.logger.exception("Error while processing", exc_info=get_exc_info())
            status_code = 500

        self.logger.info("Request finished")

        r_dict = {'status': "finished" if status_code == 200 else "error", 'result': response, 'status_code': status_code}
        return json.dumps(r_dict), status_code
