### This code is property of the GGAO ###


# Native imports
import logging
import sys
import os
from typing import Union
from logging.config import dictConfig


FORMATS = {
    0:  "[%(asctime)s] [%(levelname)s] [%(name)s] [%(funcName)s():%(lineno)s]" "[PID:%(process)d TID:%(thread)d] %(message)s",
    1:  "[%(asctime)s] [%(levelname)s] %(message)s"
}


class LoggerHandler:
    logger = None

    def __init__(self, service_name: str, level: Union[int, str] = logging.INFO, disable_existing_loggers: bool = False, format_style: int = 0):
        logger_config = {
            'version': 1,
            'disable_existing_loggers': disable_existing_loggers,
        }
        try:
            log_format = FORMATS[format_style]
        except KeyError: 
            raise ValueError(f"format_style must be one of {list(FORMATS.keys())}.")
        
        logging.basicConfig(format=log_format, stream=sys.stdout)
        dictConfig(logger_config)

        if not disable_existing_loggers:
            # Set up library logs with Warning level
            logging.getLogger('genai_sdk_services.queue_controller').disabled = True
            logging.getLogger('genai_sdk_services.storage').disabled = True
            logging.getLogger('genai_sdk_services.db').disabled = True
            logging.getLogger('genai_sdk_services.files').disabled = True
            logging.getLogger('genai_sdk_services.data_bunch').disabled = True
            logging.getLogger('werkzeug').disabled = True
            logging.getLogger('haystack').disabled = True
            logging.getLogger('elastisearch').disabled = True
            logging.getLogger('elastic_transport.transport').disabled = True
            logging.getLogger('hypercorn.error').disabled = True
            logging.getLogger('azure.core').disabled = True
            logging.getLogger('azure.servicebus').disabled = True
            logging.getLogger('llama_index').disabled = True
            logging.getLogger('asyncio').disabled = True
            logging.getLogger('httpx').disabled = True
            logging.getLogger('openai').disabled = True
            logging.getLogger('httpcore').disabled = True
            logging.getLogger('pdfminer').disabled = True
            os.environ['WERKZEUG_RUN_MAIN'] = 'true'
            logging.getLogger('PIL').setLevel(logging.CRITICAL)
            logging.getLogger('boto').setLevel(logging.CRITICAL)
            logging.getLogger('boto3').setLevel(logging.CRITICAL)
            logging.getLogger('botocore').setLevel(logging.CRITICAL)
            logging.getLogger('s3transfer').setLevel(logging.CRITICAL)
            logging.getLogger('urllib3').setLevel(logging.CRITICAL)
            logging.getLogger('genai_sdk_services').setLevel(logging.CRITICAL)
            logging.getLogger('haystack').setLevel(logging.CRITICAL)
            logging.getLogger('elasticsearch').setLevel(logging.CRITICAL)
            logging.getLogger('elastic_transport.transport').setLevel(logging.CRITICAL)
            logging.getLogger('hypercorn.error').setLevel(logging.CRITICAL)
            logging.getLogger('azure.core').setLevel(logging.CRITICAL)
            logging.getLogger('azure.servicebus').setLevel(logging.CRITICAL)
            logging.getLogger('llama_index').setLevel(logging.CRITICAL)
            logging.getLogger('asyncio').setLevel(logging.CRITICAL)
            logging.getLogger('httpx').setLevel(logging.CRITICAL)
            logging.getLogger('openai').setLevel(logging.CRITICAL)
            logging.getLogger('httpcore').setLevel(logging.CRITICAL)
            logging.getLogger('pdfminer').setLevel(logging.CRITICAL)


        self.logger = logging.getLogger(service_name)
        self.logger.setLevel(level)

# Global vars
logger = LoggerHandler(os.getenv('INTEGRATION_NAME', ""), level=os.environ.get('LOG_LEVEL', "INFO")).logger

# Force debug mode in development tenants
if os.getenv('TENANT', "") in os.getenv('DEBUG_TENANTS', "test, develop, devcore").replace(",", " ").split():
    os.environ['LOG_LEVEL'] = "DEBUG"
    os.environ['STORAGE_DELETE_REQUEST'] = "False"