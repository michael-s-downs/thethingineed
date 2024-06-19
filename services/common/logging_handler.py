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

        self.logger = logging.getLogger(service_name)
        self.logger.setLevel(level)
