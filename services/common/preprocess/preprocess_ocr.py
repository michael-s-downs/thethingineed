### This code is property of the GGAO ###


# Native import
import os
import time
import datetime
import math
import cv2
import itertools
from PIL import Image
from typing import Union

# Installed import
from common.genai_controllers import upload_files, download_files, extract_ocr_files
from common.genai_json_parser import get_exc_info
from common.logging_handler import LoggerHandler
from common.services import *

# Create logger
logger_handler = LoggerHandler(PREPROCESS_OCR_COMMON, level=os.environ.get('LOG_LEVEL', "INFO"))
logger = logger_handler.logger

WAIT_TIME = 0.5


def get_ocr_files(files_to_extract: list, ocr: str, prefix_map: dict, do_cells_ocr: bool = True, do_tables: bool = False, do_lines_ocr: bool = False, bytes_mode: bool = False, **kwargs) -> dict:
    """ Extract text from image files using the OCR.

    :param files_to_extract: List of files to be processed by OCR
    :param ocr: OCR to use
    :param prefix_map: Dictionary with the prefix of the paths
    :param do_cells_ocr: True or False to extract boxes
    :param do_tables: True or False to extract tables
    :param do_lines_ocr: True or False to extract lines
    :param bytes_mode: True or False to use bucket S3 to use text-extract
    :return: Message to be sent over the queue
    """
    logger.info("Extracting files from OCR with library.")
    extract_docs = {}

    try:
        files, cells, paragraphs, words, tables, lines = extract_ocr_files(files_to_extract, ocr, do_cells_ocr, do_tables, do_lines_ocr, bytes_mode, **kwargs)

        extract_docs['text'] = format_path_files(files, prefix_map['images'], prefix_map['text'])
        extract_docs['cells'] = format_path_files(cells, prefix_map['images'], prefix_map['cells'])
        extract_docs['paragraphs'] = format_path_files(paragraphs, prefix_map['images'], prefix_map['cells'])
        extract_docs['words'] = format_path_files(words, prefix_map['images'], prefix_map['cells'])
        extract_docs['tables'] = format_path_files(tables, prefix_map['images'], prefix_map['tables'])
        extract_docs['lines'] = format_path_files(lines, prefix_map['images'], prefix_map['cells'])
    except Exception as ex:
        logger.error("Error extracting files from OCR with library.", exc_info=get_exc_info())
        raise ex

    return extract_docs


def chunk(files: list, sizes: list, max_size: int, max_length: int):
    """ Generate chunks of (files, sizes) following the criteria:
        1. sum(sizes) is not over size_th
        2. len(files) is not over max_length

    :param files: List of files to be chunked
    :param sizes: List of sizes of the files
    :param max_size: Maximum size of all the files in the batch
    :param max_length: Maximum length of the batch
    :yield: (list) List containing the next elements
    """
    batch = []
    logger.info(f"Chunking files with max size {max_size} and max length {max_length}.")
    try:
        for idx, (file, size) in enumerate(zip(files, sizes)):
            if sum([x['size'] for x in batch]) + size > max_size or len(batch) == max_length:
                yield batch
                batch = []
            batch.append({'number': idx, 'filename': file, 'size': size})
        yield batch
    except:
        logger.error("Error chunking files.", exc_info=get_exc_info())


def sum_up_size(sizes: list) -> float:
    """ Get size of batches

    :param sizes: size of batch
    :return Round of sizes
    """
    return round(sum(sizes) / (1024 * 1024), 2)


def insert_at_rate(requests: list, count: int, rate: int, period: int) -> list:
    """ Insert request controller

    :param requests: list request
    :param count: number of batch
    :param rate: number of call per minute
    :param period: seconds to pass by drop older requests
    :return: list with request
    """

    def count_reqs(requests: list) -> int:
        """ Count number or requests

        :param requests: list requests
        :return: number of request
        """
        return sum([reqs for reqs, tstamp in requests])

    def drop_older(requests: list, older_than: int) -> list:
        """ Drop requests old in function of time

        :param requests: list requests
        :param older_than: time in seconds to rest time
        :return: list request of request
        """
        tstamp_window = datetime.datetime.now() + datetime.timedelta(seconds=-older_than)

        return list(itertools.dropwhile(lambda x: x[1] < tstamp_window, requests))

    def insert_requests(requests: list, count: int):
        """ Append to list request

        :param requests: list request
        :param count: number of batch
        """
        requests.append((count, datetime.datetime.now()))

    while count_reqs(requests) + count > rate:
        time.sleep(WAIT_TIME)
        requests = drop_older(requests, period)

    insert_requests(requests, count)

    return requests


def format_path_files(files: list, prefix_image: str, prefix_folder: str, replace_path: str = "") -> list:
    """ Change path of files with structure in storage

    :param files: list of files
    :param prefix_image: prefix of path to image
    :param prefix_folder: prefix of path to folder
    :param replace_path: param to replace
    :return: list of files
    """
    returning_files = []
    for target_local_file in files:
        target_remote_file = target_local_file.replace(prefix_image, prefix_folder + "/ocr").replace(replace_path, "")
        returning_files.append((target_remote_file, target_local_file))

    return returning_files
