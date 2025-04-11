### This code is property of the GGAO ###


# Native imports
import re
import os
import time
from typing import List, Union

# Installed imports
from pdfminer.pdfparser import PDFSyntaxError

# Custom imports
from common.genai_controllers import get_mimetype, get_number_pages, get_texts_from_file, get_images_from_file, upload_batch_files_async
from common.genai_json_parser import get_exc_info
from common.logging_handler import LoggerHandler
from common.preprocess.preprocess_utils import *
from common.services import *

# Create logger
logger_handler = LoggerHandler(PREPROCESS_EXTRACT_COMMON, level=os.environ.get('LOG_LEVEL', "INFO"))
logger = logger_handler.logger

# Globals variables
EXTENSIONS = ["jpeg", "png", "svg", "tiff", "ps", "pdf", "txt", "plain", "docx", "pptx", "xls", "xlsx"]
IMAGE_EXTENSIONS = ["jpeg", "png", "svg", "tiff", "ps"]
TEXT_EXTENSIONS = ["pdf", "txt", "plain", "docx", "pptx", "xls", "xlsx"]
N_PAGES_EXTENSIONS = ["pdf", "docx", "pptx", "xls", "xlsx"]


def check_corrupted(text: str, corrupted_symbols_prop: int, corrupted_len_words_prop: int) -> bool:
    """ Check if text is corrupted or scanned

    :param text: Text to check
    :param corrupted_symbols_prop: Param to check
    :param corrupted_len_words_prop: Param to check
    :return: Bool to say if text if corrupted
    """
    words = text.split()
    is_corrupted = False
    try:
        if text == "" or text is None:
            is_corrupted = True
            logger.info("Text is empty.", exc_info=get_exc_info())
        if all(t == "\x0c" for t in text):
            is_corrupted = True
            logger.info("All characters are characters binary.", exc_info=get_exc_info())
        if not len([c for c in text if c.isalnum() or c in ["\n", " "]]) / len(text) > corrupted_symbols_prop:
            is_corrupted = True
            logger.info(f"Percentage of non-alphanumeric characters is greater than {corrupted_symbols_prop*100}% without taking into account spaces and line breaks.", exc_info=get_exc_info())
        if not len([w for w in words if 2 <= len(w) <= 15 or w == "|"]) / len(words) > corrupted_len_words_prop:
            is_corrupted = True
            logger.info(f"Percentage of words different from 2 and 15 letters is greater than {corrupted_len_words_prop*100}%.", exc_info=get_exc_info())

        if (text.lower().strip().startswith('scanned by') or "camscanner" in text.lower()) and len(words) < 5 * max(1, text.lower().count("camscanner")):
            is_corrupted = True
            logger.info("Text is a document scanner.", exc_info=get_exc_info())
    except:
        logger.error("Error checking if text is corrupted.", exc_info=get_exc_info())

    return is_corrupted


def get_num_pages(filename: str, page_limit: int) -> int:
    """ Get number of pages of the document. In case it is not a PDF return 1

    :param filename: Filename of the document
    :param page_limit: Limit of pages to process
    :return: int - Number of pages
    """
    file_extension = get_mimetype(filename)

    if file_extension in N_PAGES_EXTENSIONS:
        num_pags = get_number_pages(filename)

        if num_pags > page_limit > 0:
            num_pags = page_limit
    else:
        num_pags = 1

    return num_pags

def extract_text(file: str, num_pags: int, generic: dict, specific: dict, do_cells_text: bool = True, do_lines_text: bool = False, return_dict: dict = {}):
    """ Extract text of the document

    :param file: Filename of the document to extract text from
    :param num_pags: Number of pages of the document
    :param generic: Generic configuration of genai processes
    :param specific: Specific configuration of genai processes
    :param do_cells_text: True or False if need extract boxes
    :param do_lines_text: True or False if need extract lines
    :param return_dict: Dictionary to return the result
    :return: Tuple[str, str, dict, List, List] - Tuple with:
    language, text, text per page, list of boxes, list of cells
    """
    empty_result = {'lang': "", 'text': "", 'extraction': {}, 'boxes': [], 'cells': [], 'lines': []}
    try:
        file_extension = get_mimetype(file)
        logger.info(f"Extracting text from {file} with extension '{file_extension}'.")

        allowed_file = file_extension in TEXT_EXTENSIONS
        logger.info(f"File is allowed: '{allowed_file}'.")

        if allowed_file:
            laparams = generic['project_conf']['laparams']
            numpagini = generic['preprocess_conf']['num_pag_ini']
            pagelimit = generic['preprocess_conf']['page_limit']
            corrupted_symbols_prop = generic['preprocess_conf'].get('corrupt_th_chars', 0.5)
            corrupted_len_words_prop = generic['preprocess_conf'].get('corrupt_th_words', 0.6)

            # Extract text
            try:
                extraction, boxes, cells, lines = get_texts_from_file(file, laparams, numpagini, pagelimit, do_cells_text, do_lines_text)
            except PDFSyntaxError:
                with open(file, "rb") as f:
                    d = f.read()
                with open(file, "wb") as f:
                    f.write(d[d.index(b"%PDF"):])
                extraction, boxes, cells, lines = get_texts_from_file(file, laparams, numpagini, pagelimit, do_cells_text, do_lines_text)

            text = extraction.get("text", "")
            if type(text) == bytes:
                text = text.decode()

            text = clean_text(text)
            logger.info(f"Text extractting by uhis-sdk-services: '{bool(text)}'.")

            try:
                lang = get_language(text)
                if not lang:
                    lang = "default"
            except:
                lang = "default"
            logger.info(f"Language detect is '{lang}'.")

            if lang in ["ja", "zh", "zh-cn", "zh-tw"] and lang != "default":
                corrupted_len_words_prop = 0

            text_corrupted = check_corrupted(text, corrupted_symbols_prop, corrupted_len_words_prop)
            logger.info(f"Text extract is corrupted: '{text_corrupted}'.")
            if not text_corrupted:
                process_type = generic['project_conf']['process_type']

                if process_type == "ir_index" or process_type == "preprocess":
                    if process_type == "ir_index" or "metadata" in specific.get('document', {}):
                        metadata = specific['document'].get('metadata', {})
                        text = format_indexing_metadata(text, file, num_pags, metadata)
                    
                    return_dict['lang'] = lang
                    return_dict['text'] = text
                    return_dict['extraction'] = extraction
                    return_dict['boxes'] = boxes
                    return_dict['cells'] = cells
                    return_dict['lines'] = lines
                else:
                    logger.warning(f"Process type '{process_type}' is not directly supported. Treating as standard text extraction.")
                    return_dict['lang'] = lang
                    return_dict['text'] = text
                    return_dict['extraction'] = extraction
                    return_dict['boxes'] = boxes
                    return_dict['cells'] = cells
                    return_dict['lines'] = lines
            else:
                return_dict.update(empty_result)
    except Exception:
        logger.error("Error in extract_text function", exc_info=get_exc_info())
        return_dict.update(empty_result)

def extract_images(filename: str, generic: dict) -> List[dict]:
    """ Extract images of the document

    :param filename: Filename of the document to extract images from
    :param generic: Generic configuration of genai processes
    :return:
    """
    try:
        images = get_images_from_file(filename, generic['preprocess_conf']['num_pag_ini'], generic['preprocess_conf']['page_limit'])

    except:
        logger.warning(f"Error extracting images in uhis-sdk-services from {filename}.", exc_info=get_exc_info())
        images = []

    if not images:
        images = []

    return images


def clean_text(text: str) -> str:
    """ Clean text

    :param text: Text to clean
    :return: Text cleaned
    """
    text = re.sub(" ?\(cid:[0-9]+\)", "", text)
    return text


def extract_images_conditional(generic: dict, specific: dict, workspace: Union[str, List[str]], filename: str, folder_file: str) -> list:
    """ Extract and upload images

    :param generic: Generic configuration of genai processes
    :param specific: Specific configuration of genai processes
    :param workspace: Tuple with the workspace and the project
    :param filename: Filename of the document to extract images from
    :param folder_file: Folder of the file
    """
    logger.info(f"Extract and uploaded images.")
    i_time = time.time()
    images = extract_images(filename, generic)

    files_to_upload = []
    remote_directory = "/".join([specific['path_img'], "pags"])

    # Upload image to s3 and remove it from dict
    for image in images:
        file_img = os.path.basename(image['filename'])
        remote_file = "/".join([remote_directory, file_img])

        files_to_upload.append(image['filename'])

        image['filename'] = remote_file

    if files_to_upload:
        upload_batch_files_async(workspace, files_to_upload, remote_directory)

    logger.debug(f"End to extract and uploaded images. Time: {round(time.time() - i_time, 2)}.")

    return images
