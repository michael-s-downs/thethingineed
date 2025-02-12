### This code is property of the GGAO ###


# Native imports
import os
import json
from typing import Tuple
from multiprocessing import Process, Manager

# Installed imports
from pdfminer.pdfparser import PDFSyntaxError
from pdfminer.pdfdocument import PDFEncryptionError

# Custom imports
from common.deployment_utils import BaseDeployment
from common.genai_controllers import storage_containers, db_dbs, set_queue, set_storage, set_db
from common.genai_controllers import upload_object, download_file
from common.genai_status_control import update_status
from common.genai_json_parser import (
    get_generic,
    get_specific,
    get_document,
    get_exc_info,
    get_dataset_status_key,
    get_project_type,
    get_force_ocr,
    get_languages,
    get_do_cells_text,
    get_do_lines_text,
    get_do_segments,
)
from common.services import (
    PREPROCESS_EXTRACT_SERVICE,
    PREPROCESS_OCR_SERVICE,
    PREPROCESS_END_SERVICE,
    PREPROCESS_TRANSLATION_SERVICE,
    PREPROCESS_SEGMENTATION_SERVICE,
    PREPROCESS_LAYOUT_SERVICE,
)
from common.preprocess.preprocess_extract import extract_text, get_num_pages, extract_images_conditional, EXTENSIONS
from common.utils import remove_local_files
from common.status_codes import (
    ERROR,
    EXTRACTED_DOCUMENT,
)
from common.error_messages import (
    PARSING_PARAMETERS_ERROR,
    GETTING_DOCUMENTS_PARAMS_ERROR,
    GETTING_LINES_AND_CELLS_ERROR,
    DOWNLOADING_FILES_ERROR,
    GETTING_NUM_PAGES_ERROR,
    EXTRACTING_IMAGES_AND_TEXT_ERROR,
)


class PreprocessExtractDeployment(BaseDeployment):
    def __init__(self):
        """ Creates the deployment"""
        super().__init__()
        set_queue(self.Q_IN)
        set_storage(storage_containers)
        set_db(db_dbs)

    @property
    def must_continue(self) -> bool:
        """ True if the output should be sent to next step """
        return True

    @property
    def service_name(self) -> str:
        """ Service name.
        The name must be the same as the AWS SQS queue name without the Q_ identifier.
        Example: Q_TRAIN_GPU - train_gpu
        It can be in lowercase but must have the same chars.
        The endpoint for sync deployments will be the same as the service name. """
        return PREPROCESS_EXTRACT_SERVICE

    @property
    def max_num_queue(self) -> int:
        """ Max number of messages to read from queue at once """
        return 1

    def process(self, json_input: dict) -> Tuple[bool, dict, str]:
        """ Main function. Return if the output must be written to next step, the output to write and the next step.
        :return: Tuple[bool, dict, str]
        """
        self.logger.debug(f"Data entry: {json_input}")
        message = json_input
        filename = ""
        next_service = PREPROCESS_END_SERVICE
        msg = json.dumps({'status': ERROR, 'msg': "Error while extracting text and images"})
        redis_status = db_dbs['status']
        dataset_status_key = get_dataset_status_key(json_input=json_input)

        try:
            try:
                generic = get_generic(json_input)
                specific = get_specific(json_input)
            except KeyError:
                self.logger.error("[Process] Error parsing JSON. No generic and specific configuration", exc_info=get_exc_info())
                raise Exception(PARSING_PARAMETERS_ERROR)

            try:
                origin = storage_containers['origin']
                workspace = storage_containers['workspace']
            except KeyError:
                self.logger.error(f"[Process {dataset_status_key}] Error parsing JSON. No origins defined", exc_info=get_exc_info())
                raise Exception(PARSING_PARAMETERS_ERROR)

            try:
                document = get_document(specific=specific)
                filename = document['filename']
                folder_file = os.path.splitext(filename)[0] if os.path.splitext(filename)[1][1:] in EXTENSIONS else filename
            except KeyError:
                self.logger.error(f"[Process {dataset_status_key}] Error parsing JSON. No document defined", exc_info=get_exc_info())
                raise Exception(GETTING_DOCUMENTS_PARAMS_ERROR)

            try:
                project_type = get_project_type(generic=generic)
                self.logger.debug(f"Project Type: {project_type}.")
            except KeyError:
                self.logger.error(f"[Process {dataset_status_key}] Error getting project type", exc_info=get_exc_info())
                raise Exception(PARSING_PARAMETERS_ERROR)

            try:
                force_ocr = get_force_ocr(generic=generic)
            except KeyError:
                self.logger.error(f"[Process {dataset_status_key}] Error getting force_ocr", exc_info=get_exc_info())
                raise Exception(PARSING_PARAMETERS_ERROR)

            try:
                languages = get_languages(generic=generic)
            except KeyError:
                self.logger.debug(f"[Process {dataset_status_key}] Error getting languages", exc_info=get_exc_info())
                raise Exception(PARSING_PARAMETERS_ERROR)

            try:
                do_cells_text = get_do_cells_text(generic=generic)
            except KeyError:
                self.logger.error(f"[Process {dataset_status_key}] Error getting params to extract boxes", exc_info=get_exc_info())
                raise Exception(GETTING_LINES_AND_CELLS_ERROR)

            try:
                do_lines_text = get_do_lines_text(generic=generic)
            except KeyError:
                self.logger.error(f"[Process {dataset_status_key}] Error getting params to extract boxes", exc_info=get_exc_info())
                raise Exception(GETTING_LINES_AND_CELLS_ERROR)

            try:
                do_segments = get_do_segments(generic=generic)
            except KeyError:
                self.logger.error(f"[Process {dataset_status_key}] Error getting params for segmentation", exc_info=get_exc_info())
                raise Exception(GETTING_LINES_AND_CELLS_ERROR)

            # Downloading file
            self.logger.info(f"Downloading file: '{filename}'")
            try:
                # Download document
                download_file(origin, (filename, filename))
            except Exception:
                self.logger.error(f"[Process {dataset_status_key}] Error downloading file", exc_info=get_exc_info())
                raise Exception(DOWNLOADING_FILES_ERROR)

            # Get number of pages of the doc
            self.logger.info(f"Getting number of pages to {filename}.")
            try:
                try:
                    num_pags = get_num_pages(filename, generic['preprocess_conf']['page_limit'])
                except PDFSyntaxError:
                    # Some PDFs are corrupted and they have headers that PDFMiner can't process.
                    # Remove them by truncating the document
                    with open(filename, "rb") as f:
                        d = f.read()
                    with open(filename, "wb") as f:
                        # Truncate the document, make it start by %PDF key
                        f.write(d[d.index(b"%PDF"):])
                    # Retry to get number of pages
                    num_pags = get_num_pages(filename, generic['preprocess_conf']['page_limit'])
                self.logger.info(f"Number of pages to get {num_pags}.")
            except (PDFSyntaxError, ValueError):
                self.logger.error(f"[Process {dataset_status_key}] Error getting number of pages because not are metadata", exc_info=get_exc_info())
                raise Exception(GETTING_NUM_PAGES_ERROR)
            except PDFEncryptionError:
                self.logger.error(f"[Process {dataset_status_key}] Error getting number of pages because document have password", exc_info=get_exc_info())
                raise Exception(GETTING_NUM_PAGES_ERROR)
            except Exception:
                self.logger.error(f"[Process {dataset_status_key}] Error getting number of pages", exc_info=get_exc_info())
                raise Exception(GETTING_NUM_PAGES_ERROR)

            # Extract cells and text from PDF if possible
            is_text_project = project_type == "text"
            text_extracted = False
            if generic.get('preprocess_conf', {}).get('ocr_conf',{}).get('llm_ocr_conf', {}).get('query') and force_ocr:
                self.logger.info("Force OCR is enabled, text extraction will be skipped.")
                files_extracted = {'lang': "default"}
            else:
                self.logger.info(f"Extracting text from file: {filename}")
                files_extracted = {'lang': "", 'text': "", 'extraction': {}, 'boxes': [], 'cells': [], 'lines': []}

                try:
                    process_timeout = 5  # Minutes
                    return_dict = Manager().dict({'lang': "", 'text': "", 'extraction': {}, 'boxes': [], 'cells': [], 'lines': []})
                    p = Process(target=extract_text, args=(filename, num_pags, generic, specific, do_cells_text, do_lines_text, return_dict))
                    p.start()
                    p.join(process_timeout * 60)  # Timeout
                    if p.is_alive():
                        self.logger.info(f"Extract process exceeded {process_timeout}min, terminating...")
                        p.terminate()

                    files_extracted.update(return_dict)

                    text_extracted = files_extracted['text'] != ""
                    self.logger.info(f"Text extracted: '{text_extracted}'\tLanguage extracted: '{files_extracted['lang']}'\t Numbers of pages: '{num_pags}'.")
                except Exception:
                    self.logger.warning("Error while extracting text or language. It is possible this is not an error and it just have to be processed with OCR.", exc_info=get_exc_info())

            path_IRStorage_cells = ""
            path_IRStorage_txt = ""
            if not force_ocr and text_extracted: # To not upload files if OCR is forced (only language extraction is needed)
                # Save text    
                self.logger.info("Uploading files of text.")
                try:
                    folder_file_txt = folder_file + ".txt"
                    for key in files_extracted['extraction']:
                        if key != "text":
                            path_IRStorage = os.path.join(specific['path_text'], "txt", folder_file, "pags", f"{os.path.basename(folder_file)}_{key}.txt")
                            upload_object(workspace, files_extracted.get('extraction', {})[key], path_IRStorage)
                        else:
                            path_IRStorage = os.path.join(specific['path_text'], "txt", folder_file, f"{os.path.basename(folder_file)}.txt")
                            upload_object(workspace, files_extracted.get('extraction', {})[key], path_IRStorage)
                    if files_extracted['text']:
                        path_IRStorage_txt = os.path.join(specific['path_txt'], folder_file_txt)
                        upload_object(workspace, files_extracted['text'], path_IRStorage_txt)
                except Exception:
                    self.logger.warning(f"[Process {dataset_status_key}] Error uploading texts.", exc_info=get_exc_info())

                # Save cells
                self.logger.info("Uploading files of cells, boxes and lines.")
                try:
                    path_IRStorage_cells = os.path.join(specific['path_cells'], "txt", folder_file)
                    if files_extracted['boxes']:
                        filename_blocks = os.path.join(path_IRStorage_cells, f"{os.path.basename(folder_file)}_paragraphs.txt")
                        upload_object(workspace, json.dumps(files_extracted['boxes']), filename_blocks)
                    if files_extracted['cells']:
                        filename_cells = os.path.join(path_IRStorage_cells, f"{os.path.basename(folder_file)}_words.txt")
                        upload_object(workspace, json.dumps(files_extracted['cells']), filename_cells)
                    if files_extracted['lines']:
                        filename_lines = os.path.join(path_IRStorage_cells, f"{os.path.basename(folder_file)}_lines.txt")
                        upload_object(workspace, json.dumps(files_extracted['lines']), filename_lines)
                except Exception:
                    self.logger.warning(f"[Process {dataset_status_key}] Error uploading cells.", exc_info=get_exc_info())

            # Extract images
            self.logger.info("Checking if necesary to extract images.")
            images = []
            try:
                if num_pags <= 100 and is_text_project:
                    if not text_extracted or force_ocr or do_lines_text:
                        self.logger.info(f"Extract images because: Not text extracted: '{not text_extracted}' or force OCR: '{force_ocr}' or extract lines: '{do_lines_text}'.")
                        images = extract_images_conditional(generic, specific, workspace, filename, folder_file)
                    else:
                        self.logger.info("Not necessary to extract images because text are extracted without OCR.")
                elif num_pags > 100 and is_text_project:
                    if force_ocr or do_lines_text:
                        self.logger.info(f"Extract images because: Force OCR: '{force_ocr}' or extract lines: '{do_lines_text}'.")
                        images = extract_images_conditional(generic, specific, workspace, filename, folder_file)
                    else:
                        self.logger.info("Not necessary to extract images because force ocr and extract lines are not necessary.")
                elif not is_text_project:
                    self.logger.info("Extract images because is project type different to text.")
                    images = extract_images_conditional(generic, specific, workspace, filename, folder_file)
                else:
                    self.logger.info("Not necessary to extract images.")
            except Exception:
                self.logger.warning(f"[Process {dataset_status_key}] Error extracting images.", exc_info=get_exc_info())

            img_extracted = len(images) > 0
            self.logger.info("Uploading message of queue adding info of paths and documents.")
            message['specific'].update({
                'paths': {
                    'images': images,
                    'text': path_IRStorage_txt,
                    'cells': path_IRStorage_cells
                }
            })
            message['specific']['document'].update({
                'n_pags': num_pags,
                'language': files_extracted['lang']
            })

            self.logger.info("Creating message status for update Redis and decide next step.")
            if not text_extracted and not img_extracted:
                self.logger.error(f"[Process {dataset_status_key}] Error while extracting texts and images.", exc_info=get_exc_info())
                raise Exception(EXTRACTING_IMAGES_AND_TEXT_ERROR)

            if not text_extracted or force_ocr:
                msg = json.dumps({
                    'status': EXTRACTED_DOCUMENT,
                    'msg': "No text extracted or force OCR. Sent to ocr"
                })
                next_service = PREPROCESS_OCR_SERVICE
            elif do_lines_text and img_extracted:
                msg = json.dumps({
                    'status': EXTRACTED_DOCUMENT,
                    'msg': "Text extracted. Sent to layout preprocess"
                })
                next_service = PREPROCESS_LAYOUT_SERVICE
            elif do_segments:
                msg = json.dumps({
                    'status': EXTRACTED_DOCUMENT,
                    'msg': "Text extracted. Sent to segmentation"
                })
                next_service = PREPROCESS_SEGMENTATION_SERVICE
            else:
                if files_extracted['lang'] in languages or languages[0] == "*":
                    msg = json.dumps({
                        'status': EXTRACTED_DOCUMENT,
                        'msg': "Text extracted. Sent to preprocess end"
                    })
                else:
                    msg = json.dumps({
                        'status': EXTRACTED_DOCUMENT,
                        'msg': "Text extracted. Language not supported. Sent to translation"
                    })
                    next_service = PREPROCESS_TRANSLATION_SERVICE
        except Exception as ex:
            dataset_status_key = get_dataset_status_key(json_input=json_input)
            next_service = PREPROCESS_END_SERVICE
            self.logger.error(f"[Process {dataset_status_key}] Error in preprocess extract", exc_info=get_exc_info())
            msg = json.dumps({'status': ERROR, 'msg': str(ex)})

        if filename:
            # Remove local files
            self.logger.info("Deleting local files temporary.")
            try:
                os.remove(filename)
                remove_local_files(filename)
            except Exception:
                self.logger.warning(f"[Process {dataset_status_key}] Error while deleting file {filename}.")

        update_status(redis_status, dataset_status_key, msg)
        return self.must_continue, message, next_service


if __name__ == "__main__":
    deploy = PreprocessExtractDeployment()
    deploy.async_deployment()
