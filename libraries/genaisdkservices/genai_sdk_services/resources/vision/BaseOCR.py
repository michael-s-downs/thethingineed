### This code is property of the GGAO ###


# Native imports
import os
import json
import re
from typing import Tuple
from abc import ABC, abstractmethod
from functools import partial

# Installed import
import boto3
import imageio
import numpy as np
from google.oauth2 import service_account
from genai_sdk_services.resources.vision.ocr2visionfeatures import runOCR, get_blocks_cells
from genai_sdk_services.resources.vision.utils_vision import translate
from genai_sdk_services.storage import StorageController
from pytesseract import Output, image_to_data
from azure.core.credentials import AzureKeyCredential
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import AnalyzeResult


class BaseOCR(ABC):
    ORIGIN_TYPE = "BASE"

    @abstractmethod
    def run_ocr(self, credentials, files, **kwargs):
        raise NotImplementedError(f"Method runOCR not implemented yet for {self.ORIGIN_TYPE}")

    @classmethod
    def check_origin(cls, origin_type):
        """ Check if it is a valid type for the file

        :param origin_type: Type to check
        :return: (bool) True if the type is valid
        """
        return origin_type == cls.ORIGIN_TYPE


class CloudVision(BaseOCR):
    ORIGIN_TYPE = "google-ocr"
    credentials = {}
    secret_path = os.path.join(os.getenv('SECRETS_PATH', '/secrets'), "google", "google_ocr.json")
    env_vars = ["GOOGLE_OCR_PATH"]

    def _set_credentials(self, credentials: dict):
        """ Set the credentials for the OCR

        :param credentials: (dict) Credentials to set
        """
        if not self.credentials:
            if not credentials:
                if os.path.exists(self.secret_path):
                    with open(self.secret_path, "r") as file:
                        credentials = json.load(file)
                elif os.getenv(self.env_vars[0], ""):
                    with open(os.getenv(self.env_vars[0]), "r") as file:
                        credentials = json.load(file)
                else:
                    raise Exception("Credentials not found")

            self.credentials = credentials

    def run_ocr(self, credentials: dict, files: list, **kwargs: dict) -> Tuple[list, list, list, list, list, list]:
        """ Run the OCR

        :param credentials: (dict) Credentials to use
        :param files: (list) List of files to run the OCR
        :param kwargs: (dict) Extra parameters
        :return: (tuple) Lists with several files with information of file
        """
        self._set_credentials(credentials)

        ocr_credential = service_account.Credentials.from_service_account_info(self.credentials)

        # Running the OCR
        ocr_infos = runOCR(ocr_credential, files)

        documents_blocks = []
        documents_paragraphs = []
        documents_words = []
        documents_lines = []
        documents_tables = []
        if kwargs.get('do_cells', True):
            documents_blocks, documents_paragraphs, documents_words, documents_lines = get_blocks_cells(ocr_infos)

        returning_texts = []
        for ocr_info in ocr_infos:
            text = ocr_info[0].text
            returning_texts.append(text)

        return returning_texts, documents_blocks, documents_paragraphs, documents_words, documents_tables, documents_lines


class Textract(BaseOCR):
    ORIGIN_TYPE = "aws-ocr"
    EMPTY_RELATIONSHIP = [{'Type': None, 'Ids': []}]
    credentials = {}
    secret_path = os.path.join(os.getenv('SECRETS_PATH', '/secrets'), "aws", "aws.json")
    env_vars = ["AWS_ACCESS_KEY", "AWS_SECRET_KEY", "AWS_REGION_NAME"]

    def __init__(self):
        self.sc = StorageController()

    def _get_destiny(self, file: str, bucket: str) -> Tuple[int, int]:
        """ Get the destiny of the file

        :param file: (str) File to get the destiny
        :param bucket: (str) Bucket to get the destiny
        :return: (tuple) Tuple with the width and height of the image
        """
        img = imageio.read(self.sc.load_file(("aws", bucket), file)).get_data(0)
        return img.shape[1], img.shape[0]

    @staticmethod
    def _get_destiny_bytes(file: str) -> Tuple[int, int]:
        """ Get the destiny of the file in bytes mode

        :param file: (str) File to get the destiny
        :return: (tuple) Tuple with the width and height of the image
        """
        try:
            with open(file, "rb") as f:
                data = f.read()
            img = imageio.read(data).get_data(0)
        except:
            raise Exception("Can´t set destiny bytes")

        return img.shape[1], img.shape[0]

    def _set_credentials(self, credentials: dict):
        """ Set the credentials for the OCR

        :param credentials: (dict) Credentials to set
        """
        if not self.credentials:
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
                else:
                    raise Exception("Credentials not found")

            self.credentials = credentials

    def run_ocr(self, credentials: dict, files: list, **kwargs: dict) -> Tuple[list, list, list, list, list, list]:
        """ Run the OCR

        :param credentials: (dict) Credentials to use
        :param files: (list) List of files to run the OCR
        :param kwargs: (dict) Extra parameters
        :return: (tuple) Lists with several files with information of file
        """
        self._set_credentials(credentials)

        bytes_mode = kwargs.get('bytes_mode', False)

        region_name = self.credentials.get('region_name', "eu-west-1")

        client = boto3.client(
            "textract",
            aws_access_key_id=self.credentials['access_key'],
            aws_secret_access_key=self.credentials['secret_key'],
            region_name=region_name
        )

        returning_texts = []
        returning_blocks = []
        returning_paragraphs = []
        words_blocks = []
        lines_blocks = []
        returning_tables = []

        if not bytes_mode:
            if "bucket" not in self.credentials:
                raise ValueError("Bucket not defined")
            bucket = self.credentials['bucket']
            self.sc.set_credentials(("aws", bucket))

            for file in files:
                if kwargs.get('extract_tables', False):
                    response = client.analyze_document(
                        Document={
                            'S3Object': {
                                'Bucket': bucket,
                                'Name': file
                            }
                        },
                        FeatureTypes=[
                            'TABLES'
                        ]
                    )
                else:
                    response = client.detect_document_text(
                        Document={
                            'S3Object': {
                                'Bucket': bucket,
                                'Name': file
                            }
                        }
                    )

                blocks = response['Blocks']
                origin_coords = [0, 0]

                for b in blocks:
                    if b['BlockType'] == "PAGE":
                        origin_coords = b['Geometry']['BoundingBox']['Width'], b['Geometry']['BoundingBox']['Height']
                words = [x for x in blocks if x['BlockType'] == "WORD"]
                lines = [x for x in blocks if x['BlockType'] == "LINE"]
                tables = [x for x in blocks if x['BlockType'] == "TABLE"]
                table_cells = [x for x in blocks if x['BlockType'] == "CELL"]
                table_mcells = [x for x in blocks if x['BlockType'] == "MERGED_CELL"]

                # Get cells if wanted
                cells = []
                lines_cells = []
                destiny = None
                if kwargs.get('do_cells', True):
                    destiny = self._get_destiny(file, bucket)

                    for b in words:
                        b_bb = b['Geometry']['BoundingBox']
                        r0 = b_bb['Top']
                        c0 = b_bb['Left']
                        r1 = r0 + b_bb['Height']
                        c1 = c0 + b_bb['Width']
                        r0, c0, r1, c1 = translate(r0, c0, r1, c1, origin_coords, destiny)
                        cells.append({
                            'text': b['Text'],
                            'r0': r0,
                            'c0': c0,
                            'r1': r1,
                            'c1': c1,
                            'confidence': b['Confidence'],
                            'page': b.get('Page', 0)
                        })

                if kwargs.get('do_lines', False):
                    destiny = self._get_destiny(file, bucket)
                    rotation = -1
                    for l in lines:
                        l_bb = l['Geometry']['BoundingBox']
                        r0 = l_bb['Top']
                        c0 = l_bb['Left']
                        r1 = r0 + l_bb['Height']
                        c1 = c0 + l_bb['Width']
                        r0, c0, r1, c1 = translate(r0, c0, r1, c1, origin_coords, destiny)
                        if len(l['Relationships'][0]['Ids']) >= 2:
                            first_word = {}
                            last_word = {}
                            for word in words:
                                if word["Id"] == l['Relationships'][0]["Ids"][0]:
                                    first_word = word
                                if word["Id"] == l['Relationships'][0]["Ids"][-1]:
                                    last_word = word
                                if first_word !={} and last_word !={}:
                                    break
                            first_word_bb = first_word['Geometry']['BoundingBox']
                            last_word_bb = last_word['Geometry']['BoundingBox']
                            first_word_center = (
                                np.mean([first_word_bb['Left'], first_word_bb['Left'] + first_word_bb['Width']]),
                                np.mean([first_word_bb['Top'], first_word_bb['Top'] + first_word_bb['Height']]))
                            last_word_center = (
                                np.mean([last_word_bb['Left'], last_word_bb['Left'] + last_word_bb['Width']]),
                                np.mean([last_word_bb['Top'], last_word_bb['Top'] + last_word_bb['Height']]))
                            # upright or upside down
                            if np.abs(first_word_center[1] - last_word_center[1]) < np.abs(r0 - r1):
                                if first_word_center[0] <= last_word_center[0]:  # upright
                                    rotation = 0
                                else:  # updside down
                                    rotation = 180
                            else:  # sideways
                                if first_word_center[1] <= last_word_center[1]:
                                    rotation = 90
                                else:
                                    rotation = 270
                        lines_cells.append({
                            'text': l['Text'],
                            'r0': r0,
                            'c0': c0,
                            'r1': r1,
                            'c1': c1,
                            'x_max': destiny[0],
                            'y_max': destiny[1],
                            'rotation': rotation,
                            'confidence': l['Confidence'],
                            'page': l.get('Page', 0)
                        })
                    for l in lines_cells:
                        l['rotation'] = rotation
                text = "\n".join(l['Text'] for l in lines)
                returning_texts.append(text)
                words_blocks.append([cells])
                lines_blocks.append([lines_cells])

                # Get texts that belong to more than one cell
                merged_texts, merged_coords = {}, {}
                for m_cell in table_mcells:
                    if not destiny:
                        destiny = self._get_destiny(file, bucket)

                    m_text = ''
                    children_ids = [rel['Ids'] for rel in m_cell['Relationships'] if rel['Type'] == 'CHILD'][0]
                    for children_id in children_ids:
                        children = next(filter(lambda x: x['Id'] == children_id, table_cells))
                        children_rel = children.get('Relationships', self.EMPTY_RELATIONSHIP)

                        for rel in children_rel:
                            if rel['Type'] == "CHILD":  # There is only one child
                                word_ids = rel['Ids']
                                for w_id in word_ids:
                                    try:
                                        w = next(filter(lambda x: x['Id'] == w_id, words))
                                        m_text += w['Text'] + ' '
                                    except:
                                        pass

                    # Do a mapping, if texts belongs to more than one category. Add it to the first
                    # and add coordinates to the rest of the appearances.
                    cell_0 = next(filter(lambda x: x['Id'] == children_ids[0], table_cells))
                    for i, children_id in enumerate(children_ids):
                        if i == 0 and m_text:
                            merged_texts[children_id] = m_text
                        else:
                            merged_texts[children_id] = ""
                            merged_coords[children_id] = (cell_0['RowIndex'], cell_0['ColumnIndex'])

                # Get tables adding merged text.
                returning_cells = []
                for t in tables:
                    table_c = []
                    t_child = [rel['Ids'] for rel in t['Relationships'] if rel['Type'] == 'CHILD'][0]
                    t_cells = [x for x in table_cells if x['Id'] in t_child]
                    for c in t_cells:
                        c_bb = c['Geometry']['BoundingBox']
                        r0 = c_bb['Top']
                        c0 = c_bb['Left']
                        r1 = r0 + c_bb['Height']
                        c1 = c0 + c_bb['Width']
                        r0, c0, r1, c1 = translate(r0, c0, r1, c1, origin_coords, destiny)

                        if c['Id'] in merged_texts:
                            text = merged_texts[c['Id']]
                        else:
                            c_children = c.get('Relationships', self.EMPTY_RELATIONSHIP)[0]['Ids']
                            c_words = []
                            for w_id in c_children:
                                try:
                                    c_words.append(next(filter(lambda x: x['Id'] == w_id, words))['Text'])
                                except:
                                    pass
                            text = ' '.join(c_words)

                        table_dict = {
                            'text': text,
                            'r0': r0,
                            'c0': c0,
                            'r1': r1,
                            'c1': c1,
                            'row': c['RowIndex'],
                            'column': c['ColumnIndex'],
                            'confidence': c['Confidence'],
                            'page': c.get('Page', 0)
                        }

                        if c['Id'] in merged_coords.keys():
                            table_dict['text_location'] = merged_coords[c['Id']]

                        table_c.append(table_dict)
                    returning_cells.append(table_c)
                returning_tables.append(returning_cells)
        else:
            for file in files:
                document = {'Bytes': open(file, 'rb').read()}
                if kwargs.get('extract_tables', False):
                    response = client.analyze_document(Document=document, FeatureTypes=['TABLES'])
                else:
                    response = client.detect_document_text(Document=document)

                blocks = response['Blocks']
                origin_coords = [0, 0]

                for b in blocks:
                    if b['BlockType'] == "PAGE":
                        origin_coords = b['Geometry']['BoundingBox']['Width'], b['Geometry']['BoundingBox']['Height']
                words = [x for x in blocks if x['BlockType'] == "WORD"]
                lines = [x for x in blocks if x['BlockType'] == "LINE"]
                tables = [x for x in blocks if x['BlockType'] == "TABLE"]
                table_cells = [x for x in blocks if x['BlockType'] == "CELL"]
                table_mcells = [x for x in blocks if x['BlockType'] == "MERGED_CELL"]

                # Get cells if wanted
                cells = []
                lines_cells = []
                destiny = None
                if kwargs.get('do_cells', True):
                    destiny = self._get_destiny_bytes(file)

                    for b in words:
                        b_bb = b['Geometry']['BoundingBox']
                        r0 = b_bb['Top']
                        c0 = b_bb['Left']
                        r1 = r0 + b_bb['Height']
                        c1 = c0 + b_bb['Width']
                        r0, c0, r1, c1 = translate(r0, c0, r1, c1, origin_coords, destiny)
                        cells.append({
                            'text': b['Text'],
                            'r0': r0,
                            'c0': c0,
                            'r1': r1,
                            'c1': c1,
                            'confidence': b['Confidence'],
                            'page': b.get('Page', 0)
                        })

                if kwargs.get('do_lines', False):
                    destiny = self._get_destiny_bytes(file)
                    rotation = -1
                    for l in lines:
                        l_bb = l['Geometry']['BoundingBox']
                        r0 = l_bb['Top']
                        c0 = l_bb['Left']
                        r1 = r0 + l_bb['Height']
                        c1 = c0 + l_bb['Width']
                        r0, c0, r1, c1 = translate(r0, c0, r1, c1, origin_coords, destiny)
                        if len(l['Relationships'][0]['Ids']) >= 2:
                            first_word = {}
                            last_word = {}
                            for word in words:
                                if word["Id"] == l['Relationships'][0]["Ids"][0]:
                                    first_word = word
                                if word["Id"] == l['Relationships'][0]["Ids"][-1]:
                                    last_word = word
                                if first_word != {} and last_word != {}:
                                    break
                            first_word_bb = first_word['Geometry']['BoundingBox']
                            last_word_bb = last_word['Geometry']['BoundingBox']
                            first_word_center = (
                                np.mean([first_word_bb['Left'], first_word_bb['Left'] + first_word_bb['Width']]),
                                np.mean([first_word_bb['Top'], first_word_bb['Top'] + first_word_bb['Height']]))
                            last_word_center = (
                                np.mean([last_word_bb['Left'], last_word_bb['Left'] + last_word_bb['Width']]),
                                np.mean([last_word_bb['Top'], last_word_bb['Top'] + last_word_bb['Height']]))
                            # upright or upside down
                            if np.abs(first_word_center[1] - last_word_center[1]) < np.abs(r0 - r1):
                                if first_word_center[0] <= last_word_center[0]:  # upright
                                    rotation = 0
                                else:  # updside down
                                    rotation = 180
                            else:  # sideways
                                if first_word_center[1] <= last_word_center[1]:
                                    rotation = 90
                                else:
                                    rotation = 270
                        lines_cells.append({
                            'text': l['Text'],
                            'r0': r0,
                            'c0': c0,
                            'r1': r1,
                            'c1': c1,
                            'x_max': destiny[0],
                            'y_max': destiny[1],
                            'rotation': rotation,
                            'confidence': l['Confidence'],
                            'page': l.get('Page', 0)
                        })
                    for l in lines_cells:
                        l['rotation'] = rotation
                text = "\n".join(l['Text'] for l in lines)
                returning_texts.append(text)
                words_blocks.append([cells])
                lines_blocks.append([lines_cells])

                # Get texts that belong to more than one cell
                merged_texts, merged_coords = {}, {}
                for m_cell in table_mcells:
                    if not destiny:
                        destiny = self._get_destiny_bytes(file)

                    m_text = ''
                    children_ids = [rel['Ids'] for rel in m_cell['Relationships'] if rel['Type'] == 'CHILD'][0]
                    for children_id in children_ids:
                        children = next(filter(lambda x: x['Id'] == children_id, table_cells))
                        children_rel = children.get('Relationships', self.EMPTY_RELATIONSHIP)

                        for rel in children_rel:
                            if rel['Type'] == 'CHILD':  # There is only one child
                                word_ids = rel['Ids']
                                for w_id in word_ids:
                                    try:
                                        w = next(filter(lambda x: x['Id'] == w_id, words))
                                        m_text += w['Text'] + ' '
                                    except:
                                        pass

                    # Do a mapping, if texts belongs to more than one category. Add it to the first
                    # and add coordinates to the rest of the appearances.
                    cell_0 = next(filter(lambda x: x['Id'] == children_ids[0], table_cells))
                    for i, children_id in enumerate(children_ids):
                        if i == 0 and m_text:
                            merged_texts[children_id] = m_text
                        else:
                            merged_texts[children_id] = ""
                            merged_coords[children_id] = (cell_0['RowIndex'], cell_0['ColumnIndex'])

                # Get tables adding merged text.
                returning_cells = []
                for t in tables:
                    table_c = []
                    t_child = [rel['Ids'] for rel in t['Relationships'] if rel['Type'] == 'CHILD'][0]
                    t_cells = [x for x in table_cells if x['Id'] in t_child]
                    for c in t_cells:
                        c_bb = c['Geometry']['BoundingBox']
                        r0 = c_bb['Top']
                        c0 = c_bb['Left']
                        r1 = r0 + c_bb['Height']
                        c1 = c0 + c_bb['Width']
                        r0, c0, r1, c1 = translate(r0, c0, r1, c1, origin_coords, destiny)

                        if c['Id'] in merged_texts:
                            text = merged_texts[c['Id']]
                        else:
                            c_children = c.get('Relationships', self.EMPTY_RELATIONSHIP)[0]['Ids']
                            c_words = []
                            for w_id in c_children:
                                try:
                                    c_words.append(next(filter(lambda x: x['Id'] == w_id, words))['Text'])
                                except:
                                    pass
                            text = ' '.join(c_words)

                        table_dict = {
                            'text': text,
                            'r0': r0,
                            'c0': c0,
                            'r1': r1,
                            'c1': c1,
                            'row': c['RowIndex'],
                            'column': c['ColumnIndex'],
                            'confidence': c['Confidence'],
                            'page': c.get('Page', 0)
                        }

                        if c['Id'] in merged_coords.keys():
                            table_dict['text_location'] = merged_coords[c['Id']]

                        table_c.append(table_dict)
                    returning_cells.append(table_c)
                returning_tables.append(returning_cells)

        return returning_texts, returning_blocks, returning_paragraphs, words_blocks, returning_tables, lines_blocks


class TesseractOCR(BaseOCR):
    ORIGIN_TYPE = "tesseract-ocr"
    credentials = {}

    def __init__(self):
        self.lang_map = {'ca': "cat", 'es': "spa", 'en': "eng"}

    def _get_credentials(self, credentials: dict):
        """Get credentials from the origin

        :param credentials: Credentials of service
        """
        pass

    def run_ocr(self, credentials: dict, files: list, **kwargs: dict) -> Tuple[list, list, list, list, list, list]:
        """ Run the OCR

        :param credentials: (dict) Credentials to use
        :param files: (list) List of files to run the OCR
        :param kwargs: (dict) Extra parameters
        :return: (tuple) Lists with several files with information of file
        """
        returning_texts = []
        returning_blocks = []
        returning_paragraphs = []
        words_blocks = []
        lines_blocks = []
        returning_tables = []

        for index, file in enumerate(files):
            img = imageio.imread(file)

            lang = kwargs.get('language', "eng")  # spa+eng+cat is slower but more accurate
            lang = self.lang_map.get(lang, lang)

            custom_config = fr"-l {lang}"
            result = image_to_data(img, output_type=Output.DICT, config=custom_config)

            cells, aux, text_list = [], [], []
            for left, width, top, height, text, conf in zip(result['left'], result['width'], result['top'],
                                                            result['height'],
                                                            result['text'], result['conf']):
                text = text.strip()
                if text:
                    aux.append(text)
                if not text or conf == -1:  # conf==-1 indicates line break as for the empty texts
                    if aux:
                        text_list.append(" ".join(aux))
                        aux = []
                elif kwargs.get('do_cells', True):
                    cells.append({
                        'text': text,
                        'c0': left,
                        'c1': left + width,
                        'r0': top,
                        'r1': top + height,
                        'confidence': conf,
                        'page': kwargs.get('page', index)
                    })
            if aux:
                text_list.append(" ".join(aux))

            words_blocks.append([cells])
            returning_texts.append("\n".join(text_list))

        return returning_texts, returning_blocks, returning_paragraphs, words_blocks, returning_tables, lines_blocks


class FormRecognizer(BaseOCR):
    ORIGIN_TYPE = "azure-ocr"
    credentials = {}
    secret_path = os.path.join(os.getenv('SECRETS_PATH', '/secrets'), "azure", "azure_ocr.json")
    env_vars = ["AZ_KEY_CREDENTIAL", "AZ_OCR_ENDPOINT"]

    def __init__(self):
        self.sc = StorageController()

    def _get_destiny(self, file: str, bucket: str) -> Tuple[int, int]:
        """ Get the destiny of the file

        :param file: (str) File to get the destiny
        :param bucket: (str) Bucket to get the destiny
        :return: (tuple) Tuple with the width and height of the image
        """
        img = imageio.read(self.sc.load_file(("azure", bucket), file)).get_data(0)
        return img.shape[1], img.shape[0]

    @staticmethod
    def _get_destiny_bytes(file: str) -> Tuple[int, int]:
        """ Get the destiny of the file in bytes mode

        :param file: (str) File to get the destiny
        :return: (tuple) Tuple with the width and height of the image
        """
        try:
            with open(file, "rb") as f:
                data = f.read()
            img = imageio.read(data).get_data(0)
        except:
            raise Exception("Can´t set destiny bytes")

        return img.shape[1], img.shape[0]

    def _set_credentials(self, credentials: dict):
        """ Set the credentials for the OCR

        :param credentials: (dict) Credentials to set
        """
        if not self.credentials:
            if not credentials:
                if os.path.exists(self.secret_path):
                    with open(self.secret_path, "r") as file:
                        credentials = json.load(file)
                elif os.getenv(self.env_vars[0], ""):
                    credentials = {
                        'key_credential': os.getenv(self.env_vars[0]),
                        'endpoint': os.getenv(self.env_vars[1])
                    }
                else:
                    raise Exception("Credentials not found")

        self.credentials = credentials

    @staticmethod
    def parse_dict(d: dict, n_pag: int, origin: tuple, destiny: tuple) -> dict:
        """ Parse cell dictionary to adapt format

        :param d: (dict) Dict to parse
        :param n_pag: (int) Number of the page where the cell has been extracted
        :param origin: (tuple) Width and height of page in original coordinates system
        :param destiny: (tuple) Width and height of page in destiny coordinates system

        :return: (dict) Dictionary with cell information adapted and coordinates translated
        """
        text = d['content']
        x1, y1, x2, y2, x3, y3, x4, y4 = d['polygon']

        r0 = (y1 + y2) / 2
        r1 = (y4 + y3) / 2
        c0 = (x1 + x4) / 2
        c1 = (x2 + x3) / 2
        r0, c0, r1, c1 = translate(r0, c0, r1, c1, origin, destiny)

        parsed_dict = {
            'text': text,
            'r0': r0,
            'c0': c0,
            'r1': r1,
            'c1': c1,
            'page': n_pag
        }
        if 'confidence' in d:
            parsed_dict['confidence'] = d['confidence']

        return parsed_dict

    def parse_result(self, result: AnalyzeResult, n_pag: int, destiny_coords: tuple, **kwargs) -> Tuple[str, list, list, list]:
        """ Parse output of OCR for current file to extract word cells, line cells, paragraph cells and text

        :param result: (dict) Dictionary with OCR output for current file
        :param n_pag: (int) Number of the processed page
        :param destiny_coords: (tuple) Width and height of page in destiny coordinates system

        :return: (tuple) Tuple with parsed text, word cells, line cells, and paragraph cells for current file
        """
        text = result.content

        page = result.pages[0]
        origin_coords = page['width'], page['height']
        partial_parsedict = partial(self.parse_dict, n_pag=n_pag, origin=origin_coords, destiny=destiny_coords)

        words = []
        lines = []
        paragraphs = []
        if kwargs.get('do_cells', True):
            words = page['words']
            words = list(map(partial_parsedict, words))

            paragraphs = result.paragraphs if result.paragraphs else []
            paragraphs = [{**p, **p.pop('boundingRegions')[0]} for p in paragraphs]
            paragraphs = list(map(partial_parsedict, paragraphs))

        if kwargs.get('do_lines', False):
            lines = page['lines']
            lines = list(map(partial_parsedict, lines))

        return text, words, lines, paragraphs

    def run_ocr(self, credentials: dict, files: list, **kwargs: dict) -> Tuple[list, list, list, list, list, list]:
        """ Run the OCR

        :param credentials: (dict) Credentials to use
        :param files: (list) List of files to run the OCR
        :param kwargs: (dict) Extra parameters
        :return: (tuple) Lists with several files with information of file
        """
        self._set_credentials(credentials)

        bytes_mode = kwargs.get('bytes_mode', True)

        client = DocumentIntelligenceClient(credential=AzureKeyCredential(self.credentials['key_credential']), endpoint=self.credentials['endpoint'])

        returning_texts = []
        returning_blocks = []
        returning_paragraphs = []
        words_blocks = []
        lines_blocks = []
        returning_tables = []
        if not bytes_mode:
            bucket = os.getenv("STORAGE_BACKEND", "")
            if not bucket:
                raise ValueError("Bucket not defined")
            self.sc.set_credentials(("azure", bucket))

            for file in files:
                document = {'urlSource': f"https://d2astorage.blob.core.windows.net/{bucket}/{file}"}
                response = client.begin_analyze_document(model_id='prebuilt-read', analyze_request=document)
                result = response.result()
                try:
                    n_pag = int(re.search(r"_pag_(\d+).jpeg$", file).group(1))
                except AttributeError:
                    n_pag = 0
                destiny = self._get_destiny(file, bucket)
                text, words, lines, paragraphs = self.parse_result(result, n_pag, destiny, **kwargs)

                returning_texts.append(text)
                words_blocks.append([words])
                lines_blocks.append([lines])
                returning_paragraphs.append([paragraphs])
        else:
            for file in files:
                document = {'base64Source': open(file, 'rb').read()}
                response = client.begin_analyze_document(model_id='prebuilt-read', analyze_request=document)
                result = response.result()
                try:
                    n_pag = int(re.search(r"_pag_(\d+).jpeg$", file).group(1))
                except AttributeError:
                    n_pag = 0
                destiny = self._get_destiny_bytes(file)
                text, words, lines, paragraphs = self.parse_result(result, n_pag, destiny, **kwargs)

                returning_texts.append(text)
                words_blocks.append([words])
                lines_blocks.append([lines])
                returning_paragraphs.append([paragraphs])

        return returning_texts, returning_blocks, returning_paragraphs, words_blocks, returning_tables, lines_blocks
