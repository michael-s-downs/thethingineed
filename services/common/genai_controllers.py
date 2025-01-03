### This code is property of the GGAO ###


"""
Functions to use Uhis controllers (uhis_sdk_services)
"""
# Native imports
import datetime
import os
import time
from typing import List, Tuple, Union

# Installed imports
import pandas

# Custom imports
from genai_sdk_services.data_bunch import DataBunchController
from genai_sdk_services.queue_controller import QueueController
from genai_sdk_services.storage import StorageController
from genai_sdk_services.db import DBController
from genai_sdk_services.files import FilesController

# Instantiate controllers
fc = FilesController()
sc = StorageController()
qc = QueueController()
dbc = DBController()
data_bunch_c = DataBunchController()

db_credentials_redis = {
    'status': os.getenv('REDIS_DB_STATUS'),
    'timeout': os.getenv('REDIS_DB_TIMEOUT'),
    'session': os.getenv('REDIS_DB_SESSION'),
    'templates': os.getenv('REDIS_DB_TEMPLATES')
}

# Global variables
provider = os.getenv('PROVIDER', "aws")
bytes_mode = eval(os.getenv('BYTES_MODE', "False"))

storage_containers = {
    'origin': (provider, os.getenv('STORAGE_DATA')),
    'workspace': (provider, os.getenv('STORAGE_BACKEND'))
}

db_dbs = {db: ("redis", value) for db, value in db_credentials_redis.items()}


# Methods db
def set_db(db_provider: dict):
    """ Set credentials to allow the usage of the uhis controller

    :param db_provider: Credentials configuration JSON.
    """
    for origin in db_provider.values():
        dbc.set_credentials(origin)
        data_bunch_c.set_credentials("csv", sc_credentials=None, dbc_credentials=origin)


# Methods queues
def set_queue(queue: tuple):
    """ Set queue.

    Condition: An environment variable must have been set with the URL of the queue.
    The name of the variable must be the same as the second item of the queue tuple.
    Also, AWS credentials environment variables must have been set.

    :param queue: <tuple(str, str)> uhis_sdk_service.QueueController origin. Queue to write to
    """
    url = os.getenv(queue[1], queue[1])
    qc.set_credentials(queue, url)


def write_to_queue(origin_qc: Tuple[str, str], message: dict):
    """ Write message to queue

    :param origin_qc: <tuple(str, str)> uhis_sdk_service.QueueController origin. Queue to write to
    :param message: Message to write
    """
    date = datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d%H%M%S%f')
    qc.write(origin_qc, message, group_id=str(date))


def read_from_queue(origin_qc: Tuple[str, str], max_num: int, delete: bool = False):
    """ Read message from queue

    :param origin_qc: <tuple(str, str)> uhis_sdk_service.QueueController origin. Queue to read from
    :param max_num: Max number of messages to read from queue
    :param delete: True if messages must be deleted after reading
    """
    data, entries = qc.read(origin_qc, max_num=max_num, delete=delete)

    return data, entries


def delete_from_queue(origin: Tuple[str, str], entries: list):
    """ Delete message from queue

    :param origin: <tuple(str, str)> uhis_sdk_service.QueueController origin. Queue to delete from
    :param entries: Ids of the messages to delete
    """
    qc.delete_messages(origin, entries)


# Methods files storages
def set_storage(storage_provider: dict):
    """ Set credentials to allow the usage of the uhis controller

    :param storage_provider: Credentials configuration JSON.
    """
    for origin in storage_provider.values():
        sc.set_credentials(origin)
        data_bunch_c.set_credentials("csv", sc_credentials=origin, dbc_credentials=None)


def check_file(origin: Union[str, List[str]], prefix: str) -> bool:
    """ List files in a Storage

    :param origin: <tuple(str, str)> uhis_sdk_service.StorageController origin
    :param prefix: prefix to filter files
    :return: List of files that start by prefix
    """
    return sc.check_file(origin, prefix)


def get_sizes(origin:  Union[str, List[str]], files: list) -> list:
    """ Get size of files

    :param origin: <tuple(str, str)> uhis_sdk_service.StorageController origin
    :param files: Path to the file to upload the object to
    """
    return sc.get_size_of_files(origin, files)


def list_files(origin: Union[str, List[str]], prefix: str) -> List[str]:
    """ List files in a Storage

    :param origin: <tuple(str, str)> uhis_sdk_service.StorageController origin
    :param prefix: prefix to filter files
    :return: List of files that start by prefix
    """
    return sc.list_files(origin, prefix)


def download_files(origin: Union[str, List[str]], files: list):
    """ Download files

    :param origin: <tuple(str, str)> uhis_sdk_service.StorageController origin
    :param files: <List[tuple(str, str)]> remote_filename and local_filename
    """
    for remotefile, localfile in files:
        sc.download_file(origin, remotefile, localfile)


def download_file(origin: Union[str, List[str]], file: Tuple[str, str]):
    """ Download files

    :param origin: <tuple(str, str)> uhis_sdk_service.StorageController origin
    :param file: <tuple(str, str)> remote_filename and local_filename
    """
    remotefile, localfile = file
    sc.download_file(origin, remotefile, localfile)


def download_directory(origin: Union[str, List[str]], path: str):
    """ Download directory

    :param origin: <tuple(str, str)> uhis_sdk_service.StorageController origin
    :param path: str path of directory
    """
    sc.download_directory(origin, path)


def load_file(origin: Union[str, List[str]], file: str) -> bytes:
    """ Load file from storage

    :param origin: <tuple(str, str)> uhis_sdk_service.StorageController origin
    :param file: File to get content of
    :return: Content of the file
    """
    return sc.load_file(origin, file)


def upload_files(origin: Union[str, List[str]], files: list):
    """ Upload files

    :param origin: <tuple(str, str)> uhis_sdk_service.StorageController origin
    :param files: <tuple(str, str)> remote_filename and local_filename
    """
    for remotefile, localfile in files:
        sc.upload_file(origin, localfile, remotefile)


def upload_object(origin: Union[str, List[str]], obj: Union[bytes, str], file: str):
    """ Upload object to storage

    :param origin: <tuple(str, str)> uhis_sdk_service.StorageController origin
    :param obj: Object to upload
    :param file: Path to the file to upload the object to
    """
    return sc.upload_object(origin, obj, remote_file=file)


def delete_files(origin: Tuple[str, str], files: list):
    """ Delete folder to S3
    :param origin: <tuple(str, str)> uhis_sdk_service.StorageController origin
    :param files: List of paths
    """
    return sc.delete_files(origin, files)


def delete_file(origin: Tuple[str, str], file: str):
    """ Delete file in S3
    :param origin: <tuple(str, str)> uhis_sdk_service.StorageController origin
    :param file: Path in s3
    """
    return sc.delete_files(origin, [file])


def delete_folder(origin: Tuple[str, str], folder: str):
    """ Delete folder in S3

    :param origin: <tuple(str, str)> uhis_sdk_service.StorageController origin
    :param folder: Path in s3

    """
    try:
        files = list_files(origin, folder)
        delete_files(origin, files)
    except Exception:
        raise Exception("Error deleting folder")


# Methods properties files
def get_mimetype(filename: str) -> str:
    """ Get mimetype of file

    :param filename: <str> Name of the file
    :return: <str> Mimetype of the file
    """
    return fc.get_type(filename)


def get_number_pages(filename: str) -> int:
    """ Get number of pages of file

    :param filename: <str> Name of the file
    :return: <int> Number of pages of the file
    """
    return fc.get_number_pages(filename)


def get_texts_from_file(filename: str, laparams: str, numpagini: int, pagelimit: int, do_cells_text: bool, do_lines_text: bool) -> Tuple[dict, list, list, list]:
    """ Get text from file

    :param filename: <str> Name of the file
    :param laparams: <str> Params for pdfminer
    :param numpagini: <int> Number of the first page to extract
    :param pagelimit: <int> Number of pages to extract
    :param do_cells_text: <bool> True if cells must be extracted
    :param do_lines_text: <bool> True if lines must be extracted
    :return: <str> Text of the file
    """
    texts, boxes, words, lines = fc.get_text(filename, laprams=laparams, num_pag_ini=numpagini, page_limit=pagelimit, do_cells=do_cells_text, do_lines=do_lines_text)
    return texts, boxes, words, lines


def get_images_from_file(filename: str, num_pag_ini: int, page_limit: int) -> list:
    """ Get images from file

    :param filename: <str> Name of the file
    :param num_pag_ini: <int> Number of the first page to extract
    :param page_limit: <int> Number of pages to extract
    :return: <list> List of images
    """

    return fc.extract_images(filename, num_pag_ini=num_pag_ini, page_limit=page_limit)


def extract_ocr_files(files: list, origin: str, do_cells_ocr: bool, do_tables: bool, do_lines_ocr: bool, bytes_mode: bool = False, **kwargs) -> Tuple[list, list, list, list, list, list]:
    """ Extract all files with OCR

    :param files: <list> List of files to extract
    :param origin: <str> Ocr to use
    :param do_cells_ocr: <bool> If do cells ocr
    :param do_tables: <bool> If extract tables
    :param do_lines_ocr: <bool> If do lines ocr
    :param bytes_mode: <bool> If use bucket to use text-extract ocr
    :return: <tuple> Files, blocks, paragraphs, words, tables, lines
    """

    texts, cells, paragraphs, words, tables, lines = fc.extract_multiple_text(files, ocr_origin=origin, do_cells=do_cells_ocr, extract_tables=do_tables, do_lines=do_lines_ocr, bytes_mode=bytes_mode, **kwargs)
    return texts, cells, paragraphs, words, tables, lines


# Methods athena resource
def select_athena(origin: Union[str, List[str]], athena_table: str, output_name: str):
    """ Select dataset from athena

    :param origin: <tuple(str, str)> uhis_sdk_service.db.DBController origin
    :param athena_table: <str> Name of the table
    :param output_name: <str> Path where the file will be located
    :return: Csv generated by Athena """
    # Read raw dataset from path_name
    result = dbc.select(origin, ["*"], [athena_table], output=output_name)

    return result


def create_athena(origin_athena: Tuple[str, str], table: str, s3_path: str, table_type: str,
                  n_classes: int = None, n_entities: int = None, n_metadata: int = None):
    """ Create Athena table

    :param origin_athena: <tuple(str, str)> uhis_sdk_service.db.DBController origin
    :param table: <str> Name of the table
    :param s3_path: <str> Path in S3 where the files are located
    :param table_type: <str> Type of the table to create
    :param n_classes: <int> Number of classes in PREDICTION case
    :param n_entities: <int> Number of entities in EXTRACTION case
    :param n_metadata: <int> Number of metadata fields in IR case
    """
    dbc.create(origin=origin_athena, table=table, s3_path=s3_path, type=table_type, n_classes=n_classes, n_entities=n_entities, n_metadata=n_metadata)


def partition_athena(origin_athena: Tuple[str, str], table: str, s3_path: str):
    """ Create partitions of Athena table

    :param origin_athena: <tuple(str, str)> uhis_sdk_service.db.DBController origin
    :param table: <str> Name of the table
    :param s3_path: <str> Path in S3 where the files are located
    """
    dbc.partition(origin=origin_athena, table=table, s3_path=s3_path)


def execute_query_athena(origin_athena: Tuple[str, str], query: str, output: str) -> dict:
    """ Execute a query to get result to Athena.

    :param origin_athena: <tuple(str, str)> uhis_sdk_service.db.DBController origin
    :param query: Sentence to execute
    :param output: Output name
    :return: Csv created by Athena
    """
    result = dbc.execute_query(origin=origin_athena, query=query, output=output)

    return result


def get_query_athena(origin_athena: Tuple[str, str], sentences: list, athena_tables: list, conditions: list, **kwargs: str) -> str:
    """ Get info by query select with conditions

    :param origin_athena: <tuple(str, str)> uhis_sdk_service.db.DBController origin
    :param sentences: List with sentences
    :param athena_tables: List with athena tables
    :param conditions: Conditions to query
    :param kwargs: More options to add query
    :return: Results to execute sentence of select
    """
    results = dbc.get_query_select(origin_athena, sentences, athena_tables, where=conditions, **kwargs)

    return results


def delete_athena(origin: Tuple[str, str], athena_table: str, output_name: str):
    """ Delete dataset from athena

    :param origin: <tuple(str, str)> uhis_sdk_service.db.DBController origin
    :param athena_table: <str> Name of the table
    :param output_name: <str> Path where the file will be located
    :return: Response of Athena """
    result = dbc.delete(origin, athena_table, output=output_name)

    return result


# Methods read info from file
def get_dataset(origin: Union[str, List[str]], dataset_type: str, path_name: str) -> pandas.DataFrame:
    """ Get a dataset

    :param origin: <tuple> Origin of where the dataset is located
    :param dataset_type: Type of dataset
    :param path_name: Path to the dataset
    :return: pandas.DataFrame with the dataset
    """
    return data_bunch_c.get_dataset(dataset_type, origin, path_name=path_name)
