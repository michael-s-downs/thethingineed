### This code is property of the GGAO ###


# Native imports
import os
from typing import Tuple, Union

# Custom imports
from genai_sdk_services.storage import StorageController
from genai_sdk_services.queue_controller import QueueController


# Global vars
provider = os.getenv('PROVIDER', "aws").lower()
queue_url = os.getenv('INTEGRATION_QUEUE_URL', "").format(integration_name=os.getenv('INTEGRATION_NAME').upper())
queue_delete_on_read = eval(os.getenv('QUEUE_DELETE_ON_READ', "False"))

sc = StorageController({'user_functions': []})
qc = QueueController()

sc.set_credentials((provider, os.getenv('STORAGE_DATA')))
sc.set_credentials((provider, os.getenv('STORAGE_BACKEND')))
qc.set_credentials((provider, queue_url), url=queue_url)


def storage_normalize_path(path: str) -> str:
    """ Normalize slash in ending or root path

    :param path: Folder path of storage
    :return: Folder path normalized
    """
    path = f"{path}/".replace("//", "/")
    path = "" if path == "/" else path
    return path

def storage_validate_container(container: str) -> bool:
    """ Validate if container exist

    :param container: Name of storage container
    :return: True or False if container exists
    """
    try:
        pass #TODO

        valid = True
    except:
        valid = False

    return valid

def storage_validate_folder(path: str, container: str) -> bool:
    """ Validate if folder exists

    :param path: Folder path of storage
    :param container: Name of storage container
    :return: True or False if folder exists
    """
    path = storage_normalize_path(path)

    try:
        if path == "":
            # Avoid to list full container
            valid = storage_validate_container(container)
        else:
            files = sc.list_files((provider, container), prefix=path)
            valid = len(files) > 0
    except:
        valid = False

    return valid

def storage_list_folder(path: str, container: str, recursivity: bool=True, extensions_include: list=[], extensions_exclude: list=[], files_exclude: list=[]) -> list:
    """ List all files of a folder and optionally filter by
    the exclude and include lists of files or extensions

    :param path: Folder path of storage
    :param container: Name of storage container
    :param recursivity: Get files from subfolders
    :param extensions_include: Extensions to include
    :param extensions_exclude: Extensions to exclude
    :param files_exclude: Files to exclude
    :return: Files filtered
    """
    path = storage_normalize_path(path)

    try:
        files = sc.list_files((provider, container), prefix=path)
        files = [file for file in files if not file.endswith("/")]
    except:
        files = []

    files = [file for file in files if file not in files_exclude]
    files = [file for file in files if not (not recursivity and "/" in file.replace(path, ""))]

    files_filtered = []
    for file in files:
        extension = file.split(".")[-1].lower()

        if extensions_include:
            if extension in extensions_include:
                files_filtered.append(file)
        elif extensions_exclude:
            if extension not in extensions_exclude:
                files_filtered.append(file)
        else:
            files_filtered.append(file)

    return files_filtered

def storage_put_file(path: str, file: Union[bytes, str], container: str) -> bool:
    """ Upload from memory a file to storage

    :param path: File path of storage where allocate
    :param file: File content in bytes or string to upload
    :param container: Name of storage container
    :return: True or False if can upload file
    """
    try:
        sc.upload_object((provider, container), file, path)
        valid = True
    except:
        valid = False

    return valid

def storage_upload_file(local_path: str, remote_path: str, container: str) -> bool:
    """ Upload from disk a file to storage

    :param local_path: File path from local disk
    :param remote_path: File path of storage where allocate
    :param container: Name of storage container
    :return: True or False if can upload file
    """
    try:
        result = sc.upload_file((provider, container), local_path, remote_path)
        valid = True if result else False
    except:
        valid = False

    return valid

def storage_get_file(path: str, container: str) -> bytes:
    """ Load in memory a file from storage

    :param path: File path of storage from load
    :param container: Name of storage container
    :return: File content in bytes loaded
    """
    try:
        object = sc.load_file((provider, container), path)
    except:
        object = None

    return object

def storage_download_folder(local_path: str, remote_path: str, container: str) -> bool:
    """ Download files from a path to local with the same structure

    :param local_path: Folder path of storage to download
    :param remote_path: Folder path local to allocate
    :param container: Name of storage container
    :return: True or False if can download folder
    """
    try:
        remote_path = storage_normalize_path(remote_path)
        files = storage_list_folder(remote_path, container=container)

        for file in files:
            if not file.endswith("/"):
                file_relative = local_path + file.replace(remote_path, "")
                file_path = os.path.dirname(file_relative)

                if file_path:
                    os.makedirs(file_path, exist_ok=True)

                file_object = storage_get_file(file, container=container)
                open(file_relative, "wb").write(file_object)

        valid = True
    except:
        valid = False

    return valid

def storage_remove_files(path: str, container: str) -> bool:
    """ Delete from storage file or folder

    :param path: Folder or file path of storage
    :param container: Name of storage container
    :return: True or False if can delete folder or file
    """
    try:
        files = sc.list_files((provider, container), prefix=path)
        sc.delete_files((provider, container), files)

        valid = True
    except:
        valid = False

    return valid

def queue_write_message(message: dict, queue: str = queue_url) -> bool:
    """ Write message to queue within specific group ID

    :param message: Message to write
    :param group_id: Group ID
    :return: True or False if can write message
    """
    try:
        response = qc.write((provider, queue), message)
        status = response.get('ResponseMetadata', {}).get('HTTPStatusCode', 0) == 200 if provider == "aws" else response
    except:
        status = False

    return status

def queue_read_messages(max_num: int = 1, delete: bool = True, queue: str = queue_url) -> Tuple[list, list]:
    """ Read messages from queue

    :param max_num: Max numbers of messages to read
    :param delete: If True the messages will be deleted from the queue
    :return: Messages and metadata of messages
    """
    try:
        messages, messages_metadata = qc.read((provider, queue), max_num, delete)
        messages = [] if not messages else messages # Library return None when len=0
    except:
        messages, messages_metadata = [], []
        
    return messages, messages_metadata

def queue_delete_message(messages_metadata: list, queue: str = queue_url) -> bool:
    """ Delete messages from queue

    :param messages_metadata: List of dictionaries with id and receipt handle of queue
    :return: True or False if can delete messages
    """
    try:
        response = qc.delete_messages((provider, queue), messages_metadata)
        status = True if response is not False else False # Library return None
    except:
        status = False

    return status
