### This code is property of the GGAO ###


# Native imports
import importlib
import logging
import os
import asyncio

# Custom imports
from genai_sdk_services.resources.import_user_functions import import_user_functions
from genai_sdk_services.services.storage import S3Service, BaseStorageService, FileShareService, BlobService


class StorageController(object):
    services = [S3Service, FileShareService, BlobService]
    user_functions_services = []
    origins = {}

    def __init__(self, config: dict = None):
        """ Init the controller with the given configuration

        :param config: Configuration of the controller. Can include user functions
        """
        self.logger = logging.getLogger(__name__)

        if config:
            user_functions = config.get('user_functions', None)
            if user_functions is not None:
                if type(user_functions) != list:
                    user_functions = [user_functions]

                ufs = import_user_functions()
                for to_be_imported in ufs:
                    module = to_be_imported.__module__
                    class_ = to_be_imported.__name__
                    if class_ in user_functions:
                        globals()[f"{class_}"] = getattr(importlib.import_module(f"{module}"), class_)

                        self.logger.debug(f"Found  user function {class_}")
                        self.user_functions_services.append(globals()[class_])

    def set_credentials(self, origin: tuple, credentials: dict = None):
        """ Set credentials to access the storage

        :param origin: tuple(str, str) Origin of the data, tuple with the type of the origin
                      (i.e. aws_bucket, azure_blob, azure_fileshare) and the origin (i.e. the name of the bucket).
        :param credentials: Credentials to access the storage
        """
        if origin[0] not in self.origins:
            self.origins[origin[0]] = self._get_origin(origin[0])
        self.origins[origin[0]].set_credentials(origin[1], credentials)

    def _get_origin(self, origin_type: str) -> BaseStorageService:
        """ Get the service of the origin of the data

        :param origin_type: (str) Type of the origin to get the service of.
        :return: <BaseStorageService> Service to be used
        """
        for origin in self.user_functions_services:
            if origin.check_origin(origin_type):
                return origin()

        for origin in self.services:
            if origin.check_origin(origin_type):
                return origin()

        raise ValueError("Type not supported")

    def download_file(self, origin: tuple, remote_file: str, local_file: str = None) -> bool:
        """ Download a file from remote storage into a local file.

        :param origin: <tuple(str, str)> Origin of the data, tuple with the type of the origin
                      (i.e. aws_bucket, azure_blob, azure_fileshare) and the origin (i.e. the name of the bucket).
        :param remote_file: Name of the file in remote storage
        :param local_file: Name of the local file
        :return: (bool) True if file has been downloaded successfully
        """
        try:
            if origin[0] not in self.origins:
                self.origins[origin[0]] = self._get_origin(origin[0])
            if self.check_file(origin, remote_file):
                return self.origins[origin[0]].download_file(origin[1], remote_file, local_file)
            else:
                self.logger.warning("File does not exist: %s" % remote_file)
                return False
        except Exception as ex:
            self.logger.exception("Error while downloading %s" % remote_file)
            raise ex
    

    def download_directory(self, origin: tuple, remote_directory: str, local_directory: str = None, suffix: str = None) -> bool:
        """ Download a directory from remote storage into a local file.

        :param origin: tuple(st, str) Origin of the data, tuple with the type of the origin
                      (i.e. aws_bucket, azure_blob, azure_fileshare) and the origin (i.e. the name of the bucket).
        :param remote_directory: (str) Name of the directory in remote storage.
        :param local_directory: (str) Name of the local directory
        :param suffix: (str) Filter files to download by suffix
        :return: (bool) True if file has been downloaded successfully
        """
        try:
            if origin[0] not in self.origins:
                self.origins[origin[0]] = self._get_origin(origin[0])
            return self.origins[origin[0]].download_directory(
                origin[1], remote_directory, local_directory, suffix)
        except Exception as ex:
            self.logger.exception("Error while downloading %s" % remote_directory)
            raise ex

    def download_directory_async(self, origin: tuple, remote_directory: str, local_directory: str = None, suffix: str = None) -> bool:
        """ Download directory from remote storage into a local file async.

        :param origin: tuple(st, str) Origin of the data, tuple with the type of the origin
                      (i.e. aws_bucket, azure_blob, azure_fileshare) and the origin (i.e. the name of the bucket).
        :param remote_directory: (str) Name of the directory in remote storage.
        :param local_directory: (str) Name of the local directory
        :param suffix: (str) Filter files to download by suffix
        :return: (bool) True if file has been downloaded successfully
        """
        try:
            if origin[0] not in self.origins:
                self.origins[origin[0]] = self._get_origin(origin[0])
            return asyncio.run(self.origins[origin[0]].download_directory_async(
                origin[1], remote_directory, local_directory, suffix))
        except Exception as ex:
            self.logger.exception("Error while downloading %s" % remote_directory)
            raise ex

    def download_batch_files_async(self, origin: tuple, files_list: list, local_directory: str) -> bool:
        """ Download a list of files from remote storage into a local file async.

        :param origin: tuple(st, str) Origin of the data, tuple with the type of the origin
                      (i.e. aws_bucket, azure_blob, azure_fileshare) and the origin (i.e. the name of the bucket).
        :param files_list: (list) List of files to download with the complete dirname.
        :param local_directory: (str) Name of the local directory
        :return: (bool) True if file has been downloaded successfully
        """
        try:
            if origin[0] not in self.origins:
                self.origins[origin[0]] = self._get_origin(origin[0])
            return asyncio.run(self.origins[origin[0]].download_batch_files_async(
                origin[1], files_list, local_directory))
        except Exception as ex:
            self.logger.exception(f"Error while downloading async to {local_directory}")
            raise ex

    def upload_batch_files_async(self, origin: tuple, files_list: list, remote_directory: str) -> bool:
        """ Uploads a list of files to a remote storage async.

        :param origin: tuple(st, str) Origin of the data, tuple with the type of the origin
                      (i.e. aws_bucket, azure_blob, azure_fileshare) and the origin (i.e. the name of the bucket).
        :param files_list: (list) List of files to upload.
        :param remote_directory: (str) Name of the remote directory
        :return: (bool) True if file has been downloaded successfully
        """
        try:
            if origin[0] not in self.origins:
                self.origins[origin[0]] = self._get_origin(origin[0])
            return asyncio.run(self.origins[origin[0]].upload_batch_files_async(
                origin[1], files_list, remote_directory))
        except Exception as ex:
            self.logger.exception(f"Error while uploading async to {remote_directory}")
            raise ex

    def upload_folder_async(self, origin: tuple, local_directory: str, remote_directory: str) -> bool:
        """ Uploads all files from a local folder to a remote storage async.

        :param origin: tuple(st, str) Origin of the data, tuple with the type of the origin
                      (i.e. aws_bucket, azure_blob, azure_fileshare) and the origin (i.e. the name of the bucket).
        :param local_directory: (str) Name of the local directory
        :param remote_directory: (str) Name of the remote directory
        :return: (bool) True if file has been downloaded successfully
        """
        try:
            if origin[0] not in self.origins:
                self.origins[origin[0]] = self._get_origin(origin[0])
            return asyncio.run(self.origins[origin[0]].upload_folder_async(
                origin[1], local_directory, remote_directory))
        except Exception as ex:
            self.logger.exception(f"Error while uploading async to {remote_directory}")
            raise ex

    def upload_object(self, origin: tuple, object_: bytes, remote_file: str) -> bool:
        """ Upload a local file to remote storage

        :param origin: <tuple(str, str)> Origin of the data, tuple with the type of the origin
                      (i.e. aws_bucket, azure_blob, azure_fileshare) and the origin (i.e. the name of the bucket).
        :param object_: (bytes) Object in bytes to upload
        :param remote_file: (str) Name of the remote file to save object in.
        :return: (bool) True if the file has been uploaded successfully
        """
        try:
            if origin[0] not in self.origins:
                self.origins[origin[0]] = self._get_origin(origin[0])
            return self.origins[origin[0]].upload_object(origin[1], object_, remote_file)
        except Exception as ex:
            self.logger.exception("Error while uploading object")
            raise ex

    def upload_file(self, origin: tuple, local_file: str, remote_file: str = None) -> bool:
        """ Upload a local file to remote storage

        :param origin: <tuple(str, str)> Origin of the data, tuple with the type of the origin
                      (i.e. aws_bucket, azure_blob, azure_fileshare) and the origin (i.e. the name of the bucket).
        :param local_file: (str) Name of the local file
        :param remote_file: (str) Name of the remote file
        :return: <bool> True if the file has been uploaded successfully
        """
        if os.path.exists(local_file):
            try:
                if origin[0] not in self.origins:
                    self.origins[origin[0]] = self._get_origin(origin[0])
                return self.origins[origin[0]].upload_file(origin[1], local_file, remote_file)
            except Exception as ex:
                self.logger.exception("Error while uploading %s" % local_file)
                raise ex
        else:
            self.logger.warning("File does not exist: %s" % local_file)
            return False

    def load_file(self, origin: tuple, remote_file: str) -> bytes:
        """ Return an object loaded from remote storage

        :param origin: <tuple(str, str)> Origin of the data, tuple with the type of the origin
                      (i.e. aws_bucket, azure_blob, azure_fileshare) and the origin (i.e. the name of the bucket).
        :param remote_file: (str) File to load
        :return: (bytes) Content of file
        """
        try:
            if origin[0] not in self.origins:
                self.origins[origin[0]] = self._get_origin(origin[0])

            if self.check_file(origin, remote_file):
                return self.origins[origin[0]].load_file(origin[1], remote_file)
            else:
                self.logger.warning("File does not exist: %s" % remote_file)
                return b""
        except Exception as ex:
            self.logger.exception("Error while loading %s" % remote_file)
            raise ex

    def delete_files(self, origin: tuple, files: list) -> bool:
        """ Delete files from remote storage

        :param origin: <tuple(str, str)> Origin of the data, tuple with the type of the origin
                      (i.e. aws_bucket, azure_blob, azure_fileshare) and the origin (i.e. the name of the bucket).
        :param files: (str) Files to delete
        :return: (bool) True if the files has been deleted successfully
        """
        try:
            if origin[0] not in self.origins:
                self.origins[origin[0]] = self._get_origin(origin[0])

            if self.check_files(origin, files):
                return self.origins[origin[0]].delete_files(origin[1], files)
            else:
                self.logger.warning("Files do not exist")
                return False
        except Exception as ex:
            self.logger.exception("Error while deleting.")
            raise ex

    def copy_file(self, origin: tuple, src: str, dst: str, dst_bucket: str = None) -> bool:
        """ Copy src to dst

        :param origin: <tuple(str, str)> Origin of the data, tuple with the type of the origin
                      (i.e. aws_bucket, azure_blob, azure_fileshare) and the origin (i.e. the name of the bucket).
        :param src: (str) Source file(s) to copy
        :param dst: (str) Destiny of file(s)
        :param dst_bucket: (str) Destiny bucket
        :return: (bool) True if file(s) have been copied successfully
        """
        try:
            if origin[0] not in self.origins:
                self.origins[origin[0]] = self._get_origin(origin[0])
            return self.origins[origin[0]].copy_file(origin[1], src, dst, dst_bucket)
        except Exception as ex:
            self.logger.exception("Error while copying.")
            raise ex

    def move_file(self, origin: tuple, src: str, dst: str, dst_bucket: str = None) -> bool:
        """ Move src to dst

        :param origin: <tuple(str, str)> Origin of the data, tuple with the type of the origin
            (i.e. aws_bucket, azure_blob, azure_fileshare) and the origin (i.e. the name of the bucket).
        :param src: (str) Source file to move
        :param dst: (str) Destiny to move the file to
        :param dst_bucket:(str) Origin of the bucket destiny
        :return: (bool) True if the file have been moved successfully
        """
        try:
            if origin[0] not in self.origins:
                self.origins[origin[0]] = self._get_origin(origin[0])
            return self.origins[origin[0]].move_file(origin[1], src, dst, dst_bucket)
        except Exception as ex:
            self.logger.exception("Error while moving.")
            raise ex

    def check_file(self, origin: tuple, file: str) -> bool:
        """ Check if the file exists

        :param origin: <tuple(str, str)> Origin of the data, tuple with the type of the origin
            (i.e. aws_bucket, azure_blob, azure_fileshare) and the origin (i.e. the name of the bucket).
        :param file: (str) File to check if exists
        :return: (bool) True if the file exists
        """
        try:
            if origin[0] not in self.origins:
                self.origins[origin[0]] = self._get_origin(origin[0])
            return self.origins[origin[0]].check_file(origin[1], file)
        except Exception as ex:
            self.logger.exception("Error while checking.")
            raise ex

    def check_files(self, origin: tuple, files: list) -> bool:
        """ Check if files exist

        :param origin: <tuple(str, str)> Origin of the data, tuple with the type of the origin
            (i.e. aws_bucket, azure_blob, azure_fileshare) and the origin (i.e. the name of the bucket).
        :param files: (list) Files to check if exist
        :return: (bool) True if all files exist
        """
        try:
            exists = True
            for file in files:
                exists = exists and self.check_file(origin, file)

            return exists
        except Exception as ex:
            self.logger.exception("Error while checking.")
            raise ex

    def get_size_of_file(self, origin: tuple, file: str) -> int:
        """ Return the size of the file in bytes

        :param origin: <tuple(str, str)> Origin of the data, tuple with the type of the origin
            (i.e. aws_bucket, azure_blob, azure_fileshare) and the origin (i.e. the name of the bucket).
        :param file: (str) File to get the size
        :return: (int) Size of the file in bytes
        """
        try:
            if origin[0] not in self.origins:
                self.origins[origin[0]] = self._get_origin(origin[0])
            return self.origins[origin[0]].get_size_of_file(origin[1], file)
        except Exception as ex:
            self.logger.exception("Error while getting file size.")
            raise ex

    def get_size_of_files(self, origin: tuple, files: list) -> list:
        """ Return the size of all the files in bytes

        :param origin: <tuple(str, str)> Origin of the data, tuple with the type of the origin
            (i.e. aws_bucket, azure_blob, azure_fileshare) and the origin (i.e. the name of the bucket).
        :param files: (list) Files to get the size
        :return: <list(int)> List containing the size of all the files in bytes
        """
        sizes = []

        for file in files:
            try:
                sizes.append(self.get_size_of_file(origin, file))
            except Exception as ex:
                self.logger.exception("Error while getting files' size.")
                raise ex

        return sizes

    def list_files(self, origin: tuple, prefix: str = "") -> list:
        """ List all files in remote storage that start with the prefix (optional)

        :param origin: <tuple(str, str)> Origin of the data, tuple with the type of the origin
            (i.e. aws_bucket, azure_blob, azure_fileshare) and the origin (i.e. the name of the bucket).
        :param prefix: (str) Prefix the file must start with
        :return: (list) Files
        """
        try:
            if origin[0] not in self.origins:
                self.origins[origin[0]] = self._get_origin(origin[0])
            return self.origins[origin[0]].list_files(origin[1], prefix)
        except Exception as ex:
            self.logger.exception("Error while listing.")
            raise ex

    def count_files(self, origin: tuple, prefix: str = "") -> int:
        """ Count files in remote storage that start with the prefix (optional)

        :param origin: <tuple(str, str)> Origin of the data, tuple with the type of the origin
            (i.e. aws_bucket, azure_blob, azure_fileshare) and the origin (i.e. the name of the bucket).
        :param prefix: (str) Prefix the file must start with
        :return: (int) Number of files
        """
        try:
            if origin[0] not in self.origins:
                self.origins[origin[0]] = self._get_origin(origin[0])
            return self.origins[origin[0]].count_files(origin[1], prefix)
        except Exception as ex:
            self.logger.exception("Error while counting.")
            raise ex
