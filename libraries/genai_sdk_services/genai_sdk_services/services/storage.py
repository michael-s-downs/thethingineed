### This code is property of the GGAO ###


# Native imports
import logging
import os
import re
import json
from io import BytesIO
from typing import Tuple, Union
from abc import ABCMeta, abstractmethod

# Installed imports
import boto3
import asyncio
import aiofiles
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.storage.fileshare import ShareClient, ShareServiceClient, ShareDirectoryClient, ShareFileClient
from azure.storage.blob.aio import BlobServiceClient as aioBlobServiceClient

N_ASYNC_THREADS = int(os.getenv("N_ASYNC_THREADS", 150))

class BaseStorageService():
    @abstractmethod
    def download_file(self, origin, remote_file, local_file=None):
        """ Download a file from origin into a local file.

        :param origin: Bucket of the data
        :param remote_file: Name of the file in origin
        :param local_file: Name of the local file
        :return: (bool) True if file has been downloaded successfully
        """
        pass

    @abstractmethod
    def upload_file(self, origin, local_file, remote_file=None):
        """ Upload a local file to origin

        :param origin: Bucket of the data
        :param local_file: Name of the local file
        :param remote_file: Name of the origin file
        :return: (bool) True if the file has been uploaded successfully
        """
        pass

    @abstractmethod
    def load_file(self, origin, remote_file):
        """ Return an object loaded from origin

        :param origin: Bucket of the data
        :param remote_file: File to load
        :return: Content of file
        """
        pass

    @abstractmethod
    def delete_files(self, origin, files):
        """ Delete files from origin

        :param origin: Bucket of the data
        :param files: Files to delete
        :return: (bool) True if the files has been deleted successfully
        """
        pass

    @abstractmethod
    def check_file(self, origin, file):
        """ Check if the file exists

        :param origin: Bucket of the data
        :param file: File to check if exists
        :return: (bool) True if the file exists
        """
        pass

    @abstractmethod
    def check_files(self, origin, files):
        """ Check if files exist

        :param origin: Bucket of the data
        :param files: Files to check if exist
        :return: (bool) True if all files exist
        """
        pass

    @abstractmethod
    def get_size_of_file(self, origin, file):
        """ Return the size of the file in bytes

        :param origin: Bucket to check file from
        :param file: File to get the size
        :return: (int) Size of the file in bytes
        """
        pass

    @abstractmethod
    def get_size_of_files(self, origin, files):
        """ Return the size of all the files in bytes

        :param origin: Bucket to check files from
        :param files: Files to get the size
        :return: (list) List containing the size of all the files in bytes
        """
        pass

    @abstractmethod
    def list_files(self, origin, prefix=None):
        """ List all files in the bucket that start with the prefix (optional)

        :param origin: Bucket of the data
        :param prefix: (Optional) Prefix the file must start with
        :return: (list) Files
        """
        pass

    @abstractmethod
    def count_files(self, origin, prefix=None):
        """ Count files in the bucket that start with the prefix (optional)

        :param origin: Bucket of the data
        :param prefix: (Optional) Prefix the file must start with
        :return: (int) Number of files
        """
        pass

    @classmethod
    def check_origin(cls, origin_type):
        """ Check if it is a valid origin for the service

        :param origin_type: Bucket to check
        :return: (bool) True if the origin is valid
        """
        return origin_type in cls.ORIGIN_TYPES


class S3Service(BaseStorageService):

    ORIGIN_TYPES = ["aws", "aws_buckets"]
    clients = {}
    resources = {}
    buckets = {}
    credentials = {}
    secret_path = os.path.join(os.getenv('SECRETS_PATH', '/secrets'), "aws", "aws.json")
    env_vars = ["AWS_ACCESS_KEY", "AWS_SECRET_KEY", "AWS_REGION_NAME"]

    def __init__(self):
        """ Initialize the s3 resource and the bucket. Credentials must have access to the bucket """
        self.logger = logging.getLogger(__name__)

    def get_resource(self, origin: str):
        """ Get the s3 resource for the bucket

        :param origin: (str) Bucket to get the resource
        """
        if origin and origin in self.resources:
            return self.resources[origin]

        bucket_credentials = self.credentials[origin]

        if eval(os.getenv("AWS_ROLE", "False")):
            s3_resource = boto3.resource("s3")
        else:
            s3_resource = boto3.resource("s3", aws_access_key_id=bucket_credentials['access_key'], aws_secret_access_key=bucket_credentials['secret_key'])

        self.resources[origin] = s3_resource

        return s3_resource

    def get_client(self, origin: str):
        """ Get the s3 client for the bucket

        :param origin: (str) Bucket to get the client
        """
        if origin and origin in self.clients:
            return self.clients[origin]

        bucket_credentials = self.credentials[origin]

        if eval(os.getenv("AWS_ROLE", "False")):
            s3_client = boto3.client("s3")
        else:
            s3_client = boto3.client("s3", aws_access_key_id=bucket_credentials['access_key'], aws_secret_access_key=bucket_credentials['secret_key'])

        self.clients[origin] = s3_client

        return s3_client

    def set_credentials(self, origin: str, credentials: dict):
        """ Set the credentials for the bucket

        :param origin: (str) Bucket to set the credentials
        :param credentials: (dict) Credentials to set
        """
        if origin and origin not in self.credentials:
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
                elif eval(os.getenv("AWS_ROLE", "False")):
                    credentials = {}
                else:
                    raise Exception("Credentials not found")

            self.credentials[origin] = credentials

    def get_bucket(self, origin: str):
        """ Get the bucket object

        :param origin: Bucket to get
        """
        if origin and origin in self.buckets:
            return self.buckets[origin]

        s3_resource = self.get_resource(origin)

        bucket = s3_resource.Bucket(origin)

        self.buckets[origin] = bucket

        return bucket

    def download_file(self, origin: str, remote_file: str, local_file: str = None) -> bool:
        """ Download a file from s3 into a local file.

        :param origin: (str) S3 bucket to download file from
        :param remote_file: (str) Name of the file in s3
        :param local_file: (str) Name of the local file
        :return: (bool) True if file has been downloaded successfully
        """
        bucket = origin
        bucket = self.get_bucket(bucket)

        if local_file is None:
            self.logger.debug("local_file not specified, file will be saved as %s" % remote_file)
            local_file = remote_file

        dirname = os.path.dirname(local_file)
        if not dirname:
            dirname = "."

        if not os.path.exists(dirname):
            self.logger.debug("Directory does not exist. Creating directory...")
            os.makedirs(dirname)

        self.logger.debug("Downloading %s..." % remote_file)
        bucket.download_file(Key=remote_file, Filename=local_file)

        return os.path.exists(local_file)

    def download_directory(self, origin: str, remote_directory: str, local_directory: str = None, suffix: str = None):
        """ Download a directory from s3 into a local directory.

        :param origin: (str) S3 bucket to download directory from
        :param remote_directory: (str) Name of the directory in s3
        :param local_directory: (str) Name of the local directory
        :param suffix: (str) Suffix of the files to download
        """
        bucket = origin
        bucket = self.get_bucket(bucket)

        remote_directory = _assert_has_slash(remote_directory)

        if local_directory is None:
            self.logger.debug("local_file not specified, file will be saved as %s" % remote_directory)
            local_directory = remote_directory

        self.logger.debug(f"downloading directory from {origin}:{remote_directory}")
        for remote_object in bucket.objects.filter(Prefix=remote_directory):
            remote_key = remote_object.key
            remote_key_no_parent = remote_object.key[len(remote_directory):]

            remote_suffix = remote_key_no_parent.split(".")[-1]
            if type(suffix) is list and len(suffix) > 0:
                if remote_suffix not in suffix:
                    continue

            local_key = local_directory + remote_key_no_parent
            local_subdirectory = os.path.dirname(local_key)

            if not _is_directory(remote_key):
                if not os.path.exists(local_subdirectory):
                    os.makedirs(local_subdirectory, exist_ok=True)
                self.download_file(origin, remote_key, local_key)

    def upload_object(self, origin: str, object_: bytes, remote_file: str) -> bool:
        """ Upload an object into a s3 bucket.

        :param origin: (str) S3 bucket to download file from
        :param remote_file: (str) Name of the file in s3
        :param object_: (bytes) Object to upload
        :return: (bool) True if file has been downloaded successfully
        """
        bucket = origin
        bucket = self.get_bucket(bucket)

        bucket.put_object(Body=object_, Key=remote_file)

    def upload_file(self, origin: str, local_file: str, remote_file: str = None) -> bool:
        """ Upload a local file to s3

        :param origin: (str) S3 Bucket to upload file to
        :param local_file: (str) Name of the local file
        :param remote_file: (str) Name of the s3 file
        :return: (bool) True if the file has been uploaded successfully
        """
        bucket = origin
        bucket = self.get_bucket(bucket)

        if remote_file is None:
            self.logger.debug("s3_file not specified, file will be uploaded to %s" % remote_file)
            remote_file = local_file

        self.logger.debug("Uploading %s..." % local_file)
        bucket.upload_file(Filename=local_file, Key=remote_file)

    def load_file(self, origin: str, remote_file: str) -> bytes:
        """ Return an object loaded from s3

        :param origin: (str) S3 Bucket to load file from
        :param remote_file: (str) File to load
        :return: (bytes) Content of file
        """
        bucket = origin
        s3_client = self.get_client(bucket)

        self.logger.debug("Loading %s..." % remote_file)
        return s3_client.get_object(Bucket=bucket, Key=remote_file)['Body'].read()

    def delete_files(self, origin: str, files: str) -> bool:
        """ Delete files from s3

        :param origin: (str) S3 Bucket to delete files from
        :param files: (str) Files to delete
        :return: (bool) True if the files has been deleted successfully
        """
        bucket = origin
        s3_client = self.get_client(bucket)

        keys = []
        for key in files:
            keys.append({'Key': key})

        delete = {'Objects': keys}

        self.logger.debug("Deleting %s..." % str(files))
        s3_client.delete_objects(Bucket=bucket, Delete=delete)

    def copy_file(self, origin: str, src: str, dst: str, dst_bucket: str = None) -> bool:
        """ Copy file from s3

        :param origin: (str) Old S3 Bucket
        :param src: (str) old path
        :param dst: (str) new path
        :param dst_bucket: (str) New S3 Bucket
        :return: (bool) True if the files has been copied successfully
        """
        bucket = origin
        s3_client = self.get_client(bucket)

        if dst_bucket is None:
            dst_bucket = bucket

        self.logger.debug(f"Copying {bucket}/{src} to {dst}")
        copy_source = {'Bucket': bucket, 'Key': src}
        s3_client.copy(copy_source, dst_bucket, dst)

    def move_file(self, origin: str, src: str, dst: str, dst_bucket: str = None) -> bool:
        """ Move file from s3

        :param origin: (str) Old S3 Bucket
        :param src: (str) old path
        :param dst: (str) new path
        :param dst_bucket: (str) New S3 Bucket
        :return: (bool) True if the files has been copied successfully
        """
        self.logger.debug(f"Moving {origin}/{src} to {dst}")
        self.copy_file(origin, src, dst, dst_bucket)
        self.delete_files(origin, files=[src])

    def check_file(self, origin: str, file: str) -> bool:
        """ Check if the file exists

        :param origin: (str) S3 Bucket to check file from
        :param file: (str) File to check if exists
        :return: (bool) True if the file exists
        """
        bucket = origin
        bucket = self.get_bucket(bucket)
        try:
            self.logger.debug("Checking %s..." % file)
            bucket.Object(file).load()
        except Exception as ex:
            try:
                if ex.response['Error']['Code'] == "404":
                    self.logger.warning("%s not found." % file)
                    return False
            except Exception as ex:
                raise ex
        else:
            self.logger.debug("%s found" % file)
            return True

    def check_files(self, origin: str, files: list) -> bool:
        """ Check if files exist

        :param origin: (str) S3 Bucket to check files from
        :param files: (list) Files to check if exist
        :return: (bool) True if all files exist
        """
        exists = False

        for file in files:
            exists = exists and self.check_file(origin, file)

        return exists

    def get_size_of_file(self, origin: str, file: str) -> Union[int, bool]:
        """ Return the size of the file in bytes

        :param origin: (str) S3 Bucket to check file from
        :param file: (str) File to get the size
        :return: (int) Size of the file in bytes
        """
        bucket = origin
        bucket = self.get_bucket(bucket)
        try:
            self.logger.debug("Getting the size of %s..." % file)
            return bucket.Object(file).content_length
        except Exception as ex:
            try:
                if ex.response['Error']['Code'] == "404":
                    self.logger.warning("%s not found." % file)
                    return False
            except Exception as ex:
                self.logger.error("Error while getting file size.")
                raise ex

    def get_size_of_files(self, origin: str, files: list) -> list:
        """ Return the size of all the files in bytes

        :param origin: (str) S3 Bucket to check files from
        :param files: (list) Files to get the size
        :return: (list) List containing the size of all the files in bytes
        """
        sizes = []

        for file in files:
            sizes.append(self.get_size_of_file(origin, file))

        return sizes

    def list_files(self, origin: str, prefix: str = "", limit: int = -1) -> list:
        """ List all files in s3

        :param origin: (str) S3 Bucket to list file of
        :param prefix: (str) Prefix the file must start with
        :param limit: (str) (100 by default) Limit of files to list
        :return: (list) Files
        """
        bucket = origin
        s3_client = self.get_client(bucket)
        self.logger.debug("Listing files...")

        if limit <= 0:
            limit = 10**9

        keys = []

        kwargs = {
            'Bucket': bucket,
            'MaxKeys': min(1000, limit),
            'Prefix': prefix
        }

        while len(keys) < limit:
            resp = s3_client.list_objects_v2(**kwargs)
            for obj in resp['Contents']:
                keys.append(obj['Key'])

            try:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break

        return keys

    def count_files(self, origin: str, prefix: str = "") -> int:
        """ Count files in s3 Bucket

        :param origin: (str) S3 Bucket to count files of
        :param prefix: (str) Prefix the file must start with
        :return: (int) Number of files
        """
        bucket = origin
        bucket = self.get_bucket(bucket)

        self.logger.debug("Counting files...")
        return sum(1 for _ in bucket.objects.filter(Prefix=prefix).all())


class BlobService(BaseStorageService):

    ORIGIN_TYPES = ["azure", "azure_blob"]
    clients = {}
    resources = {}
    buckets = {}
    credentials = {}
    secret_path = os.path.join(os.getenv('SECRETS_PATH', '/secrets'), "azure", "azure.json")
    env_vars = ["AZ_CONN_STR_STORAGE"]

    def __init__(self):
        """ Initialize the Blob resource and the bucket. Credentials must have access to the container """
        self.logger = logging.getLogger(__name__)

    def get_resource(self, origin: str):
        raise RuntimeError("This method is not implemented")

    def get_bucket(self, origin: str, async_mode: bool = False):
        """ Obtain the client to interact with a specific container.

        param origin: (str) Name of the container to get the client.
        param async_mode: (bool) Optional. Flag to set the client sync or async.
        return: ContainerClient
        """
        bucket_credentials = self.credentials[origin]
        if async_mode:
                aio_service_client = aioBlobServiceClient.from_connection_string(bucket_credentials['conn_str'])
                bucket = aio_service_client.get_container_client(origin)
        else:
            bucket = ContainerClient.from_connection_string(bucket_credentials['conn_str'], origin)

        self.buckets[origin] = bucket
        return bucket

    def get_client(self, origin: str, file: str, async_mode: bool = False):
        """ Obtain a client to interact with the specified blob.

        param origin: (str) Name of the container.
        param file: (str) The blob with which to interact.
        param async_mode: (bool) Optional. Flag to use async.
        return: ContainerClient
        """
        bucket = self.get_bucket(origin, async_mode)
        if async_mode:
            client = bucket.get_blob_client(blob=file)
        else:
            client = bucket.get_blob_client(file)

        return client, bucket

    def set_credentials(self, origin: str, credentials: dict):
        """ Set the credentials for the service

        :param origin: (str) type of the queue and the name of the queue
        :param credentials: (dict) Credentials to connect to the service
        """
        if origin and origin not in self.credentials:
            if not credentials:
                if os.path.exists(self.secret_path):
                    with open(self.secret_path, "r") as file:
                        conn_str = json.load(file).get('conn_str_storage', "")
                        credentials = {'conn_str': conn_str}
                elif os.getenv(self.env_vars[0], ""):
                    credentials = {'conn_str': os.getenv(self.env_vars[0])}
                else:
                    raise Exception("Credentials not found")

            self.credentials[origin] = credentials

    def download_file(self, origin: str, remote_file: str, local_file: str = None) -> bool:
        """ Download a file from Blob service into a local file.

        :param origin: (str) Blob container to download file from
        :param remote_file: (str) Name of the file in Blob service
        :param local_file: (str) Name of the local file
        :return: (bool) True if file has been downloaded successfully
        """
        file, container = self.get_client(origin, remote_file)

        try:
            if local_file is None:
                self.logger.debug("local_file not specified, file will be saved as %s" % remote_file)
                local_file = remote_file

            dirname = os.path.dirname(local_file)
            if not dirname:
                dirname = "."

            if not os.path.exists(dirname):
                self.logger.debug("Directory does not exist. Creating directory...")
                os.makedirs(dirname)

            self.logger.debug("Downloading %s..." % remote_file)

            with open(local_file, "wb") as file_handle:
                data = file.download_blob()
                data.readinto(file_handle)
        except Exception as ex:
            raise ex
        else:
            return os.path.exists(local_file)
        finally:
            file.close()
            container.close()

    async def download_directory_async(self, origin: str, remote_directory: str, local_directory: str = None, suffix: list = None) -> bool:
        """ Download a directory from Blob service into a local file.

        :param origin: (str) Blob container to download files from
        :param remote_directory: (str) Name of the directory in Blob service
        :param local_directory: (str) Name of the local directory
        :param suffix: (list) Types of files to download
        :return: (bool) True if all files has been downloaded successfully
        """
        remote_directory = _assert_has_slash(remote_directory)
        if local_directory:
            local_directory = _assert_has_slash(local_directory)

        blob_service_client = aioBlobServiceClient.from_connection_string(self.credentials[origin]['conn_str'])
        container_client = blob_service_client.get_container_client(origin)

        semaphore = asyncio.Semaphore(N_ASYNC_THREADS)  # Limit concurrent downloads

        async def limited_download(blob_name, local_file_name):
            async with semaphore:
                await self.download_file_async(blob_service_client, origin, blob_name, local_file_name)

        tasks = []
        async for blob in container_client.list_blobs(name_starts_with=remote_directory):
            if local_directory:
                local_file_name = f"{local_directory}{blob.name.split('/')[-1]}"
            else:
                local_file_name = blob.name

            remote_key_no_parent = blob.name[len(remote_directory):]

            remote_suffix = remote_key_no_parent.split(".")[-1]
            if type(suffix) is list and len(suffix) > 0:
                if remote_suffix not in suffix:
                    continue
            tasks.append(limited_download(blob.name, local_file_name))

        await asyncio.gather(*tasks)
        await blob_service_client.close()
    
    async def download_batch_files_async(self, origin: str, files_list: list, local_directory: str) -> bool:
        """ Download a batch of files from Blob service into local files.

        :param origin: (str) Blob container to download files from
        :param remote_directory: (str) Name of the directory in Blob service
        :param local_directory: (str) Name of the local directory
        :param suffix: (list) Types of files to download
        :return: (bool) True if all files has been downloaded successfully
        """
        local_directory = _assert_has_slash(local_directory)

        blob_service_client = aioBlobServiceClient.from_connection_string(self.credentials[origin]['conn_str'])
        container_client = blob_service_client.get_container_client(origin)

        semaphore = asyncio.Semaphore(N_ASYNC_THREADS)  # Limit concurrent downloads

        if len(files_list) < 1:
            raise Exception("No files received to download")
        
        async def limited_download(blob_name, local_file_name):
            async with semaphore:
                await self.download_file_async(blob_service_client, origin, blob_name, local_file_name)

        tasks = []
        for file_name in files_list:
            async for blob in container_client.list_blobs(name_starts_with=file_name):
                local_file_name = f"{local_directory}{blob.name.split('/')[-1]}"
                tasks.append(limited_download(blob.name, local_file_name))

        await asyncio.gather(*tasks)
        await blob_service_client.close()


    async def download_file_async(self, blob_service_client, container_name, remote_file, local_file_name):
        """ Download a file from Blob service into a local file using async azure library.

        Args:
            blob_service_client (_type_): _description_
            container_name (str): _description_
            remote_file (str): _description_
            local_path (str): 
        """

        if local_file_name is None:
            self.logger.debug("local_file not specified, file will be saved as %s" % remote_file)
            local_file_name = remote_file

        dirname = os.path.dirname(local_file_name)
        if not dirname:
            dirname = "."

        if not os.path.exists(dirname):
            self.logger.debug("Directory does not exist. Creating directory...")
            os.makedirs(dirname)

        blob_client = blob_service_client.get_blob_client(container=container_name, blob=remote_file)
        
        async with aiofiles.open(f"{dirname}/{local_file_name.split('/')[-1]}", "wb") as file:
            stream = await blob_client.download_blob()
            async for chunk in stream.chunks():
                await file.write(chunk)


    def download_directory(self, origin: str, remote_directory: str, local_directory: str = None, suffix: str = None) -> bool:
        """ Download a directory from Blob service into a local file.

        :param origin: (str) Blob container to download files from
        :param remote_directory: (str) Name of the directory in Blob service
        :param local_directory: (str) Name of the local directory
        :param suffix: (list) Types of files to download
        :return: (bool) True if all files has been downloaded successfully
        """
        ok = True
        bucket = origin
        bucket = self.get_bucket(bucket)

        try:
            remote_directory = _assert_has_slash(remote_directory)

            if local_directory is None:
                self.logger.debug("local_file not specified, file will be saved as %s" % remote_directory)
                local_directory = remote_directory
            local_directory = _assert_has_slash(local_directory)
            self.logger.debug(f"downloading directory from {origin}:{remote_directory}")
            for remote_object in list(bucket.list_blob_names(name_starts_with=remote_directory)):
                remote_key_no_parent = remote_object[len(remote_directory):]

                remote_suffix = remote_key_no_parent.split(".")[-1]
                if type(suffix) is list and len(suffix) > 0:
                    if remote_suffix not in suffix:
                        continue

                local_key = local_directory + remote_key_no_parent
                local_subdirectory = os.path.dirname(local_key)

                if not _is_directory(remote_object):
                    if not os.path.exists(local_subdirectory):
                        os.makedirs(local_subdirectory, exist_ok=True)
                    ok = ok and self.download_file(origin, remote_object, local_key)
        except Exception as ex:
            raise ex
        else:
            return ok
        finally:
            bucket.close()

    def upload_object(self, origin: str, object_: bytes, remote_file: str) -> bool:
        """ Upload an object into a Blob container.

        :param origin: (str) Blob container to upload object from
        :param remote_file: (str) Name of the file in Blob service
        :param object_: (bytes) Object to upload
        :return: (bool) True if object has been uploaded successfully
        """
        file, container = self.get_client(origin, remote_file)

        try:
            if type(object_) == str:
                object_ = bytes(object_, "utf-8")

            with BytesIO(object_) as stream:
                file.upload_blob(stream, overwrite=True)
        except Exception as ex:
            raise ex
        else:
            return True
        finally:
            file.close()
            container.close()

    def upload_file(self, origin: str, local_file: str, remote_file: str = None) -> bool:
        """ Upload a file into a Blob container.

        :param origin: (str) Blob container to upload file from
        :param local_file: (str) File to upload
        :param remote_file: (str) Name of the file in Blob service
        :return: (bool) True if file has been uploaded successfully
        """
        file, container = self.get_client(origin, remote_file)

        if remote_file is None:
            self.logger.debug("s3_file not specified, file will be uploaded to %s" % remote_file)
            remote_file = local_file
        try:
            self.logger.debug("Uploading %s..." % local_file)
            with open(local_file, "rb") as source:
                file.upload_blob(source, overwrite=True)
        except Exception as ex:
            raise ex
        else:
            return True
        finally:
            file.close()
            container.close()
    

    async def upload_batch_files_async(self, origin: str, file_paths: list, remote_folder: str):
        """Upload multiple files asynchronously with controlled concurrency.

        :param origin: (str) Blob container to upload files.
        :param file_paths: (list) Files to upload.
        :param remote_folder: (str) Name of the folder in Blob service.
        :return: (bool) True if file has been uploaded successfully
        """

        remote_folder = _assert_has_slash(remote_folder)

        blob_service_client = aioBlobServiceClient.from_connection_string(self.credentials[origin]['conn_str'])
        container_client = blob_service_client.get_container_client(origin)

        semaphore = asyncio.Semaphore(N_ASYNC_THREADS)

        tasks = [self.upload_file_async(file_path, remote_folder, container_client, semaphore) for file_path in file_paths]
        await asyncio.gather(*tasks)
        await blob_service_client.close()


    async def upload_folder_async(self, origin: str, local_folder: list, remote_folder: str):
        """Upload multiple files asynchronously with controlled concurrency.

        :param origin: (str) Blob container to upload files.
        :param local_folder: (str) Name of the local folder.
        :param remote_folder: (str) Name of the folder in Blob service.
        :return: (bool) True if file has been uploaded successfully
        """

        remote_folder = _assert_has_slash(remote_folder)
        local_folder = _assert_has_slash(local_folder)

        blob_service_client = aioBlobServiceClient.from_connection_string(self.credentials[origin]['conn_str'])
        container_client = blob_service_client.get_container_client(origin)

        semaphore = asyncio.Semaphore(N_ASYNC_THREADS)
        
        tasks = []
        for file_name in os.listdir(local_folder):
            tasks.append(self.upload_file_async(f"{local_folder}{file_name}", remote_folder, container_client, semaphore))

        await asyncio.gather(*tasks)
        await blob_service_client.close()


    async def upload_file_async(self, file_path: str, remote_folder:str, container_client, semaphore):
        blob_name = f"{remote_folder}{file_path.split('/')[-1]}"
        async with semaphore:
            async with aiofiles.open(file_path, "rb") as file:
                blob_client = container_client.get_blob_client(blob_name)
                await blob_client.upload_blob(await file.read(), overwrite=True)

    def load_file(self, origin: str, remote_file: str) -> bytes:
        """ Return content of file in bytes

        :param origin: (str) Blob container to load file from
        :param remote_file: (str) File to load
        :return: (bytes) Content of file
        """
        file, container = self.get_client(origin, remote_file)

        self.logger.debug("Loading %s..." % remote_file)

        try:
            file_contents = file.download_blob().readall()
        except Exception as ex:
            raise ex
        else:
            return file_contents
        finally:
            file.close()
            container.close()

    def delete_files(self, origin: str, files: list) -> bool:
        """ Delete files from Blob service

        :param origin: (str) Blob container to delete files from
        :param files: (list) Files to delete
        :return: (bool) True if the files has been deleted successfully
        """
        try:
            for key in files:
                file, container = self.get_client(origin, key)
                try:
                    file.delete_blob()
                except Exception as ex:
                    raise ex
                finally:
                    file.close()
                    container.close()
        except Exception as ex:
            raise ex
        else:
            return True

    def copy_file(self, origin: str, src: str, dst: str, dst_bucket: str = None) -> bool:
        """ Copy file from Blob service

        :param origin: (str) Blob container to copy file from
        :param src: (str) Name of the file to copy
        :param dst: (str) New name
        :param dst_bucket: (str) Blob container to the copy
        :return: (bool) True if the files has been deleted successfully
        """

        if dst_bucket is None:
            dst_bucket = origin
        copy_client, container = self.get_client(dst_bucket, dst)

        try:
            arch_to_copy = self.load_file(origin, src)
            self.logger.debug(f"Copying {origin}/{src} to {dst}")
            copy_client.upload_blob(arch_to_copy)
        except Exception as ex:
            raise ex
        else:
            return True
        finally:
            copy_client.close()
            container.close()

    def move_file(self, origin: str, src: str, dst: str, dst_bucket: str = None) -> bool:
        """ Copy (or rename) file from Blob service

        :param origin: (str) Blob container to move file from
        :param src: (str) Name of the file to move
        :param dst: (str) New name
        :param dst_bucket: (str) Blob container to the new file
        :return: (bool) True if the files has been deleted successfully
        """
        self.logger.debug(f"Moving {origin}/{src} to {dst}")
        try:
            self.copy_file(origin, src, dst, dst_bucket)
            self.delete_files(origin, files=[src])
        except Exception as ex:
            raise ex
        else:
            return True

    def check_file(self, origin: str, file: str) -> bool:
        """ Check if the file exists

        :param origin: (str) Blob container to check file from
        :param file: (str) File to check if exists
        :return: (bool) True if the file exists
        """
        file_client, container = self.get_client(origin, file)

        try:
            self.logger.debug("Checking %s..." % file)
            file_client.get_blob_properties()
        except Exception:
            return False
        else:
            return True
        finally:
            file_client.close()
            container.close()

    def check_files(self, origin: str, files: list) -> bool:
        """ Check if files exist

        :param origin: (str) Blob container to check files from
        :param files: (list) Files to check if exist
        :return: (bool) True if all files exist
        """
        exists = False
        for file in files:
            exists = exists and self.check_file(origin, file)

        return exists

    def get_size_of_file(self, origin: str, file: str) -> Union[int, bool]:
        """ Return the size of the file in bytes

        :param origin: (str) Blob container to get size of file from
        :param file: (str) File to get the size
        :return: (int) Size of the file in bytes
        """
        file_client, container = self.get_client(origin, file)

        try:
            self.logger.debug("Getting the size of %s..." % file)
            size = file_client.get_blob_properties()['size']
        except Exception:
            return False
        else:
            return size
        finally:
            file_client.close()
            container.close()

    def get_size_of_files(self, origin: str, files: list) -> list:
        """ Return the size of all the files in bytes

        :param origin: (str) Blob container to get size of files from
        :param files: (list) Files to get the size
        :return: (list) List containing the size of all the files in bytes
        """
        sizes = []
        for file in files:
            sizes.append(self.get_size_of_file(origin, file))

        return sizes

    def list_files(self, origin: str, prefix: str = "", limit: int = -1) -> list:
        """ List all files in a Blob container

        :param origin: (str) Blob container to list file of
        :param prefix: (str) Prefix the file must start with
        :param limit: (str) (100 by default) Limit of files to list
        :return: (list) Files
        """
        bucket = origin
        bucket = self.get_bucket(bucket)
        self.logger.debug("Listing files...")

        try:
            if limit <= 0:
                limit = 10 ** 9

            keys = []
            num = self.count_files(origin, prefix)
            while len(keys) < min(1000, limit, num):
                resp = bucket.list_blob_names(name_starts_with=prefix)
                for obj in resp:
                    keys.append(obj)
        except Exception as ex:
            raise ex
        else:
            return keys
        finally:
            bucket.close()

    def count_files(self, origin: str, prefix: str = "") -> int:
        """ Count files in a Blob container

        :param origin: (str) Blob container to count files of
        :param prefix: (str) Prefix the file must start with
        :return: (int) Number of files
        """
        bucket = origin
        bucket = self.get_bucket(bucket)
        try:
            file_list = bucket.list_blob_names(name_starts_with=prefix)
            self.logger.debug("Counting files...")
            file_count = len([i for i in file_list])
        except Exception as ex:
            raise ex
        else:
            return file_count
        finally:
            bucket.close()


class FileShareService(BaseStorageService):

    ORIGIN_TYPES = ["azure_fileshare"]
    file_clients = {}
    folder_clients = {}
    buckets = {}
    credentials = {}
    secret_path = os.path.join(os.getenv('SECRETS_PATH', '/secrets'), "azure", "azure.json")
    env_vars = ["AZ_CONN_STR_STORAGE"]

    def __init__(self):
        """ Initialize the FileShare resource and the bucket. Credentials must have access to the bucket """
        self.logger = logging.getLogger(__name__)

    def get_resource(self, origin: str):
        raise RuntimeError("This method is not implemented")

    def get_bucket(self, origin: str):
        """Return an object of the ShareClient class.

        :param origin: (str) The name of the bucket
        """
        if origin and origin in self.buckets:
            return self.buckets[origin]
        bucket_credentials = self.credentials[origin]
        bucket = ShareClient.from_connection_string(bucket_credentials['conn_str'], origin)
        self.buckets[origin] = bucket

        return bucket

    def get_file_client(self, origin: str, file: str):
        """Return an object of the ShareFileClient class.

        :param origin: (str) The name of the bucket
        :param file: (str) The name of the file
        """
        if file and file in self.file_clients:
            return self.file_clients[file]
        bucket = self.get_bucket(origin)
        client = bucket.get_file_client(file)
        self.file_clients[file] = client
        return client

    def get_folder_client(self, origin: str, folder: str):
        """Return an object of the ShareDirectoryClient class."""
        if folder and folder in self.folder_clients:
            return self.folder_clients[folder]
        bucket = self.get_bucket(origin)
        client = bucket.get_directory_client(folder)
        self.folder_clients[folder] = client
        return client

    def set_credentials(self, origin: str, credentials: dict):
        """ Set the credentials for the service

        :param origin: (str) type of the queue and the name of the queue
        :param credentials: (dict) Credentials to connect to the service
        """
        if origin and origin not in self.credentials:
            if not credentials:
                if os.path.exists(self.secret_path):
                    with open(self.secret_path, "r") as file:
                        conn_str = json.load(file).get('conn_str_storage', "")
                        credentials = {'conn_str': conn_str}
                elif os.getenv(self.env_vars[0], ""):
                    credentials = {'conn_str': os.getenv(self.env_vars[0])}
                else:
                    raise Exception("Credentials not found")

            self.credentials[origin] = credentials

    def _list_all_files(self, origin: str, current: str = "", paths: list = None) -> list:
        """ List all files in a Share

        :param origin: (str) Share to list file of
        :param current: (DO NOT MODIFY) This is a private parameter required for recursion.
        :param paths: (DO NOT MODIFY) This is a private parameter required for recursion.
        :return: (list) Files
        """
        if paths is None:
            paths = []
        bucket_credentials = self.credentials[origin]
        share_service_client = ShareServiceClient.from_connection_string(bucket_credentials['conn_str'])
        share_client = share_service_client.get_share_client(origin)
        client_directory = share_client.get_directory_client(current)

        for item in client_directory.list_directories_and_files():
            if item._is_directory:
                new_path = f"{current}/{item.name}" if current else item.name
                self._list_all_files(origin, new_path, paths)
            else:
                complete_path = f"{current}/{item.name}" if current else item.name
                paths.append(complete_path)

        return paths

    def download_file(self, origin: str, remote_file: str, local_file: str = None) -> bool:
        """ Download a file from FileShare service into a local file.

        :param origin: (str) Share to download file from
        :param remote_file: (str) Name of the file in FileShare
        :param local_file: (str) Name of the local file
        :return: (bool) True if file has been downloaded successfully
        """
        file = self.get_file_client(origin, remote_file)

        if local_file is None:
            self.logger.debug("local_file not specified, file will be saved as %s" % remote_file)
            local_file = remote_file

        dirname = os.path.dirname(local_file)
        if not dirname:
            dirname = "."

        if not os.path.exists(dirname):
            self.logger.debug("Directory does not exist. Creating directory...")
            os.makedirs(dirname)

        self.logger.debug("Downloading %s..." % remote_file)

        with open(local_file, "wb") as file_handle:
            data = file.download_file()
            data.readinto(file_handle)
        return os.path.exists(local_file)

    def download_directory(self, origin: str, remote_directory: str, local_directory: str = None, suffix: str = None) -> bool:
        """ Download a directory from FileShare service into a local file.

        :param origin: (str) Share to download files from
        :param remote_directory: (str) Name of the directory in FileShare service
        :param local_directory: (str) Name of the local directory
        :param suffix: (list) Types of files to download
        :return: (bool) True if all files has been downloaded successfully
        """
        ok = True
        remote_directory = _assert_has_slash(remote_directory)
        all = self.list_files(origin, remote_directory)
        bucket_credentials = self.credentials[origin]
        direct = ShareDirectoryClient.from_connection_string(bucket_credentials['conn_str'], origin, remote_directory)
        if not direct.exists():
            raise RuntimeError("Error in remote_directory")
        if local_directory is None:
            self.logger.debug("local_file not specified, file will be saved as %s" % remote_directory)
            local_directory = remote_directory
        local_directory = _assert_has_slash(local_directory)
        self.logger.debug(f"downloading directory from {origin}:{remote_directory}")
        for file in all:
            remote_key_no_parent = file[len(remote_directory):]
            remote_suffix = remote_key_no_parent.split(".")[-1]
            if type(suffix) is list and len(suffix) > 0:
                if remote_suffix not in suffix:
                    continue

            local_key = local_directory + remote_key_no_parent
            local_subdirectory = os.path.dirname(local_key)

            if not os.path.exists(local_subdirectory):
                os.makedirs(local_subdirectory, exist_ok=True)
            ok = ok and self.download_file(origin, file, local_key)
        return ok

    def upload_object(self, origin: str, object_: bytes, remote_file: str) -> bool:
        """ Upload an object into a Share.

        :param origin: (str) Share to download file from
        :param remote_file: (str) Name of the file in FileShare
        :param object_: (bytes) Object to upload
        :return: (bool) True if file has been downloaded successfully
        """
        try:
            folder, file = self.is_directory_or_file(remote_file)
            path = "/".join(folder)
            directory_client = self.get_folder_client(origin, path)
            if not directory_client.exists():
                directory_client.create_directory()
            file_client = directory_client.get_file_client(file[0])
            with open(object_, "rb") as source:
                file_client.upload_file(source)
        except Exception as ex:
            raise ex
        else:
            return True

    def upload_file(self, origin: str, local_file: str, remote_file: str = None) -> bool:
        """ Upload a local file to FileShare

        :param origin: (str) Share to upload file to
        :param local_file: (str) Name of the local file
        :param remote_file: (str) Name of the FileShare file
        :return: (bool) True if the file has been uploaded successfully
        """

        if remote_file is None:
            self.logger.debug("s3_file not specified, file will be uploaded to %s" % remote_file)
            remote_file = local_file
        try:
            folder, file = self.is_directory_or_file(remote_file)
            path = "/".join(folder)
            directory_client = self.get_folder_client(origin, path)
            if not directory_client.exists():
                directory_client.create_directory()
            file_client = directory_client.get_file_client(file[0])
            with open(local_file, "rb") as source:
                file_client.upload_file(source)
        except Exception as ex:
            raise ex
        else:
            return True

    def load_file(self, origin: str, remote_file: str) -> bytes:
        """ Return an object loaded from FileShare in bytes

        :param origin: (str) Share to load file from
        :param remote_file: (str) File to load
        :return: (bytes) Content of file in bytes
        """
        file = self.get_file_client(origin, remote_file)

        self.logger.debug("Loading %s..." % remote_file)

        file_contents = file.download_file().readall()
        return file_contents

    def delete_files(self, origin: str, files: list) -> bool:
        """ Delete files from FileShare service

        :param origin: (str) Share to delete files from
        :param files: (list) Files to delete
        :return: (bool) True if the files has been deleted successfully
        """
        try:
            for key in files:
                file = self.get_file_client(origin, key)
                file.delete_file()
        except Exception as ex:
            raise ex
        else:
            return True

    def copy_file(self, origin: str, src: str, dst: str, dst_bucket: str = None) -> bool:
        """ Copy file from FileShare service

        :param origin: (str) Share to copy file from
        :param src: (str) Name of the file to copy
        :param dst: (str) New name
        :param dst_bucket: (str) Share to the copy
        :return: (bool) True if the files has been deleted successfully
        """
        arch_to_copy = self.load_file(origin, src)

        if dst_bucket is None:
            dst_bucket = origin
        copy_client = self.get_file_client(dst_bucket, dst)
        try:
            self.logger.debug(f"Copying {origin}/{src} to {dst}")
            copy_client.upload_file(arch_to_copy)
        except Exception as ex:
            raise ex
        else:
            return True

    def move_file(self, origin: str, src: str, dst: str, dst_bucket: str = None):
        """ Copy (or rename) file from FileShare service

        :param origin: (str) Share to move file from
        :param src: (str) Name of the file to move
        :param dst: (str) New name
        :param dst_bucket: (str) Share to the new file
        :return: (bool) True if the files has been deleted successfully
        """
        self.logger.debug(f"Moving {origin}/{src} to {dst}")
        try:
            self.copy_file(origin, src, dst, dst_bucket)
            self.delete_files(origin, files=[src])
        except Exception as ex:
            raise ex
        else:
            return True

    def check_file(self, origin: str, file: str) -> bool:
        """ Check if the file exists

        :param origin: (str) Share to check file from
        :param file: (str) File to check if exists
        :return: (bool) True if the file exists
        """
        try:
            file_client = self.get_file_client(origin, file)
            self.logger.debug("Checking %s..." % file)
            file_client.get_file_properties()
            self.logger.debug("%s found" % file)
            return True
        except Exception:
            self.logger.warning("%s not found." % file)
            return False

    def check_files(self, origin: str, files: list) -> bool:
        """ Check if files exist

        :param origin: (str) Share to check files from
        :param files: (list) Files to check if exist
        :return: (bool) True if all files exist
        """
        exists = False

        for file in files:
            exists = exists and self.check_file(origin, file)

        return exists

    def get_size_of_file(self, origin: str, file: str) -> Union[int, bool]:
        """ Return the size of the file in bytes

        :param origin: (str) Share to check file from
        :param file: (str) File to get the size
        :return: (int) Size of the file in bytes
        """
        file_client = self.get_file_client(origin, file)
        try:
            self.logger.debug("Getting the size of %s..." % file)
            return file_client.get_file_properties()['size']
        except Exception:
            self.logger.warning("%s not found." % file)
            return False

    def get_size_of_files(self, origin: str, files: list) -> list:
        """ Return the size of all the files in bytes

        :param origin: (str) Share to check files from
        :param files: (list) Files to get the size
        :return: (list) List containing the size of all the files in bytes
        """
        sizes = []

        for file in files:
            sizes.append(self.get_size_of_file(origin, file))

        return sizes

    def list_files(self, origin: str, prefix: str = "", limit: int = -1):
        """ List all files in a Share that start with prefix

        :param origin: (str) Share to list file of
        :param prefix: (str) Prefix the file must start with
        :param limit: (int) (100 by default) Limit of files to list
        :return: (list) Files
        """

        all = self._list_all_files(origin)
        keys = [arch for arch in all if arch.startswith(prefix)]
        self.logger.debug("Listing files...")

        if limit <= 0:
            limit = 10**9

        keys = keys[:min(limit, 1000)]

        return keys

    def count_files(self, origin: str, prefix: str = "") -> int:
        """ Count files in a Share

        :param origin: (str) Share to count files of
        :param prefix: (str) Prefix the file must start with
        :return: (int) Number of files
        """
        all = self._list_all_files(origin)
        keys = [arch for arch in all if arch.startswith(prefix)]
        self.logger.debug("Counting files...")
        file_count = len(keys)
        return file_count

    @staticmethod
    def is_directory_or_file(path: str) -> Tuple[list, list]:
        """ Identify folders and file

        :param path: (str) representing the file's location.
        :return: (tuple) Two lists. folders with folder names, and file with the file name.
        """
        folders = []
        file = []
        separator = "/"
        path = path.replace("\\", "/")
        list = path.split(separator)
        for i in list:
            if not re.findall(r"^[^\.]+\.[^\.]+$", i):
                folders.append(i)
            else:
                file.append(i)
        return folders, file


def _assert_has_slash(path: str) -> str:
    """ Add a slash at the end of the path if it doesn't have it

    :param path: (str) Path to add the slash
    :return: (str) Path with slash at the end
    """
    return path if _is_directory(path) else path + "/"


def _is_directory(path: str) -> bool:
    """ Check if the path is a directory

    :param path: (str) Path to check
    :return: (bool) True if the path is a directory
    """
    return path[-1] == "/"
