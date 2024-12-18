### This code is property of the GGAO ###


# Native imports
import importlib
import logging
import mimetypes
from typing import Tuple

# Installed imports
import magic

# Custom imports
from genai_sdk_services.resources.import_user_functions import import_user_functions
from genai_sdk_services.services.file import BaseFileService, ImageService, PdfService, TxtService, WordService, ExcelService, PowerPointService

FILE_FORMATS_MAP = {
    'image/jpeg': "jpeg",
    'image/png': "png",
    'image/svg+xml': "svg",
    'image/tiff': "tiff",
    'application/postscript': "ps",
    'application/pdf': "pdf",
    'text': "txt",
    'text/plain': "plain",
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document': "docx",
    'application/msword': "doc",
    'application/vnd.openxmlformats-officedocument.presentationml.presentation': "pptx",
    'application/vnd.ms-powerpoint': "ppt",
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': "xlsx",
    'application/vnd.ms-excel': "xls"
}


class FilesController(object):

    def __init__(self, config: dict = None):
        """ Init the controller with the given configuration

        param config: Configuration of the controller. Can include user functions
        """

        self.services = [ImageService, PdfService, TxtService, WordService, ExcelService, PowerPointService]
        self.user_functions_services = []
        self.origins = {}

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

    def set_user_functions(self, user_functions: dict = None):
        """ Set the user custom functions

        :param user_functions: (dict) User functions to be imported
        """
        if user_functions is None:
            user_functions = []

        if len(user_functions) == 0:
            self.user_functions_services = user_functions
        else:
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

    def _get_file(self, type_file: str) -> BaseFileService:
        """ Get the service of the given type

        :param type_file: (str) File type to get the service of
        :return: <BaseFileService> Service to use
        """
        for file in self.user_functions_services:
            self.logger.debug(file)
            if file.check_file(type_file):
                self.logger.debug(f"Using user function {file}")
                return file()

        for file in self.services:
            if file.check_file(type_file):
                return file()

        raise ValueError("Type not supported")

    def get_type(self, filename: str) -> str:
        """ Get the type of the file

        :param filename: (str) Name of the file to get type of
        :return: (str) Type of the file
        """
        if type(filename) is list:
            filename = filename[0]

        try:
            mime = magic.from_buffer(open(filename, "rb").read(1024*1024), mime=True)
            if mime.startswith("text/"):
                mime = "text"
            return FILE_FORMATS_MAP.get(mime, filename.split(".")[-1])
        except AttributeError:
            self.logger.exception(f"Error while getting type of {filename}")
            return ""

    def get_text(self, file: str, **kwargs) -> Tuple[dict, list, list, list]:
        """ Get the text from a single file

        :param file: (str) File to read
        :param kwargs: (dict) Additional arguments
        :return: (tuple) Texts of the file
        """
        self.logger.debug("Controller - Getting text from %s" % file)
        type_ = self.get_type(file)
        if type_ not in self.origins:
            self.origins[type_] = self._get_file(type_)
        try:
            return self.origins[type_].get_text(file, **kwargs)
        except ValueError as ex:
            self.logger.exception("Error while getting text")
            raise ex

    def get_multiple_text(self, files: list, **kwargs) -> Tuple[list, list, list, list, list, list]:
        """ Get texts from multiple files

        :param files: (str) Files to read
        :param kwargs: (dict) Additional arguments
        :return: (tuple) Lists with the texts of the files
        """
        self.logger.debug("Controller - Getting text from %s" % files)
        type_ = self.get_type(files[0])
        if type_ not in self.origins:
            self.origins[type_] = self._get_file(type_)
        try:
            return self.origins[type_].get_multiple_text(files, **kwargs)
        except Exception as ex:
            self.logger.exception("Error while getting text")
            raise ex

    def get_text_from_bytes(self, file: bytes, type_file: str, **kwargs) -> Tuple[dict, list, list, list]:
        """ Get text from a buffered file

        :param file: (bytes) Buffer (in bytes) to get text from
        :param type_file: (str) Type of the file
        :param kwargs: (dict) Additional arguments
        :return: (tuple) Lists with the texts of the files
        """
        self.logger.debug("Controller - Getting text from %s" % file)
        if type_file not in self.origins:
            self.origins[type_file] = self._get_file(type_file)
        try:
            return self.origins[type_file].get_text_from_bytes(file, **kwargs)
        except Exception as ex:
            self.logger.exception("Error while getting text from bytes")
            raise ex

    def extract_text(self, file: str, **kwargs) -> Tuple[str, list, list, list, list]:
        """ Extract text from a single file and stores it into a text file

        :param file: (str) File to extract text from
        :param kwargs: (dict) Additional arguments
        :return: (tuple) Filename and lists with pages, blocks, lines and words
        """
        self.logger.debug("Controller - Extracting texts from %s" % file)
        type_ = self.get_type(file)
        if type_ not in self.origins:
            self.origins[type_] = self._get_file(type_)
        try:
            return self.origins[type_].extract_text(file, **kwargs)
        except Exception as ex:
            self.logger.exception("Error while extracting text")
            raise ex

    def extract_multiple_text(self, files: list, **kwargs) -> Tuple[list, list, list, list, list, list]:
        """ Extract text from multiple files and stores them into text files

        :param files: (list) Files to extract text from
        :param kwargs: (dict) Additional arguments
        :return: (tuple) Path to texts
        """
        self.logger.debug("Controller - Extracting texts from %s" % files)
        type_ = self.get_type(files[0])
        if type_ not in self.origins:
            self.origins[type_] = self._get_file(type_)
        try:
            return self.origins[type_].extract_multiple_text(files, **kwargs)
        except Exception as ex:
            self.logger.exception("Error while extracting text")
            raise ex

    def extract_images(self, file: str, **kwargs) -> list:
        """ Extract images from file and stores them

        :param file: (str) File to extract images from
        :param kwargs: (dict) Additional arguments
        :return: (list) List of dicts containing filename and number of the pagemthe image corresponds to
        """
        self.logger.debug("Controller - Extracting images from %s" % file)
        type_ = self.get_type(file)
        if type_ not in self.origins:
            self.origins[type_] = self._get_file(type_)
        try:
            return self.origins[type_].extract_images(file, **kwargs)
        except Exception as ex:
            self.logger.exception("Error while extracting images")
            raise ex

    def write(self, text: str, filename: str):
        """ Write text to filename

        :param text: (str) Text to write
        :param filename: (str) Filename to write text into
        """
        self.logger.debug("Controller - Writing text to %s" % filename)
        type_ = self.get_type(filename)
        if type_ not in self.origins:
            self.origins[type_] = self._get_file(type_)
        try:
            return self.origins[type_].write(text, filename)
        except Exception as ex:
            self.logger.error("Error while writing file")
            self.logger.error("Exception", exc_info=True)
            raise ex

    def get_number_pages(self, filename: str) -> int:
        """ Get number of pages in filename

        :param filename: (str) Filename to get number of pages of
        :return: (int) Number of pages in filename
        """
        self.logger.debug("Controller - Writing text to %s" % filename)
        type_ = self.get_type(filename)
        if type_ not in self.origins:
            self.origins[type_] = self._get_file(type_)
        try:
            return self.origins[type_].get_number_pages(filename)
        except Exception as ex:
            self.logger.error("Error while getting number of pages")
            self.logger.error("Exception", exc_info=True)
            raise ex

    def check_type(self, filename: str) -> bool:
        """ Check if the type of the file is available

        :param filename: (str) Name of the file to check
        :return: (bool) True if the type is supported: ("jpeg", "jpg", "png", "svg", "tiff", "svg", "ps", "pdf", "txt", "plain"
        """
        return self.get_type(filename) in self.services
