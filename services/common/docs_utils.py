### This code is property of the GGAO ###


# Native imports
from io import BytesIO
from typing import Tuple

# Installed imports
from PIL import Image


# Global vars
formats_plain = ["txt", "csv", "tsv", "json"]
formats_image = ["jpeg", "jpg", "png", "bmp", "tiff", "tif"]
formats_office = ["doc", "docx", "xls", "xlsx", "ppt", "pptx"]

formats_convert = ["bmp", "tiff", "tif"]
formats_supported = ["pdf", "jpeg", "jpg", "png", "txt", "docx", "xls", "xlsx", "pptx"]
formats_pass = formats_supported + []
formats_acceptable = formats_pass + formats_convert

file_formats_map = {
    'application/pdf': ".pdf",
    'image/jpg': ".jpg",
    'image/png': ".png",
    'text': ".txt",
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document': ".docx",
    'application/msword': ".doc",
    'application/vnd.openxmlformats-officedocument.presentationml.presentation': ".pptx",
    'application/vnd.ms-powerpoint': ".ppt",
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': ".xlsx",
    'application/vnd.ms-excel': ".xsl"
}


def parse_file_name(file: str, folder: str) -> str:
    """ Get file name from file path
    and parse to original file name

    :param file: File path
    :param folder: Folder path
    :return: File name parsed
    """
    file = file.replace("_parsed.pdf", "")
    file_name = file[file.startswith(folder) and len(folder):].lstrip("/")
    return file_name

def image_to_pdf(image: bytes) -> bytes:
    """ Convert image to PDF

    :param image: Image to convert
    :return: Image converted to PDF
    """
    buf = BytesIO()
    Image.open(BytesIO(image)).convert("RGB").save(buf, format="pdf")
    pdf = buf.getvalue()

    return pdf

def document_conversion(file_name: str, file_bytes: bytes) -> Tuple[str, bytes]:
    """ Convert document into a supported format

    :param file_name: Name of file
    :param file_bytes: File content in bytes
    :return: Name and content of file converted
    """
    extension = file_name.split(".")[-1].lower()

    if extension in formats_convert:
        if extension in formats_image:
            file_name = f"{file_name}_parsed.pdf"
            file_bytes = image_to_pdf(file_bytes)

    return file_name, file_bytes
