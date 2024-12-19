### This code is property of the GGAO ###

import pytest
from io import BytesIO
from PIL import Image
from docs_utils import (
    parse_file_name,
    image_to_pdf,
    document_conversion,
)

# Mocks and Global Variables
folder = "/test_folder/"
valid_image_bytes = BytesIO()
Image.new("RGB", (100, 100)).save(valid_image_bytes, format="BMP")  # Create sample BMP image
valid_image_bytes = valid_image_bytes.getvalue()

valid_png_bytes = BytesIO()
Image.new("RGB", (100, 100)).save(valid_png_bytes, format="PNG")
valid_png_bytes = valid_png_bytes.getvalue()


# Test parse_file_name
def test_parse_file_name():
    assert parse_file_name("/test_folder/file_parsed.pdf", folder) == "file"
    assert parse_file_name("/test_folder/file.txt", folder) == "file.txt"
    assert parse_file_name("other_folder/file_parsed.pdf", folder) == "other_folder/file"
    assert parse_file_name("file_parsed.pdf", "") == "file"
    assert parse_file_name("/test_folder/subfolder/file_parsed.pdf", folder) == "subfolder/file"


# Test image_to_pdf
def test_image_to_pdf_valid_image():
    # Test with valid BMP image bytes
    pdf_bytes = image_to_pdf(valid_image_bytes)
    assert isinstance(pdf_bytes, bytes)
    assert pdf_bytes.startswith(b"%PDF")  # PDF files start with %PDF

def test_image_to_pdf_invalid_image():
    # Test with invalid image bytes
    with pytest.raises(OSError):
        image_to_pdf(b"invalid_image_data")


# Test document_conversion
def test_document_conversion_bmp_to_pdf():
    file_name, file_bytes = document_conversion("sample.bmp", valid_image_bytes)
    assert file_name == "sample.bmp_parsed.pdf"
    assert file_bytes.startswith(b"%PDF")

def test_document_conversion_tiff_to_pdf():
    tiff_bytes = BytesIO()
    Image.new("RGB", (50, 50)).save(tiff_bytes, format="TIFF")
    tiff_bytes = tiff_bytes.getvalue()

    file_name, file_bytes = document_conversion("image.tiff", tiff_bytes)
    assert file_name == "image.tiff_parsed.pdf"
    assert file_bytes.startswith(b"%PDF")

def test_document_conversion_non_convertible_format():
    file_bytes = b"sample text file"
    file_name, returned_bytes = document_conversion("document.txt", file_bytes)
    assert file_name == "document.txt"
    assert returned_bytes == file_bytes

def test_document_conversion_png_no_conversion():
    file_name, file_bytes = document_conversion("image.png", valid_png_bytes)
    assert file_name == "image.png"
    assert file_bytes == valid_png_bytes

def test_document_conversion_unsupported_extension():
    file_name, file_bytes = document_conversion("file.xyz", b"unknown format")
    assert file_name == "file.xyz"
    assert file_bytes == b"unknown format"
