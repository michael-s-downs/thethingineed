### This code is property of the GGAO ###

import pytest
from unittest.mock import patch, mock_open
from common.preprocess.preprocess_extract import (
    check_corrupted,
    get_num_pages,
    extract_text,
    extract_images,
    clean_text,
    extract_images_conditional
)
from pdfminer.pdfparser import PDFSyntaxError

def test_check_corrupted():
    # Case 1: Empty text
    text = ""
    corrupted_symbols_prop = 0.1
    corrupted_len_words_prop = 0.1
    result = check_corrupted(text, corrupted_symbols_prop, corrupted_len_words_prop)
    assert result == True

    # Case 2: All characters are binary
    text = "\x0c\x0c\x0c"
    result = check_corrupted(text, corrupted_symbols_prop, corrupted_len_words_prop)
    assert result == True

    # Case 3: High percentage of non-alphanumeric characters
    text = "!!!@@@###"
    result = check_corrupted(text, corrupted_symbols_prop, corrupted_len_words_prop)
    assert result == True

    # Case 4: High percentage of words with length different from 2-15 characters
    text = "a " * 100
    result = check_corrupted(text, corrupted_symbols_prop, corrupted_len_words_prop)
    assert result == True

    # Case 5: Scanned text
    text = "Scanned by CamScanner"
    result = check_corrupted(text, corrupted_symbols_prop, corrupted_len_words_prop)
    assert result == True

    # Case 6: Valid text
    text = "This is a valid text"
    result = check_corrupted(text, corrupted_symbols_prop, corrupted_len_words_prop)
    assert result == False

    # Case 7: Exception during processing
    with patch('common.preprocess.preprocess_extract.get_exc_info', side_effect=Exception("Test exception")):
        text = "This will cause an exception"
        result = check_corrupted(text, corrupted_symbols_prop, corrupted_len_words_prop)
        assert result == False

def test_get_num_pages():
    filename = "test.pdf"
    page_limit = 10

    # Case 1: PDF file with number of pages greater than the limit
    with patch('common.preprocess.preprocess_extract.get_mimetype', return_value='pdf'):
        with patch('common.preprocess.preprocess_extract.get_number_pages', return_value=15):
            result = get_num_pages(filename, page_limit)
            assert result == 10

    # Case 2: PDF file with number of pages less than the limit
    with patch('common.preprocess.preprocess_extract.get_mimetype', return_value='pdf'):
        with patch('common.preprocess.preprocess_extract.get_number_pages', return_value=5):
            result = get_num_pages(filename, page_limit)
            assert result == 5

    # Case 3: Non-PDF file
    with patch('common.preprocess.preprocess_extract.get_mimetype', return_value='txt'):
        result = get_num_pages(filename, page_limit)
        assert result == 1

def test_extract_text():
    file = "test.pdf"
    num_pags = 5
    generic = {
        'project_conf': {
            'laparams': {'detect_vertical': True},
            'process_type': 'ir_index'
        },
        'preprocess_conf': {
            'num_pag_ini': 1,
            'page_limit': 10,
            'corrupt_th_chars': 0.5,
            'corrupt_th_words': 0.6
        }
    }
    specific = {
        'document': {
            'metadata': {'title': 'Test Document'}
        }
    }
    
    # Define empty_result for comparison
    empty_result = {'lang': "", 'text': "", 'extraction': {}, 'boxes': [], 'cells': [], 'lines': []}
    
    # Case 2: Allowed file, successful extraction, non-corrupt text, ir_index process
    return_dict = {}
    mock_extraction = {"text": "This is a test document"}
    mock_boxes = ["box1"]
    mock_cells = ["cell1"]
    mock_lines = ["line1"]
    
    with patch('common.preprocess.preprocess_extract.get_mimetype', return_value='pdf'):
        with patch('common.preprocess.preprocess_extract.get_texts_from_file', 
                  return_value=(mock_extraction, mock_boxes, mock_cells, mock_lines)):
            with patch('common.preprocess.preprocess_extract.clean_text', return_value="This is a test document"):
                with patch('common.preprocess.preprocess_extract.get_language', return_value="en"):
                    with patch('common.preprocess.preprocess_extract.check_corrupted', return_value=False):
                        with patch('common.preprocess.preprocess_extract.format_indexing_metadata', 
                                  return_value="Indexed: This is a test document"):
                            extract_text(file, num_pags, generic, specific, return_dict=return_dict)
                            
                            assert return_dict['lang'] == "en"
                            assert return_dict['text'] == "Indexed: This is a test document"
                            assert return_dict['extraction'] == mock_extraction
                            assert return_dict['boxes'] == mock_boxes
                            assert return_dict['cells'] == mock_cells
                            assert return_dict['lines'] == mock_lines
    
    # Case 3: Allowed file, PDFSyntaxError and recovery
    return_dict = {}
    pdf_content = b"junk data%PDF-content"
    mock_file = mock_open(read_data=pdf_content)
    
    with patch('common.preprocess.preprocess_extract.get_mimetype', return_value='pdf'):
        with patch('common.preprocess.preprocess_extract.get_texts_from_file',
                  side_effect=[PDFSyntaxError("Test error"), 
                              (mock_extraction, mock_boxes, mock_cells, mock_lines)]):
            with patch('builtins.open', mock_file):
                with patch('common.preprocess.preprocess_extract.clean_text', return_value="This is a test document"):
                    with patch('common.preprocess.preprocess_extract.get_language', return_value="en"):
                        with patch('common.preprocess.preprocess_extract.check_corrupted', return_value=False):
                            with patch('common.preprocess.preprocess_extract.format_indexing_metadata', 
                                      return_value="Indexed: This is a test document"):
                                extract_text(file, num_pags, generic, specific, return_dict=return_dict)
                                
                                assert return_dict['lang'] == "en"
                                assert return_dict['text'] == "Indexed: This is a test document"
    
    # Case 4: Allowed file, text in bytes
    return_dict = {}
    mock_extraction_bytes = {"text": b"This is a test document"}
    
    with patch('common.preprocess.preprocess_extract.get_mimetype', return_value='pdf'):
        with patch('common.preprocess.preprocess_extract.get_texts_from_file', 
                  return_value=(mock_extraction_bytes, mock_boxes, mock_cells, mock_lines)):
            with patch('common.preprocess.preprocess_extract.clean_text', return_value="This is a test document"):
                with patch('common.preprocess.preprocess_extract.get_language', return_value="en"):
                    with patch('common.preprocess.preprocess_extract.check_corrupted', return_value=False):
                        with patch('common.preprocess.preprocess_extract.format_indexing_metadata', 
                                  return_value="Indexed: This is a test document"):
                            extract_text(file, num_pags, generic, specific, return_dict=return_dict)
                            
                            assert return_dict['text'] == "Indexed: This is a test document"
    
    # Case 5: Allowed file, error detecting language
    return_dict = {}
    
    with patch('common.preprocess.preprocess_extract.get_mimetype', return_value='pdf'):
        with patch('common.preprocess.preprocess_extract.get_texts_from_file', 
                  return_value=(mock_extraction, mock_boxes, mock_cells, mock_lines)):
            with patch('common.preprocess.preprocess_extract.clean_text', return_value="This is a test document"):
                with patch('common.preprocess.preprocess_extract.get_language', side_effect=Exception("Test exception")):
                    with patch('common.preprocess.preprocess_extract.check_corrupted', return_value=False):
                        with patch('common.preprocess.preprocess_extract.format_indexing_metadata', 
                                  return_value="Indexed: This is a test document"):
                            extract_text(file, num_pags, generic, specific, return_dict=return_dict)
                            
                            assert return_dict['lang'] == "default"
    
    # Case 6: Allowed file, Japanese language
    return_dict = {}
    
    with patch('common.preprocess.preprocess_extract.get_mimetype', return_value='pdf'):
        with patch('common.preprocess.preprocess_extract.get_texts_from_file', 
                  return_value=(mock_extraction, mock_boxes, mock_cells, mock_lines)):
            with patch('common.preprocess.preprocess_extract.clean_text', return_value="This is a test document"):
                with patch('common.preprocess.preprocess_extract.get_language', return_value="ja"):
                    with patch('common.preprocess.preprocess_extract.check_corrupted', return_value=False):
                        with patch('common.preprocess.preprocess_extract.format_indexing_metadata', 
                                  return_value="Indexed: This is a test document"):
                            extract_text(file, num_pags, generic, specific, return_dict=return_dict)
                            
                            assert return_dict['lang'] == "ja"
    
    # New Case 7: Allowed file, get_language returns None
    return_dict = {}
    
    with patch('common.preprocess.preprocess_extract.get_mimetype', return_value='pdf'):
        with patch('common.preprocess.preprocess_extract.get_texts_from_file', 
                  return_value=(mock_extraction, mock_boxes, mock_cells, mock_lines)):
            with patch('common.preprocess.preprocess_extract.clean_text', return_value="This is a test document"):
                with patch('common.preprocess.preprocess_extract.get_language', return_value=None):
                    with patch('common.preprocess.preprocess_extract.check_corrupted', return_value=False):
                        with patch('common.preprocess.preprocess_extract.format_indexing_metadata', 
                                  return_value="Indexed: This is a test document"):
                            extract_text(file, num_pags, generic, specific, return_dict=return_dict)
                            
                            assert return_dict['lang'] == "default"
                            assert return_dict['text'] == "Indexed: This is a test document"

def test_extract_text_exception_handling():
    file = "test.pdf"
    num_pags = 5
    generic = {
        'project_conf': {
            'laparams': {'detect_vertical': True},
            'process_type': 'ir_index'
        },
        'preprocess_conf': {
            'num_pag_ini': 1,
            'page_limit': 10,
            'corrupt_th_chars': 0.5,
            'corrupt_th_words': 0.6
        }
    }
    specific = {
        'document': {
            'metadata': {'title': 'Test Document'}
        }
    }
    
    return_dict = {}
    empty_result = {'lang': "", 'text': "", 'extraction': {}, 'boxes': [], 'cells': [], 'lines': []}
    
    with patch('common.preprocess.preprocess_extract.get_mimetype', side_effect=Exception("Test exception")):
        extract_text(file, num_pags, generic, specific, return_dict=return_dict)
        
        assert return_dict == empty_result
                            
def test_extract_images():
    filename = "test.pdf"
    generic = {'preprocess_conf': {'num_pag_ini': 1, 'page_limit': 10}}

    # Case 1: Successful image extraction
    with patch('common.preprocess.preprocess_extract.get_images_from_file', return_value=[{'filename': 'image1.png'}]):
        result = extract_images(filename, generic)
        assert result == [{'filename': 'image1.png'}]

    # Case 2: Exception during image extraction
    with patch('common.preprocess.preprocess_extract.get_images_from_file', side_effect=Exception("Test exception")):
        with patch('common.preprocess.preprocess_extract.get_exc_info', return_value="Exception info"):
            with patch('common.preprocess.preprocess_extract.logger.warning') as mock_logger:
                result = extract_images(filename, generic)
                assert result == []
                mock_logger.assert_called_once_with(f"Error extracting images in uhis-sdk-services from {filename}.", exc_info="Exception info")

    # Case 3: No images found
    with patch('common.preprocess.preprocess_extract.get_images_from_file', return_value=[]):
        result = extract_images(filename, generic)
        assert result == []

def test_clean_text():
    text = "This is a test text (cid:1234)"
    result = clean_text(text)
    assert result == "This is a test text"

def test_extract_images_conditional():
    generic = {'preprocess_conf': {'num_pag_ini': 1, 'page_limit': 10}}
    specific = {'path_img': 'path/to/images'}
    workspace = "workspace"
    filename = "test.pdf"
    folder_file = "folder"
    with patch('common.preprocess.preprocess_extract.extract_images', return_value=[{'filename': 'image1.png'}]):
        with patch('common.preprocess.preprocess_extract.upload_batch_files_async') as mock_upload:
            result = extract_images_conditional(generic, specific, workspace, filename, folder_file)
            assert result == [{'filename': 'path/to/images/pags/image1.png'}]
            mock_upload.assert_called_once()
