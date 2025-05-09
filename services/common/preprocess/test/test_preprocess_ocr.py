### This code is property of the GGAO ###

import pytest
from unittest.mock import patch
import datetime
from preprocess_ocr import get_ocr_files, chunk, sum_up_size, insert_at_rate, format_path_files

@pytest.fixture
def mock_dependencies():
    with patch('preprocess_ocr.extract_ocr_files') as mock_extract_ocr_files, \
         patch('preprocess_ocr.format_path_files') as mock_format_path_files, \
         patch('preprocess_ocr.get_exc_info') as mock_get_exc_info, \
         patch('preprocess_ocr.LoggerHandler') as mock_logger_handler:
        yield {
            'extract_ocr_files': mock_extract_ocr_files,
            'format_path_files': mock_format_path_files,
            'get_exc_info': mock_get_exc_info,
            'logger_handler': mock_logger_handler
        }

def test_get_ocr_files(mock_dependencies):
    mock_dependencies['extract_ocr_files'].return_value = ([], [], [], [], [], [])
    mock_dependencies['format_path_files'].side_effect = lambda x, y, z: x

    result = get_ocr_files([], 'ocr', {'images': '', 'text': '', 'cells': '', 'tables': ''})

    assert result == {
        'text': [],
        'cells': [],
        'paragraphs': [],
        'words': [],
        'tables': [],
        'lines': []
    }

def test_sum_up_size():
    sizes = [1048576, 2097152, 3145728]  # 1MB, 2MB, 3MB in bytes

    result = sum_up_size(sizes)

    assert result == 6.0

def test_insert_at_rate():
    requests = [(1, datetime.datetime.now() - datetime.timedelta(seconds=10))]
    count = 1
    rate = 2
    period = 5

    with patch('preprocess_ocr.time.sleep', return_value=None):
        result = insert_at_rate(requests, count, rate, period)

    assert len(result) == 2
    assert result[-1][0] == count
    assert isinstance(result[-1][1], datetime.datetime)

def test_format_path_files():
    files = ['path/to/file1', 'path/to/file2']
    prefix_image = 'images'
    prefix_folder = 'folder'

    result = format_path_files(files, prefix_image, prefix_folder)

    expected = [
        ('folder/ocr/file1', 'path/to/file1'),
        ('folder/ocr/file2', 'path/to/file2')
    ]

    assert result == expected

def test_get_ocr_files_exception(mock_dependencies):
    mock_dependencies['extract_ocr_files'].side_effect = Exception("Test error")
    
    with patch('preprocess_ocr.logger') as mock_logger:
        with pytest.raises(Exception) as exc_info:
            get_ocr_files([], 'ocr', {'images': '', 'text': '', 'cells': '', 'tables': ''})
        
        mock_logger.error.assert_called_once_with(
            "Error extracting files from OCR with library.",
            exc_info=mock_dependencies['get_exc_info'].return_value
        )
    
    mock_dependencies['get_exc_info'].assert_called_once()

def test_chunk_normal_case():
    files = ['file1.txt', 'file2.txt', 'file3.txt']
    sizes = [100, 200, 300]
    max_size = 1000
    max_length = 10
    
    with patch('preprocess_ocr.logger') as mock_logger:
        result = list(chunk(files, sizes, max_size, max_length))
    
    assert len(result) == 1
    assert len(result[0]) == 3
    assert result[0][0]['number'] == 0
    assert result[0][0]['filename'] == 'file1.txt'
    assert result[0][0]['size'] == 100
    assert result[0][1]['number'] == 1
    assert result[0][1]['filename'] == 'file2.txt'
    assert result[0][1]['size'] == 200
    assert result[0][2]['number'] == 2
    assert result[0][2]['filename'] == 'file3.txt'
    assert result[0][2]['size'] == 300

def test_chunk_max_length_reached():
    files = ['file1.txt', 'file2.txt', 'file3.txt']
    sizes = [100, 100, 100]
    max_size = 1000  
    max_length = 2  
    
    with patch('preprocess_ocr.logger') as mock_logger:
        result = list(chunk(files, sizes, max_size, max_length))
    
    assert len(result) == 2
    
    assert len(result[0]) == 2
    assert result[0][0]['number'] == 0
    assert result[0][0]['filename'] == 'file1.txt'
    assert result[0][0]['size'] == 100
    assert result[0][1]['number'] == 1
    assert result[0][1]['filename'] == 'file2.txt'
    assert result[0][1]['size'] == 100
    
    assert len(result[1]) == 1
    assert result[1][0]['number'] == 2
    assert result[1][0]['filename'] == 'file3.txt'
    assert result[1][0]['size'] == 100

def test_chunk_empty_input():
    files = []
    sizes = []
    max_size = 1000
    max_length = 10
    
    with patch('preprocess_ocr.logger') as mock_logger:
        result = list(chunk(files, sizes, max_size, max_length))
    
    assert len(result) == 1
    assert len(result[0]) == 0

def test_chunk_exception_handling():
    files = ['file1.txt']
    sizes = [100]
    max_size = 1000
    max_length = 10
    
    with patch('preprocess_ocr.zip') as mock_zip:
        mock_zip.side_effect = Exception("Test error")
        with patch('preprocess_ocr.logger') as mock_logger:
            with patch('preprocess_ocr.get_exc_info') as mock_get_exc_info:
                result = list(chunk(files, sizes, max_size, max_length))
                
                mock_logger.error.assert_called_once_with(
                    "Error chunking files.",
                    exc_info=mock_get_exc_info.return_value
                )
                
                mock_get_exc_info.assert_called_once()
    
    assert len(result) == 0
