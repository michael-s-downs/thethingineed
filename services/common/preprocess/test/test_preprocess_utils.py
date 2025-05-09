### This code is property of the GGAO ###

import pytest
from common.preprocess.preprocess_utils import format_indexing_metadata, get_language

def test_format_indexing_metadata():
    text = "This is a test text."
    filename = "testfile.txt"
    num_pags = 10
    metadata = {"author": "John Doe", "year": "2023"}
    
    result = format_indexing_metadata(text, filename, num_pags, metadata)
    
    expected = f"testfile.txt\tThis is a test text.\ten\t10\tauthor: John Doe\tyear: 2023"
    
    assert result == expected

def test_format_indexing_metadata_with_special_chars():
    text = "This is a test\ntext with\ttabs\rand newlines."
    filename = "testfile.txt"
    num_pags = 10
    metadata = {"author": "John Doe", "year": "2023"}
    
    result = format_indexing_metadata(text, filename, num_pags, metadata)
    
    expected = f"testfile.txt\tThis is a test\\\\ntext with\\\\ttabs\\\\rand newlines.\ten\t10\tauthor: John Doe\tyear: 2023"
    
    assert result == expected

def test_format_indexing_metadata_empty_metadata():
    text = "This is a test text."
    filename = "testfile.txt"
    num_pags = 10
    metadata = {}
    
    result = format_indexing_metadata(text, filename, num_pags, metadata)
    expected = f"testfile.txt\tThis is a test text.\ten\t10"
    
    assert result == expected

def test_format_indexing_metadata_none_metadata():
    text = "This is a test text."
    filename = "testfile.txt"
    num_pags = 10
    metadata = None
    
    result = format_indexing_metadata(text, filename, num_pags, metadata)
    expected = f"testfile.txt\tThis is a test text.\ten\t10"
    
    assert result == expected

def test_format_indexing_metadata_trailing_tab():
    text = "Test"
    filename = "file.txt"
    num_pags = 1
    metadata = {"key": ""} 
    
    result = format_indexing_metadata(text, filename, num_pags, metadata)
    expected = f"file.txt\tTest\tet\t1\tkey: "  
    
    assert result == expected
    
    assert result == expected

def test_get_language():
    text = "This is a test text."
    result = get_language(text)
    assert result == "en"

    result_with_acc = get_language(text, return_acc=True)
    assert result_with_acc[0] == "en"
    assert isinstance(result_with_acc[1], float)
    
    possible_langs = ["en", "es"]
    result_with_possible_langs = get_language(text, possible_langs=possible_langs)
    assert result_with_possible_langs == "en"
    
    result_with_possible_langs_and_acc = get_language(text, return_acc=True, possible_langs=possible_langs)
    assert result_with_possible_langs_and_acc[0] == "en"
    assert isinstance(result_with_possible_langs_and_acc[1], float)
    
    text_unknown = "asdfghjkl"
    result_unknown = get_language(text_unknown, possible_langs=["fr", "de"])
    assert result_unknown == "unknown"
    
    result_unknown_with_acc = get_language(text_unknown, return_acc=True, possible_langs=["fr", "de"])
    assert result_unknown_with_acc[0] == "unknown"
    assert result_unknown_with_acc[1] == 0.0
    
    text_es = "Esto es una prueba en español."
    result_es = get_language(text_es)
    assert result_es == "es"