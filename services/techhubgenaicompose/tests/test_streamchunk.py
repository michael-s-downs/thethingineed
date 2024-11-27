### This code is property of the GGAO ###

import pytest 

from statistics import mean 

from common.errors.genaierrors import GenaiError 

# Importar la clase StreamChunk 
from  compose.streamchunk import StreamChunk 

  

def test_stream_chunk_initialization(): 

    response_dict = { 

        "content": "Test content", 

        "meta": {"author": "Fernando Alonso"}, 

        "scores": {"accuracy": 0.8, "relevance": 0.9}, 

        "answer": "Test answer", 

        "tokens": 100 

    } 

    chunk = StreamChunk(response_dict) 

      

    assert chunk.content == "Test content" 

    assert chunk.meta == {"author": "Fernando Alonso"} 

    assert chunk.scores == {"accuracy": 0.8, "relevance": 0.9} 

    assert chunk.answer == "Test answer" 

    assert chunk.tokens == 100 

  

def test_get_mean_score(): 

    response_dict = { 

        "scores": {"accuracy": 0.8, "relevance": 0.9, "fluency": 0.7} 

    } 

    chunk = StreamChunk(response_dict) 

    assert chunk.get_mean_score() == mean([0.8, 0.9, 0.7]) 

  

def test_get_metadata(): 

    response_dict = { 

        "meta": {"author": "Fernando Alonso", "length": "short"} 

    } 

    chunk = StreamChunk(response_dict) 

      

    assert chunk.get_metadata("author") == "Fernando Alonso" 

      

    with pytest.raises(GenaiError): 

        chunk.get_metadata("nonexistent") 

  

def test_get(): 

    response_dict = { 

        "content": "Test content", 

        "meta": {"author": "Fernando Alonso"}, 

        "scores": {"accuracy": 0.8, "relevance": 0.9}, 

        "answer": "Test answer" 

    } 

    chunk = StreamChunk(response_dict) 

      

    assert chunk.get("content") == "Test content" 

    assert chunk.get("answer") == "Test answer" 

    assert chunk.get("metadata") == {"author": "Fernando Alonso"} 

    assert chunk.get("scores") == {"accuracy": 0.8, "relevance": 0.9} 

      

    with pytest.raises(GenaiError): 

        chunk.get("nonexistent") 