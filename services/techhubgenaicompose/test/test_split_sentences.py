### This code is property of the GGAO ###

import pytest

from compose.utils.split_sentences import _split_sentences, split_by_word_respecting_sent_boundary

import nltk
nltk.download('punkt')


def test_split_sentences_basic():
    text = "This is a sentence. This is another sentence."
    sentences = _split_sentences(text)
    assert sentences == ["This is a sentence. ", "This is another sentence."]

def test_split_sentences_empty_input():
    text = ""
    sentences = _split_sentences(text)
    assert sentences == []

def test_split_sentences_single_sentence():
    text = "This is the only sentence."
    sentences = _split_sentences(text)
    assert sentences == ["This is the only sentence."]


def test_split_by_word_respecting_sent_boundary_short_text():
    text = "Short text."
    split_length = 10
    split_overlap = 0
    splits, pages, start_idxs = split_by_word_respecting_sent_boundary(text, split_length, split_overlap)
    
    assert splits == ["Short text."]
    assert pages == [1]
    assert start_idxs == [0]

def test_split_by_word_respecting_sent_boundary_overlap_exceed_split():
    text = "This is a sentence."
    split_length = 5
    split_overlap = 10  # Overlap is larger than the split length
    splits, pages, start_idxs = split_by_word_respecting_sent_boundary(text, split_length, split_overlap)
    
    # In this case, the entire text should still be returned as one split
    assert splits == ["This is a sentence."]
    assert pages == [1]
    assert start_idxs == [0]

def test_split_by_word_respecting_sent_boundary_exact_word_limit():
    text = "This is sentence one. This is sentence two."
    split_length = 5
    split_overlap = 0
    splits, pages, start_idxs = split_by_word_respecting_sent_boundary(text, split_length, split_overlap)
    
    assert splits == ["This is sentence one. ", "This is sentence two."]
    assert pages == [1, 1]
    assert start_idxs == [0, 22]

def test_split_by_word_respecting_sent_boundary_large_text():
    text = "Sentence one. Sentence two. Sentence three. Sentence four. Sentence five."
    split_length = 5
    split_overlap = 2
    splits, pages, start_idxs = split_by_word_respecting_sent_boundary(text, split_length, split_overlap)
    
    assert splits == ["Sentence one. Sentence two. ", "Sentence two. Sentence three. ", "Sentence three. Sentence four. ", "Sentence four. Sentence five."]
    assert pages == [1, 1, 1, 1]
    assert start_idxs == [0, 14, 28, 44]

def test_split_by_word_respecting_sent_boundary_no_overlap():
    text = "Sentence one. Sentence two. Sentence three. Sentence four."
    split_length = 4
    split_overlap = 0
    splits, pages, start_idxs = split_by_word_respecting_sent_boundary(text, split_length, split_overlap)
    
    assert splits == ["Sentence one. Sentence two. ", "Sentence three. Sentence four."]
    assert pages == [1, 1]
    assert start_idxs == [0, 28]


def test_split_by_word_respecting_sent_boundary_single_sentence():
    text = "A single sentence."
    split_length = 10
    split_overlap = 0
    splits, pages, start_idxs = split_by_word_respecting_sent_boundary(text, split_length, split_overlap)
    
    assert splits == ["A single sentence."]
    assert pages == [1]
    assert start_idxs == [0]
