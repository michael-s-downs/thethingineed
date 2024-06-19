### This code is property of the GGAO ###


# Native imports
from functools import reduce
from typing import Tuple

# Installed imports
from langdetect import detect, detect_langs


def format_indexing_metadata(text: str, filename: str, num_pags: int, metadata: dict) -> str:
    """Format metadata for indexing

    :param text: Text to format
    :param filename: Name of the file
    :param num_pags: Number of pages of the file
    :param metadata: Metadata of the file
    :return: Formatted metadata to indexing
    """
    text = text.replace("\n", "\\\\n")
    text = text.replace("\t", "\\\\t")
    text = text.replace("\r", "\\\\r")

    try:
        extract_text = reduce(
            lambda x, y: x + "\t" + y,
            [f"{key}: {metadata[key]}" for key in metadata],
        )
    except TypeError:
        extract_text = ""

    return_text = f"{filename}\t{text}\t{get_language(text)}\t{num_pags}\t{extract_text}"

    return return_text[:-1] if return_text and return_text.endswith("\t") else return_text


def get_language(text: str, return_acc: bool = False, possible_langs: list = []) -> Tuple[str, float]:
    """Detect text language

    :param text: Text to detect language of
    :param return_acc: Return accuracy of the detection
    :param possible_langs: List of possible languages to consider
    :return: Language detected and optionally the accuracy
    """
    if possible_langs:
        possible_langs_set = set(possible_langs)
        detected_langs = detect_langs(text)
        filtered_langs = [lang for lang in detected_langs if lang.lang in possible_langs_set]
        
        if not filtered_langs:
            return ("unknown", 0.0) if return_acc else "unknown"
        
        best_lang = filtered_langs[0]
        
        if return_acc:
            return best_lang.lang, best_lang.prob
        return best_lang.lang

    if return_acc:
        lang = detect_langs(text)[0]
        return lang.lang, lang.prob
    return detect(text)
