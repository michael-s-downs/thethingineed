### This code is property of the GGAO ###


# Native imports
import re
import math
from copy import deepcopy


def mean(unique_docs:dict , model_formats: dict) -> list:
    """Returns the mean score

    Args:
        unique_docs (dict): Dictionary of haystack like docs
        model_formats (dict): Model format to identify types, whether sparse or dense vectors have different treatment in length based algorithms

    Returns:
        list: List of haystack like docs
    """
    docs = []
    for doc_id in unique_docs:
        doc = unique_docs[doc_id]
        meta = doc.meta
        norm, score = 0, 0
        for meta_key in meta:
            model_format = meta_key.split("--")[0]
            mf_type = model_formats.get(model_format)
            if mf_type is not None:  # Detect if it is an score metadata
                score += meta[meta_key]
                norm += 1

        doc.score = score / (norm + 1e-10)
        docs.append(doc)
    return docs


def length(unique_docs: dict, query: str, model_formats: dict, log2:bool = False) -> list:
    """Returns the score based on query length

    Args:
        unique_docs (dict): Dictionary of haystack like docs
        query (str): Query to ponderate
        model_formats (dict): Model format to identify types, whether sparse or dense vectors have different treatment in length based algorithms
        log2(bool, optional): If set to true, log2 will be applied

    Returns:
       list: List of haystack like docs
    """

    query_len = len(re.sub(r"\W+", " ", query).split())
    if log2:
        query_len = math.log2(max(2, query_len))

    docs = []
    for doc_id in unique_docs:
        doc = unique_docs[doc_id]
        meta = doc.meta
        norm, score = 0, 0
        for meta_key, meta_value in meta.items():
            model_format = meta_key.split("--")[0]
            mf_type = model_formats.get(model_format)

            if mf_type is None:  # Detect if it is an score metadata
                continue
            elif mf_type == "dense":
                score += meta_value * query_len
                norm += query_len
            elif mf_type == "sparse":
                score += meta_value
                norm += 1

        doc.score = score / (norm + 1e-10)
        docs.append(doc)
    return docs


def position(unique_docs: dict, model_formats: dict, norm: bool = False) -> list:
    """Returns the score based on position

    Args:
        unique_docs (dict): Dictionary of haystack like docs
        model_formats (dict): Model format to identify types, whether sparse or dense vectors have different treatment in length based algorithms
        norm (bool, optional): If set to False it will give 0-1 scores otherwise it will return min-1, for each model will be different. Defaults to False.

    Returns:
       list: List of haystack like docs
    """
    try:
        meta = deepcopy(list(unique_docs.values())[0].meta)
    except:
        meta = {}

    for meta_key in meta:
        model_format = meta_key.split("--")[0]
        mf_type = model_formats.get(model_format)

        if mf_type is None:  # Detect if it is an score metadata
            continue

        scores = []
        for doc_id in unique_docs:
            score = unique_docs[doc_id].meta.setdefault(meta_key, 0)
            scores.append(score)

        if scores:
            scores_sort = sorted(scores, reverse=True)

            if norm:
                min_score = min(scores_sort)
                factor = (1 - min_score) / (len(scores_sort) + 1e-10)
            else:
                min_score = 0
                factor = 1 / (len(scores_sort) + 1e-10)

            for doc_id in unique_docs:
                score = unique_docs[doc_id].meta[meta_key]
                unique_docs[doc_id].meta[meta_key] = min_score + scores_sort.index(score) * factor

    return mean(unique_docs, model_formats)


def normalize(unique_docs:dict, query: str, model_formats: dict, loglength: bool = False) -> list:
    """Returns the normalized score for each document and then averages them

    Args:
        unique_docs (dict): Dictionary of haystack like docs
        model_formats (dict): Model format to identify types, whether sparse or dense vectors have different treatment in length based algorithms

    Returns:
        list: List of haystack like docs
    """

    try:
        meta = deepcopy(list(unique_docs.values())[0].meta)
    except:
        meta = {}

    for meta_key in meta:

        model_format = meta_key.split("--")[0]
        mf_type = model_formats.get(model_format)
        if mf_type is None:  # Detect if it is an score metadata
            continue

        scores = [unique_docs[doc_id].meta.setdefault(meta_key, 0) for doc_id in unique_docs]
        score_max, score_min = max(scores), min(scores)
        if score_max != score_min:
            for doc_id in unique_docs:
                unique_docs[doc_id].meta[meta_key] = (unique_docs[doc_id].meta[meta_key] - score_min) / (score_max - score_min + 1e-10) # Normalize if not all scores are the same

    if loglength:
        docs = length(unique_docs, query, model_formats, log2=True)
    else:
        docs = mean(unique_docs, model_formats)
    return docs


def rescore_documents(unique_docs: dict, query: str, rescoring_function: str, model_formats:dict) -> list:
    """Rescore documents based on a given

    Args:
        unique_docs (dict): Dictionary of haystack like docs
        query (str): Input query
        rescoring_function (str): String that identifies which function to use
        model_formats (dict): Model format to identify types, whether sparse or dense vectors have different treatment in length based algorithms

    Returns:
        list: List of haystack like docs
    """

    if rescoring_function == "length":
        docs = length(unique_docs, query, model_formats)
    if rescoring_function == "loglength":
        docs = length(unique_docs, query, model_formats, log2=True)
    elif rescoring_function == "mean":
        docs = mean(unique_docs, model_formats)
    elif rescoring_function == "pos":
        docs = position(unique_docs, model_formats)
    elif rescoring_function == "posnorm":
        docs = position(unique_docs, model_formats, norm=True)
    elif rescoring_function == "norm":
        docs = normalize(unique_docs, query, model_formats)
    elif rescoring_function == "nll":  # norm + loglength
        docs = normalize(unique_docs, query, model_formats, loglength=True)

    return docs
