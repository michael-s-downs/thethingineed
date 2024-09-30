### This code is property of the GGAO ###


# Native imports
import re
import math
from copy import deepcopy


def mean(unique_docs: dict, model_formats: dict, retrievers: list,) -> list:
    """Returns the mean score

    Args:
        unique_docs (dict): Dictionary of haystack like docs
        model_formats (dict): Model format to identify types, whether sparse or dense vectors have different treatment in length based algorithms

    Returns:
        list: List of haystack like docs
    """
    docs = []
    for doc in unique_docs.values():
        norm, score = 0, 0
        for retriever_type in retrievers:
            score += doc.metadata[retriever_type]
            norm += 1

        doc.score = score / (norm + 1e-10)
        docs.append(doc)
    return docs


def length(unique_docs: dict, query: str, model_formats: dict, retrievers: list, log2: bool = False) -> list:
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
    for doc in unique_docs.values():
        norm, score = 0, 0
        for retriever_type in retrievers:
            mf_type = model_formats.get(retriever_type, "dense")
            if mf_type == "dense":
                score += doc.metadata[retriever_type] * query_len
                norm += query_len
            elif mf_type == "sparse":
                score += doc.metadata[retriever_type]
                norm += 1

        doc.score = score / (norm + 1e-10)
        docs.append(doc)
    return docs


def position(unique_docs: dict, model_formats: dict, retrievers: list, norm: bool = False) -> list:
    """Returns the score based on position

    Args:
        unique_docs (dict): Dictionary of haystack like docs
        model_formats (dict): Model format to identify types, whether sparse or dense vectors have different treatment in length based algorithms
        norm (bool, optional): If set to False it will give 0-1 scores otherwise it will return min-1, for each model will be different. Defaults to False.

    Returns:
       list: List of haystack like docs
    """
    for retriever_type in retrievers:
        scores = []
        for doc in unique_docs.values():
            score = doc.metadata[retriever_type]
            scores.append(score)

        if scores:
            scores_sort = sorted(scores, reverse=False)

            if norm:
                min_score = min(scores_sort)
                factor = (1 - min_score) / (len(scores_sort) + 1e-10)
            else:
                min_score = 0
                factor = 1 / (len(scores_sort) + 1e-10)

            for doc in unique_docs.values():
                score = doc.metadata[retriever_type]
                pos = scores_sort.index(score)
                doc.metadata[retriever_type] = min_score + pos * factor

    return mean(unique_docs, model_formats, retrievers)


def normalize(unique_docs: dict, query: str, model_formats: dict, retrievers: list, loglength: bool = False) -> list:
    """Returns the normalized score for each document and then averages them

    Args:
        unique_docs (dict): Dictionary of haystack like docs
        model_formats (dict): Model format to identify types, whether sparse or dense vectors have different treatment in length based algorithms

    Returns:
        list: List of haystack like docs
    """

    for retriever_type in retrievers:
        scores = [doc.metadata[retriever_type] for doc in unique_docs.values()]
        score_max, score_min = max(scores), min(scores)
        if score_max != score_min:
            for doc in unique_docs.values():
                doc.metadata[retriever_type] = (doc.metadata[retriever_type] - score_min) / (score_max - score_min + 1e-10) # Normalize if not all scores are the same

    if loglength:
        docs = length(unique_docs, query, model_formats, retrievers, log2=True)
    else:
        docs = mean(unique_docs, model_formats, retrievers)
    return docs

def reciprocal_rank_fusion(unique_docs: dict, model_formats:dict, retrievers: list) -> list:
    """Returns the reciprocal rank fusion

    Args:
        unique_docs (dict): Dictionary of haystack like docs

    Returns:
        list: List of haystack like docs
    """
    for retriever_type in retrievers:
        scores = []
        for key, doc in unique_docs.items():
            score = doc.metadata[retriever_type]
            scores.append((score, key))

        docs_sorted = sorted(scores, key=lambda x: x[0], reverse=True)

        for pos, doc in enumerate(docs_sorted):
            doc_key = doc[1]
            doc_score = 1/((pos+1)+1) #RRF formula: https://plg.uwaterloo.ca/~gvcormac/cormacksigir09-rrf.pdf
            unique_docs[doc_key].metadata[retriever_type] = doc_score

    for doc in unique_docs.values():
        score = 0
        for retriever_type in retrievers:
            score += doc.metadata[retriever_type]
        doc.score = score


    return mean(unique_docs, model_formats, retrievers)

def rescore_documents(unique_docs: dict, query: str, rescoring_function: str, model_formats: dict, retrievers: list) -> list:
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
        docs = length(unique_docs, query, model_formats, retrievers)
    if rescoring_function == "loglength":
        docs = length(unique_docs, query, model_formats, retrievers, log2=True)
    elif rescoring_function == "mean":
        docs = mean(unique_docs, model_formats, retrievers)
    elif rescoring_function == "pos":
        docs = position(unique_docs, model_formats, retrievers)
    elif rescoring_function == "posnorm":
        docs = position(unique_docs, model_formats, retrievers, norm=True)
    elif rescoring_function == "norm":
        docs = normalize(unique_docs, query, model_formats, retrievers)
    elif rescoring_function == "nll":  # norm + loglength
        docs = normalize(unique_docs, query, model_formats, retrievers, loglength=True)
    elif rescoring_function == "rrf":
        docs = reciprocal_rank_fusion(unique_docs, model_formats, retrievers)

    return docs
