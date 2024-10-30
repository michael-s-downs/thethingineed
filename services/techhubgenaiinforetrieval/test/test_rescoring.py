### This code is property of the GGAO ###


# Native imports
import re, copy, json

# Installed imports
import pytest
from unittest.mock import MagicMock
from unittest import mock

# Local imports
from rescoring import rescore_documents




class TestRescoring:
    unique_docs = {
        "0aeb91f9-29ae-4051-a797-778bb36f46c9": MagicMock(
            metadata={
                "bm25--score": 0.6120502936760626,
                "text-embedding-ada-002-score": 0.85331446
            },
            score=0.7326823768380313),
        "cfa95abc-f59a-4591-a316-d7a7bf5ae092": MagicMock(
            metadata={
                "bm25--score": 0.6065341488913049,
                "text-embedding-ada-002-score": 0.8496914
            },
            score=0.7281127744456524),
        "d3a95abc-f59a-4591-a316-d7a7bf5ae093": MagicMock(
            metadata={
                "bm25--score": 0.5995536021685113,
                "text-embedding-ada-002-score": 0.85015917
            },
            score=0.7248563860842556),
        "f5a95abc-f59a-4591-a316-d7a7bf5ae095": MagicMock(
            metadata={
                "bm25--score": 0.576082770152,
                "text-embedding-ada-002-score": 0.84695625
            },
            score=0.711519510076),
        "e4a95abc-f59a-4591-a316-d7a7bf5ae094": MagicMock(
            metadata={
                "bm25--score": 0.576082770152,
                "text-embedding-ada-002-score": 0.8487507
            },
            score=0.712416735076)
    }

    retrievers = ["bm25--score", "text-embedding-ada-002-score"]
    model_formats = {"bm25--score": "sparse"}
    query = "Cuantas copas ha ganado el Real Zaragoza?"

    def test_rescore_documents_length(self):
        aux_docs = copy.deepcopy(self.unique_docs)
        for doc in aux_docs.values():
            doc.score = 0.0
        docs = rescore_documents(aux_docs, self.query, "length", self.model_formats, self.retrievers)
        assert 0.8231564391992184 == docs[0].score
        assert docs[0].score != list(self.unique_docs.values())[0].score

    def test_rescore_documents_loglength(self):
        aux_docs = copy.deepcopy(self.unique_docs)
        for doc in aux_docs.values():
            doc.score = 0.0
        docs = rescore_documents(aux_docs, self.query, "loglength", self.model_formats, self.retrievers)
        assert 0.7899465388731337 == docs[0].score
        assert docs[0].score != list(self.unique_docs.values())[0].score

    def test_rescore_documents_mean(self):
        aux_docs = copy.deepcopy(self.unique_docs)
        for doc in aux_docs.values():
            doc.score = 0.0
        docs = rescore_documents(aux_docs, "", "mean", self.model_formats, self.retrievers)
        assert 0.7326823768013971 == docs[0].score
        assert docs[0].score != list(self.unique_docs.values())[0].score
    def test_rescore_documents_pos(self):
        aux_docs = copy.deepcopy(self.unique_docs)
        for doc in aux_docs.values():
            doc.score = 0.0
        docs = rescore_documents(aux_docs, self.query, "pos", self.model_formats, self.retrievers)
        assert 0.799999999944 == docs[0].score
        assert docs[0].score != list(self.unique_docs.values())[0].score

    def test_rescore_documents_posnorm(self):
        aux_docs = copy.deepcopy(self.unique_docs)
        for doc in aux_docs.values():
            doc.score = 0.0
        docs = rescore_documents(aux_docs, self.query, "posnorm", self.model_formats, self.retrievers)
        assert 0.9423039019634691 == docs[0].score
        assert docs[0].score != list(self.unique_docs.values())[0].score
    def test_rescore_documents_normalize(self):
        aux_docs = copy.deepcopy(self.unique_docs)
        for doc in aux_docs.values():
            doc.score = 0.0
        docs = rescore_documents(aux_docs, self.query, "norm", self.model_formats, self.retrievers)
        assert 0.9999999906960086 == docs[0].score
        assert docs[0].score != list(self.unique_docs.values())[0].score

    def test_rescore_documents_nll(self):
        aux_docs = copy.deepcopy(self.unique_docs)
        for doc in aux_docs.values():
            doc.score = 0.0
        docs = rescore_documents(aux_docs, self.query, "nll", self.model_formats, self.retrievers)
        assert 0.9999999876466698 == docs[0].score
        assert docs[0].score != list(self.unique_docs.values())[0].score

    def test_rescore_documents_rrf(self):
        aux_docs = copy.deepcopy(self.unique_docs)
        for doc in aux_docs.values():
            doc.score = 0.0
        docs = rescore_documents(aux_docs, self.query, "rrf", self.model_formats, self.retrievers)
        assert 0.499999999975 == docs[0].score
        assert docs[0].score != list(self.unique_docs.values())[0].score