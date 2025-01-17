### This code is property of the GGAO ###


from typing import List
from copy import deepcopy

from ..utils.defaults import EMPTY_STREAM
from ..utils.split_sentences import split_by_word_respecting_sent_boundary as split_phrase
from ..streamlist import StreamList
from common.errors.genaierrors import GenaiError

class SplitBatchMethod:
    TYPE: str

    def __init__(self, streambatch: list) -> None:
        """Instantiate streambatch

        Args:
            streambatch (list): StreamBatch
        """
        for streamlist in streambatch:
            if len(streamlist) > 1:
                raise GenaiError(status_code=500, message="All the streamlists must be of length one. Try a filter or Split method first")

        self.streambatch = streambatch

    def process(self) -> List:
        """Process the streambatch given the method
        """


class PhraseSplitBatch(SplitBatchMethod):
    TYPE = "phrasesplit"

    def process(self, split_length:int = 10, split_overlap:int = 0):
        """Process the streambatch given the method
        """
        es = deepcopy(EMPTY_STREAM)
        streams = []
        for streamlist in self.streambatch:
            for streamlist in streamlist.streamlist:
                for phrase in split_phrase(streamlist.content, split_length, split_overlap)[0]:
                    es['content'] = phrase
                    streams.append(es)

        sls = StreamList()
        sls.retrieve("streamlist", {"streamlist": streams})
        return sls


class SplitBatchFactory:

    SPLITBATCHES = [PhraseSplitBatch]

    def __init__(self, splitbatch_type: str) -> None:
        """Select the given Splitbatch

        Args:
            splitbatch_type (str): one of the available Splitbatchs
        """

        self.splitbatchmethod = None
        for splitbatchmethod in self.SPLITBATCHES:
            if splitbatchmethod.TYPE == splitbatch_type:
                self.splitbatchmethod = splitbatchmethod
                break

        if self.splitbatchmethod is None:
            raise GenaiError(status_code=404, message=f"Provided Splitbatch does not match any of the possible ones: {', '.join(f.TYPE for f in self.SPLITBATCHES)}")

    def process(self, streambatch: list, params: dict):
        """Process the streambatch with the given method
        """
        return self.splitbatchmethod(streambatch).process(**params)
