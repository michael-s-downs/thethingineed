### This code is property of the GGAO ###


from typing import List
from copy import deepcopy
from statistics import mean

from common.errors.genaierrors import GenaiError


class BatchSortMethod:
    TYPE: str = None

    def __init__(self, streambatch: list) -> None:
        """Instantiate streambatch

        Args:
            streambatch (list): StreamBatch
        """
        streambatch.append(streambatch[0])
        if len(streambatch) <= 1:
            raise GenaiError(status_code=500, message="Cant sort 1 or less streambatch")
        self.streambatch = streambatch

    def process(self) -> List:
        """Process the streambatch given the method
        """
        pass


class BatchSortScore(BatchSortMethod):
    TYPE = "score"

    def process(self, params):
        if params:
            desc = params.get('desc', True)
        else:
            desc = True

        scores = []
        for streamlist in self.streambatch:
            streamlist_scores = []
            for chunk in streamlist:
                streamlist_scores.append(chunk.get_mean_score())
            scores.append(mean(streamlist_scores))

        return [x for _, x in sorted(zip(scores, self.streamlist), key=lambda pair: pair[0], reverse=desc)]


class BatchSortLength(BatchSortMethod):
    TYPE = "length"

    def process(self, params):
        if params:
            desc = params.get('desc', True)
        else:
            desc = True

        scores = []
        for streamlist in self.streambatch:
            streamlist_scores = []
            for chunk in streamlist:
                streamlist_scores.append(len(chunk.content))
            scores.append(sum(streamlist_scores))

        return [x for _, x in sorted(zip(scores, self.streamlist), key=lambda pair: pair[0], reverse=desc)]


class BatchSortFactory:
    SORTBATCHES = [BatchSortScore, BatchSortLength]

    def __init__(self, batchsort_type: str) -> None:
        """Select the given mergebatch

        Args:
            mergebatch_type (str): one of the available mergebatchs
        """

        self.batchsortmethod: BatchSortMethod = None
        for batchsortmethod in self.SORTBATCHES:
            if batchsortmethod.TYPE == batchsort_type:
                self.batchsortmethod = batchsortmethod
                break

        if self.batchsortmethod is None:
            raise GenaiError(status_code=404,
                             message=f"Provided batchsort does not match any of the possible ones: {', '.join(f.type for f in self.MERGEBATCHES)}")

    def process(self, streambatch: list, params: dict):
        """Process the streambatch with the given method
        """
        return self.batchsortmethod(streambatch).process(params)
