### This code is property of the GGAO ###


from typing import List
from copy import deepcopy

from ..utils.defaults import EMPTY_STREAM
from ..streamlist import StreamList
from common.errors.dolffiaerrors import DolffiaError


class MergeBatchMethod:
    TYPE: str = None

    def __init__(self, streambatch: list) -> None:
        """Instantiate streambatch

        Args:
            streambatch (list): StreamBatch
        """
        for streamlist in streambatch:
            if len(streamlist) > 1:
                raise DolffiaError(status_code=500, message=f"All the streamlists must be of length one. Try a filter or merge method first")

        self.streambatch = streambatch

    def process(self) -> List:
        """Process the streambatch given the method
        """
        pass


class AddMergeBatch(MergeBatchMethod):
    TYPE = "add"

    def process(self, SEQ:str = "\n"):
        """Process the streambatch given the method
        """
        es = deepcopy(EMPTY_STREAM)
        es.update({
             "content": SEQ.join([StreamList.streamlist[0].content for StreamList in self.streambatch])
        })
        return StreamList().retrieve("streamlist",  {"streamlist": [es]})


class MergeBatchFactory:

    MERGEBATCHES = [AddMergeBatch]

    def __init__(self, mergebatch_type: str) -> None:
        """Select the given mergebatch

        Args:
            mergebatch_type (str): one of the available mergebatchs
        """

        self.mergebatchmethod: MergeBatchMethod = None
        for mergebatchmethod in self.MERGEBATCHES:
            if mergebatchmethod.TYPE == mergebatch_type:
                self.mergebatchmethod = mergebatchmethod
                break

        if self.mergebatchmethod is None:
            raise DolffiaError(status_code=404, message=f"Provided mergebatch does not match any of the possible ones: {', '.join(f.type for f in self.MERGEBATCHES)}")

    def process(self, streambatch: list, params: dict):
        """Process the streambatch with the given method
        """
        return self.mergebatchmethod(streambatch).process(**params)
