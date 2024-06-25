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
        self.streambatch = streambatch

    def process(self) -> List:
        """Process the streambatch given the method
        """
        pass


class AddMergeBatch(MergeBatchMethod):
    TYPE = "add"

    def process(self, SEQ:str = "\n"):
        """Process the streambatch transforming it into one streamlist with no-repeated chunks
        """
        sl_add = StreamList()
        chunks_id = []
        for sl in self.streambatch:
            for chunk in sl.streamlist:
                if chunk.meta.get('snippet_id') in chunks_id:
                    continue
                sl_add.append(chunk)
                chunks_id.append(chunk.meta.get('snippet_id'))
        return sl_add


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
