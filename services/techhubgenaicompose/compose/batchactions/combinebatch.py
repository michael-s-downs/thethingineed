### This code is property of the GGAO ###


from typing import List
from copy import deepcopy
from string import Template

from ..utils.defaults import EMPTY_STREAM
from ..streamlist import StreamList
from ..streamchunk import StreamChunk
from common.errors.genaierrors import GenaiError


class CombineBatchMethod:
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


class JoinCombine2(CombineBatchMethod):
    TYPE = "joincombine2"

    def __init__(self, streambatch: list) -> None:
        """Join the contents and then apply the template

        Args:
            streambatch (list): StreamBatch
        """
        if len(streambatch) != 2:
            raise GenaiError(status_code=500, message="CombineTwoSlBatch can only be used with two streamlists")

        self.streambatch = streambatch

    def process(self, template: str, SEP: str = "\n") -> List:
        """Process the streambatch given the method
        """
        sl1, sl2 = self.streambatch
        s1 = SEP.join([sl.content for sl in sl1])
        s2 = SEP.join([sl.content for sl in sl2])
        content = Template(template).substitute(s1=s1, s2=s2)

        es = deepcopy(EMPTY_STREAM)
        es.update({"content": content})
        return StreamList().retrieve("streamlist", {"streamlist": [es]})


class CombineJoin2(CombineBatchMethod):
    TYPE = "combinejoin2"

    def __init__(self, streambatch: list) -> None:
        """Applies the template to each pair of contents. If the streamlists have different lengths, the shorter one is repeated

        Args:
            streambatch (list): StreamBatch
        """
        if len(streambatch) != 2:
            raise GenaiError(status_code=500, message="CombineTwoSlBatch can only be used with two streamlists")

        self.streambatch = streambatch

    @staticmethod
    def _repeat(sl1, sl2):
        if len(sl1) != len(sl2):
            if len(sl1) < len(sl2):
                sl1 = sl1 * (len(sl2) // len(sl1) + 1)
            else:
                sl2 = sl2 * (len(sl1) // len(sl2) + 1)

        return sl1, sl2

    def process(self, template, unique_streamlist: bool = False, SEP: str = "\n") -> List:
        """Process the streambatch given the method
        """
        sl1, sl2 = self.streambatch
        contents = [Template(template).substitute(s1=s1.content, s2=s2.content) for s1, s2 in zip(sl1, sl2)]
        if unique_streamlist:
            content = SEP.join(contents)
            es = deepcopy(EMPTY_STREAM)
            es.update({"content": content})
            new_streamlist = StreamList()
            new_streamlist.append(StreamChunk(es))
        else:
            sl1, sl2 = self._repeat(sl1, sl2)
            new_streamlist = StreamList()
            for content in contents:
                es = deepcopy(EMPTY_STREAM)
                es.update({"content": content})
                new_streamlist.append(StreamChunk(es))
        return new_streamlist


class CombineBatchFactory:
    MERGEBATCHES = [JoinCombine2, CombineJoin2]

    def __init__(self, combinebatch_type: str) -> None:
        """Select the given combinebatch

        Args:
            combinebatch_type (str): one of the available combinebatchs
        """

        self.combinebatchmethod: CombineBatchMethod = None
        for combinebatchmethod in self.MERGEBATCHES:
            if combinebatchmethod.TYPE == combinebatch_type:
                self.combinebatchmethod = combinebatchmethod
                break

        if self.combinebatchmethod is None:
            raise GenaiError(status_code=404,
                             message=f"Provided combinebatch does not match any of the possible ones: {', '.join(f.TYPE for f in self.MERGEBATCHES)}")

    def process(self, streambatch: list, params: dict):
        """Process the streambatch with the given method
        """
        return self.combinebatchmethod(streambatch).process(**params)
