### This code is property of the GGAO ###


import json
from dateutil.parser import parse
from abc import abstractmethod, ABC
from typing import List, Dict
from common.errors.dolffiaerrors import PrintableDolffiaError


class SortMethod(ABC):
    """
    Abstract base class for sorting methods.
    """

    TYPE: str = None

    def __init__(self, streamlist: list) -> None:
        """
        Instantiate SortMethod.

        Args:
            streamlist (list): List of streams to be sorted.
        """
        self.streamlist = streamlist

    @abstractmethod
    def process(self) -> List:
        """
        Process the streamlist given the sorting method.

        Returns:
            List: Sorted streamlist.
        """

    def get_example(self):
        """
        Get an example of the sorting method.

        Returns:
            str: JSON string representing the example.
        """
        return json.dumps(self._get_example())

    @abstractmethod
    def _get_example(self) -> Dict:
        """
        Get an example of the sorting method.

        Returns:
            Dict: Example of the sorting method.
        """
        return {}


class SortScore(SortMethod):
    """
    Sorting method based on the score of each stream.
    """

    TYPE = "score"

    def process(self, params):
        """
        Process the streamlist given the sorting method.

        Args:
            params (dict): Sorting parameters.

        Returns:
            List: Sorted streamlist.
        """

        if params:
            desc = params.get('desc', True)
        else:
            desc = True

        return sorted(self.streamlist, key=lambda chunk: chunk.get_mean_score(), reverse=desc)


    def _get_example(self) -> Dict:
        """
        Get an example of the sorting method.

        Returns:
            Dict: Example of the sorting method.
        """
        return {
            "type": self.TYPE, 
            "params": {
                "desc": True
            }
        }


class SortDocumentID(SortMethod):
    """
    Sorting method based on the document ID of each stream.
    """

    TYPE = "doc_id"

    def process(self, params):
        """
        Process the streamlist given the sorting method.

        Args:
            params (dict): Sorting parameters.

        Returns:
            List: Sorted streamlist.
        """
        if params:
            desc = params.get('desc', True)
        else:
            desc = True

        return sorted(self.streamlist, key=lambda chunk: chunk.get_metadata('document_id'), reverse=desc)


    def _get_example(self) -> Dict:
        """
        Get an example of the sorting method.

        Returns:
            Dict: Example of the sorting method.
        """
        return {
            "type": self.TYPE, 
            "params": {
                "desc": True
            }
        }


class SortSnippetNumber(SortMethod):
    """
    Sorting method based on the snippet number of each stream.
    """

    TYPE = "sn_number"

    def process(self, params):
        """
        Process the streamlist given the sorting method.

        Args:
            params (dict): Sorting parameters.

        Returns:
            List: Sorted streamlist.
        """
        if params:
            desc = params.get('desc', True)
        else:
            desc = True

        return sorted(self.streamlist, key=lambda chunk: chunk.get_metadata('snippet_number'), reverse=desc)

    def _get_example(self) -> Dict:
        """
        Get an example of the sorting method.

        Returns:
            Dict: Example of the sorting method.
        """
        return {
            "type": self.TYPE, 
            "params": {
                "desc": True
            }
        }


class SortLength(SortMethod):
    """
    Sorting method based on the length of each stream.
    """

    TYPE = "length"

    def process(self, params):
        """
        Process the streamlist given the sorting method.

        Args:
            params (dict): Sorting parameters.

        Returns:
            List: Sorted streamlist.
        """
        if params:
            desc = params.get('desc', True)
        else:
            desc = True

        return sorted(self.streamlist, key=lambda chunk: len(chunk.content), reverse=desc)

    def _get_example(self) -> Dict:
        """
        Get an example of the sorting method.

        Returns:
            Dict: Example of the sorting method.
        """
        return {
            "type": self.TYPE, 
            "params": {
                "desc": True
            }
        }
        

class SortDate(SortMethod):
    """
    Sorting method based on the date of each stream.
    """

    TYPE = "date"

    def process(self, params):
        """
        Process the streamlist given the sorting method.

        Args:
            params (dict): Sorting parameters.

        Returns:
            List: Sorted streamlist.
        """
        if params:
            desc = params.get('desc', True)
        else:
            desc = True

        return sorted(self.streamlist, key=lambda chunk: parse(chunk.get_metadata('date')), reverse=desc)

    def _get_example(self) -> Dict:
        """
        Get an example of the sorting method.

        Returns:
            Dict: Example of the sorting method.
        """
        return {
            "type": self.TYPE, 
            "params": {
                "desc": True
            }
        }


class SortMeta(SortMethod):
    """
    Sorting method based on a specific metadata value of each stream.
    """

    TYPE = "meta"

    def process(self, params):
        """
        Process the streamlist given the sorting method.

        Args:
            params (dict): Sorting parameters.

        Returns:
            List: Sorted streamlist.
        """
        if params:
            desc = params.get('desc', True)
        else:
            desc = True

        sort_value = params.get('value')

        return sorted(self.streamlist, key=lambda chunk: chunk.get_metadata(sort_value), reverse=desc)

    def _get_example(self) -> Dict:
        """
        Get an example of the sorting method.

        Returns:
            Dict: Example of the sorting method.
        """
        return {
            "type": self.TYPE, 
            "params": {
                "desc": True
            }
        }


class SortFactory:
    """
    Factory class for creating sorting methods.
    """

    SORTS = [SortScore, SortDocumentID, SortSnippetNumber, SortLength, SortDate, SortMeta]

    def __init__(self, sort_type: str) -> None:
        """
        Select the given sorting method.

        Args:
            sort_type (str): Type of the sorting method.
        """
        self.sortmethod: SortMethod = None
        for sortmethod in self.SORTS:
            if sortmethod.TYPE == sort_type:
                self.sortmethod = sortmethod
                break

        if self.sortmethod is None:
            raise PrintableDolffiaError(status_code=404, message=f"Provided sorting method does not match any of the possible ones: {', '.join(f.TYPE for f in self.SORTS)}")

    def process(self, streamlist: list, params):
        """
        Process the streamlist with the given parameters.

        Args:
            streamlist (list): List of streams to be sorted.
            params (dict): Sorting parameters.

        Returns:
            List: Sorted streamlist.
        """
        return self.sortmethod(streamlist).process(params)
