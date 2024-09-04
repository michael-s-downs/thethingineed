### This code is property of the GGAO ###


import json
from dateutil.parser import parse
from statistics import mean
from collections import defaultdict
from abc import abstractmethod, ABC
from typing import List, Dict
from common.errors.genaierrors import PrintableGenaiError


class GroupByMethod(ABC):
    """Abstract base class for group by methods.

    This class defines the common interface for all group by methods.
    Subclasses must implement the `process` and `_get_example` methods.

    Attributes:
        TYPE (str): The type of the group by method.

    Args:
        streamlist (list): The list of streams to process.

    """

    TYPE: str = None

    def __init__(self, streamlist: list) -> None:
        """Instantiate a GroupByMethod object.

        Args:
            streamlist (list): The list of streams to process.

        """
        self.streamlist = streamlist

    @abstractmethod
    def process(self) -> List:
        """Process the streamlist using the group by method.

        This method should be implemented by subclasses to define the specific
        logic for processing the streamlist.

        Returns:
            List: The processed streamlist.

        """

    def get_example(self):
        """Get an example of the group by method.

        Returns:
            str: A JSON string representing an example of the group by method.

        """
        return json.dumps(self._get_example())

    @abstractmethod
    def _get_example(self) -> Dict:
        """Get an example of the group by method.

        This method should be implemented by subclasses to define the specific
        logic for generating an example of the group by method.

        Returns:
            Dict: A dictionary representing an example of the group by method.

        """
        return {}


class GroupByDoc(GroupByMethod):
    """
    This class represents a group by method that sorts the streamlist by document score.
    It inherits from the `GroupByMethod` class.

    Args:
        GroupByMethod (type): The base class for group by methods.

    Raises:
        GenaiError: This exception is raised when the group by sorting method is not found.

    Returns:
        list: A list of chunks sorted by document score.
    """

    TYPE = "docscore"

    def process(self, params):
        """Process the streamlist given the method.

        This method processes the streamlist based on the specified parameters.

        Args:
            params (dict): The parameters for the group by method.

        Returns:
            list: A list of chunks sorted by document score.
        """

        if params:
            desc = params.get('desc', True)
            method = params.get('method', "max")
        else:
            desc = True
            method = "max"

        if method == "max":
            method = max
        elif method == "mean":
            method = mean
        else:
            raise PrintableGenaiError(404, "Groupby sorting method not found, try max or mean")

        grouped_dict = defaultdict(list)
        group_score = {}

        for sc in self.streamlist:
            doc_id = sc.get('document_id')
            grouped_dict[doc_id].append(sc)

        for doc_key, group in grouped_dict.items():
            scores = []
            for sc in group:
                scores.append(sc.get_mean_score())
            group_score[doc_key] = method(scores)

        for doc_key in grouped_dict:
            grouped_dict[doc_key].sort(key=lambda chunk: chunk.get('snippet_number'))

        return [chunk for doc_key in dict(sorted(group_score.items(), key=lambda item: item[1], reverse=desc)) for chunk
                in grouped_dict[doc_key]]

    def _get_example(self) -> Dict:
        """Get an example of the group by method.

        Returns:
            dict: An example of the group by method.
        """
        return {
            "type": self.TYPE,
            "params": {
                "desc": True
            }
        }


class GroupByDate(GroupByMethod):
    """
    A class that represents a grouping method based on date.

    Args:
        GroupByMethod (type): The base class for grouping methods.

    Returns:
        type: The grouped data based on the date.

    Attributes:
        TYPE (str): The type of the grouping method, which is "date".
    """

    TYPE = "date"

    def process(self, params):
        """Process the streamlist given the method
        """

        if params:
            desc = params.get('desc', True)
        else:
            desc = True

        grouped_dict = defaultdict(list)
        group_score = {}

        for sc in self.streamlist:
            date_id = sc.get('date')
            grouped_dict[date_id].append(sc)
            group_score[date_id] = parse(date_id)

        for doc_key in grouped_dict:
            grouped_dict[doc_key].sort(key=lambda chunk: chunk.get('snippet_number'))

        return [chunk for doc_key in dict(sorted(group_score.items(), key=lambda item: item[1], reverse=desc)) for chunk
                in grouped_dict[doc_key]]

    def _get_example(self) -> Dict:
        return {
            "type": self.TYPE,
            "params": {
                "desc": True
            }
        }


class GroupByFactory:
    GROUPBY = [GroupByDoc, GroupByDate]

    def __init__(self, groupby_type: str) -> None:
        """Select the given GroupBy

        Args:
            groupby_type (str): one of the available merges
        """
        self.groupbymethod: GroupByMethod = None
        for groupbymethod in self.GROUPBY:
            if groupbymethod.TYPE == groupby_type:
                self.groupbymethod = groupbymethod
                break

        if self.groupbymethod is None:
            raise PrintableGenaiError(status_code=404,
                                      message=f"Provided groupby does not match any of the possible ones: {', '.join(f.TYPE for f in self.GROUPBY)}")

    def process(self, streamlist: list, params):
        """Process the streamlist with the given params

        Args:
            streamlist (list)
            params (dict)

        Returns:
            sortmethod: Sorting method selected
        """
        return self.groupbymethod(streamlist).process(params)
