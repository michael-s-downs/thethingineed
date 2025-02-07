### This code is property of the GGAO ###


import json
import re
from copy import deepcopy
from string import Template
from typing import List, Dict
from abc import abstractmethod, ABC

from compose.utils.defaults import EMPTY_STREAM
from common.genai_controllers import load_file, storage_containers
from common.errors.genaierrors import PrintableGenaiError
from ..streamchunk import StreamChunk

IRStorage_PATH = "src/compose/templates"


class MergeMethod(ABC):
    """
    Abstract base class for merge methods.

    Args:
        ABC (type): The ABC class.

    Returns:
        type: The return type.
    """

    TYPE: str

    def __init__(self, streamlist: list) -> None:
        """Instantiate the MergeMethod class.

        Args:
            streamlist (list): The list of stream objects.
        """
        self.streamlist = streamlist

    @abstractmethod
    def process(self) -> List:
        """Process the streamlist given the merge method.

        Returns:
            List: The processed streamlist.
        """

    def get_example(self):
        """Get an example of the merge method.

        Returns:
            str: The example as a JSON string.
        """
        return json.dumps(self._get_example())

    @abstractmethod
    def _get_example(self) -> Dict:
        """Get an example of the merge method.

        Returns:
            Dict: The example as a dictionary.
        """
        return {}


class AddMerge(MergeMethod):
    """
    Merge method for adding stream contents.

    Args:
        MergeMethod (type): The MergeMethod class.

    Returns:
        type: The return type.
    """

    TYPE = "add"

    def process(self, seq: str = "\n"):
        """Process the streamlist by adding the stream contents.

        Args:
            SEQ (str, optional): The separator between stream contents. Defaults to "\n".

        Returns:
            List: The processed streamlist.
        """
        es = deepcopy(EMPTY_STREAM)
        es.update(
            {
                "content": seq.join(
                    [streamlist.content for streamlist in self.streamlist]
                )
            }
        )
        return [es]

    def _get_example(self) -> Dict:
        """Get an example of the add merge method.

        Returns:
            Dict: The example as a dictionary.
        """
        return {"type": "add", "params": {"SEQ": "\n"}}


class MetaMerge(MergeMethod):
    """
    Merge method for creating a meta field.

    Args:
        MergeMethod (type): The MergeMethod class.

    Returns:
        type: The return type.
    """

    TYPE = "meta"
    DEFAULT_FIELD = "Content: $content"

    def process(self, params = {}):
        """Process the streamlist by creating a meta field.

        Args:
            params (dict): The parameters for the merge method.

        Returns:
            List: The processed streamlist.
        """
        if params:
            template = params.get("template", self.DEFAULT_FIELD)
            sep = params.get("sep", "-##########-")
            grouping_key = params.get("grouping_key", None)
        else:
            template = self.DEFAULT_FIELD
            sep = "-##########-"
            grouping_key = None

        if "$" not in template:
            try:
                template = load_file(storage_containers['workspace'], f"{IRStorage_PATH}/{template}.json").decode()
                if not template:
                    raise PrintableGenaiError(400, "Template empty")
            except ValueError:
                raise PrintableGenaiError(
                    404, f"Cloud config file doesn't exist for name {template}"
                )

        fields = [word[1:] for word in re.findall(pattern=r"\$\w+", string=template)]

        new_contents = []

        #Group new contents by the grouping key from each chunk so the result is N groups of chunks with its content merged
        if grouping_key:
            group_keys = list(dict.fromkeys([sc.get(grouping_key) for sc in self.streamlist]))
            content_groups = []
            common_metadata = []
            for group_k in group_keys:
                group = []
                metadata_group = []
                for sc in self.streamlist:
                    if sc.get(grouping_key) == group_k:
                        fields_dict = {field: sc.get(field) for field in fields}
                        grouping_value = fields_dict.pop(grouping_key, None)
                        if grouping_value and grouping_key in template:
                            group.insert(0, f"{grouping_key}: {grouping_value}")
                            template = template.replace(f"${grouping_key}", "")
                        group.append(Template(template=template).substitute(fields_dict))
                        metadata_group.append(sc.meta)

                common_pairs = metadata_group[0].copy()
                for d in metadata_group[1:]:
                    common_pairs = {k: v for k, v in common_pairs.items() if k in d and d[k] == v}
                
                content_groups.append(group)
                common_metadata.append(common_pairs)
            
            return [
                StreamChunk(
                    {"content": sep.join(group), "meta": meta_group, "scores": {}, "answer": ""}
                )
                for group, meta_group in zip(content_groups, common_metadata)
            ]

        metadata_group = []
        for sc in self.streamlist:
            fields_dict = {field: sc.get(field) for field in fields}
            grouping_value = fields_dict.pop(grouping_key, None)
            if grouping_value:
                new_contents.insert(0, f"{grouping_key}: {grouping_value}")

            new_contents.append(Template(template=template).substitute(fields_dict))
            metadata_group.append(sc.meta)

        common_pairs = metadata_group[0].copy()
        for d in metadata_group[1:]:
            common_pairs = {k: v for k, v in common_pairs.items() if k in d and d[k] == v}

        return [
            StreamChunk(
                {"content": sep.join(new_contents), "meta": common_pairs, "scores": {}, "answer": ""}
            )
        ]

    def _get_example(self) -> Dict:
        """Get an example of the meta merge method.

        Returns:
            Dict: The example as a dictionary.
        """
        return {"type": self.TYPE, "params": {"template": self.DEFAULT_FIELD}}


class MergeFactory:
    """
    Factory class for creating merge methods.

    Args:
        type (str): The type of merge method.

    Returns:
        type: The return type.
    """

    MERGES = [AddMerge, MetaMerge]

    def __init__(self, merge_type: str) -> None:
        """Instantiate the MergeFactory class.

        Args:
            merge_type (str): The type of merge method.
        """
        self.mergemethod = None
        for mergemethod in self.MERGES:
            if mergemethod.TYPE == merge_type:
                self.mergemethod = mergemethod
                break

        if self.mergemethod is None:
            raise PrintableGenaiError(
                status_code=404,
                message=f"Provided merge does not match any of the possible ones: {', '.join(f.TYPE for f in self.MERGES)}",
            )

    def process(self, streamlist: list, params: dict):
        """Process the streamlist with the given merge method.

        Args:
            streamlist (list): The list of stream objects.
            params (dict): The parameters for the merge method.

        Returns:
            List: The processed streamlist.
        """
        return self.mergemethod(streamlist).process(params)
