### This code is property of the GGAO ###


import collections
from .actions import FilterFactory, RetrieverFactory, RescoreFactory, MergeFactory, LLMFactory, SortFactory, GroupByFactory
from .streamchunk import StreamChunk
from common.errors.dolffiaerrors import DolffiaError


class StreamList(collections.MutableSequence):
    """
    A list-like collection of StreamChunk objects.

    Args:
        None

    Attributes:
        oktypes (type): The allowed type for elements in the StreamList.
        streamlist (list): The list of StreamChunk objects.

    Methods:
        __iter__(): Returns an iterator over the StreamList.
        __str__(): Returns a string representation of the StreamList.
        __repr__(): Returns a string representation of the StreamList.
        __len__(): Returns the number of elements in the StreamList.
        __getitem__(item): Returns the element at the specified index.
        __delitem__(i): Deletes the element at the specified index.
        __setitem__(i, v): Sets the element at the specified index to the given value.
        check(v): Checks if the given value is of the allowed type.
        append(streamchunk): Appends a StreamChunk object to the StreamList.
        insert(i, v): Inserts a StreamChunk object at the specified index.
        to_list(): Returns the StreamList as a regular list.
        to_list_serializable(): Returns the StreamList as a list of dictionaries.
        retrieve(retrieve_type, retriever_params): Retrieves StreamChunks using a retriever method.
        filter(filter_type, filter_params): Filters the StreamList using a filter method.
        rescore(rescore_type, rescore_params): Rescores the StreamList using a rescore method.
        merge(merge_type, merge_params): Merges the StreamList using a merge method.
        llm_action(summary_type, summary_params): Performs a summary action on the StreamList using the LLM method.
        sort(sort_type, sort_params): Sorts the StreamList using a sort method.
        groupby(groupby_type, groupby_params): Groups the StreamList using a groupby method.
    """

    def __init__(self) -> None:
        """
        Initializes a new instance of the StreamList class.
        """
        self.oktypes = StreamChunk
        self.streamlist = []

    def __iter__(self):
        """
        Returns an iterator over the StreamList.
        """
        for elem in self.streamlist:
            yield elem

    def __str__(self) -> str:
        """
        Returns a string representation of the StreamList.
        """
        return str(self.streamlist)

    def __repr__(self) -> str:
        """
        Returns a string representation of the StreamList.
        """
        return self.__str__()

    def __len__(self) -> int:
        """
        Returns the number of elements in the StreamList.
        """
        return len(self.streamlist)

    def __getitem__(self, item):
        """
        Returns the element at the specified index.
        """
        return self.streamlist[item]
    
    def __delitem__(self, i):
        """
        Deletes the element at the specified index.
        """
        del self.streamlist[i]

    def __setitem__(self, i, v):
        """
        Sets the element at the specified index to the given value.
        """
        self.check(v)
        self.streamlist[i] = v

    def check(self, v):
        """
        Checks if the given value is of the allowed type.

        Args:
            v: The value to check.

        Raises:
            DolffiaError: If the value is not of the allowed type.
        """
        if not isinstance(v, self.oktypes):
            raise DolffiaError(500, "Error trying to append non StreamChunk object.")
    
    def append(self, streamchunk):
        """
        Appends a StreamChunk object to the StreamList.

        Args:
            streamchunk: The StreamChunk object to append.
        """
        self.check(streamchunk)
        self.streamlist.append(streamchunk)

    def insert(self, i, v):
        """
        Inserts a StreamChunk object at the specified index.

        Args:
            i: The index at which to insert the StreamChunk object.
            v: The StreamChunk object to insert.
        """
        self.check(v)
        self.streamlist.insert(i, v)

    def to_list(self):
        """
        Returns the StreamList as a regular list.
        """
        return self.streamlist

    def to_list_serializable(self):
        """
        Returns the StreamList as a list of dictionaries.
        """
        return [vars(streamchunk) for streamchunk in self.streamlist]

    def join_get_content(self):
        """
        Joins the content of all StreamChunks in the StreamList.

        Returns:
            str: The joined content of all StreamChunks.
        """
        return " ".join([streamchunk.content for streamchunk in self.streamlist])
    
    def retrieve(self, retrieve_type, retriever_params) -> None:
        """
        Retrieves StreamChunks using a retriever method.

        Args:
            retrieve_type: The type of retriever method to use.
            retriever_params: The parameters for the retriever method.
        """
        retrievermethod = RetrieverFactory(retrieve_type)
        retrieve_chunks = retrievermethod.process(retriever_params)
        self.streamlist.extend([StreamChunk(chunk) for chunk in retrieve_chunks])
    
    def filter(self, filter_type, filter_params) -> None:
        """
        Filters the StreamList using a filter method.

        Args:
            filter_type: The type of filter method to use.
            filter_params: The parameters for the filter method.
        """
        filtermethod = FilterFactory(filter_type)
        self.streamlist = filtermethod.process(self.streamlist, filter_params)

    def rescore(self, rescore_type, rescore_params) -> None:
        """
        Rescores the StreamList using a rescore method.

        Args:
            rescore_type: The type of rescore method to use.
            rescore_params: The parameters for the rescore method.
        """
        rescoremethod = RescoreFactory(rescore_type)
        self.streamlist = rescoremethod.process(self.streamlist, rescore_params)

    def merge(self, merge_type, merge_params) -> None:
        """
        Merges the StreamList using a merge method.

        Args:
            merge_type: The type of merge method to use.
            merge_params: The parameters for the merge method.
        """
        mergemethod = MergeFactory(merge_type)
        self.streamlist = mergemethod.process(self.streamlist, merge_params)

    def llm_action(self, summary_type, summary_params) -> None:
        """
        Performs a summary action on the StreamList using the LLM method.

        Args:
            summary_type: The type of summary action to perform.
            summary_params: The parameters for the summary action.
        """
        llm_method = LLMFactory(summary_type)
        self.streamlist = llm_method.process(self.streamlist, summary_params)

    def sort(self, sort_type, sort_params) -> None:
        """
        Sorts the StreamList using a sort method.

        Args:
            sort_type: The type of sort method to use.
            sort_params: The parameters for the sort method.
        """
        sortmethod = SortFactory(sort_type)
        self.streamlist = sortmethod.process(self.streamlist, sort_params)

    def groupby(self, groupby_type, groupby_params) -> None:
        """
        Groups the StreamList using a groupby method.

        Args:
            groupby_type: The type of groupby method to use.
            groupby_params: The parameters for the groupby method.
        """
        groupbymethod = GroupByFactory(groupby_type)
        self.streamlist = groupbymethod.process(self.streamlist, groupby_params)
