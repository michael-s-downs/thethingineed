### This code is property of the GGAO ###


from .streamlist import StreamList
from .batchactions import MergeBatchFactory, SplitBatchFactory, CombineBatchFactory, BatchSortFactory


class StreamBatch:
    """
    Represents a batch of StreamList objects.

    Args:
        *streamlist: Variable number of StreamList objects.

    Attributes:
        streambatch (List[StreamList]): List of StreamList objects.

    Methods:
        __str__(): Returns a string representation of the StreamBatch object.
        __repr__(): Returns a string representation of the StreamBatch object.
        __len__(): Returns the number of StreamList objects in the StreamBatch.
        __getitem__(item): Returns the StreamList object at the specified index.
        to_list(): Converts the StreamBatch object to a list of lists.
        to_list_serializable(): Converts the StreamBatch object to a list of serializable lists.
        shape(): Returns the shape of the StreamBatch object.
        add(new_streamlist): Adds a new StreamList object to the StreamBatch.
        retrieve(retrieve_type, retrieve_params): Retrieves data from the StreamList objects in the StreamBatch.
        filter(filter_type, filter_params): Filters the StreamList objects in the StreamBatch.
        sort(sort_type, sort_params): Sorts the StreamList objects in the StreamBatch.
        groupby(group_type, group_params): Groups the StreamList objects in the StreamBatch.
        rescore(rescore_type, rescore_params): Rescores the StreamList objects in the StreamBatch.
        merge(merge_type, merge_params): Merges the StreamList objects in the StreamBatch.
        llm_action(summary_type, summary_params): Performs an action on the StreamList objects in the StreamBatch.
        batchmerge(merge_type, merge_params): Merges the StreamList objects in the StreamBatch using batch merge.
        batchsplit(split_type, split_params): Splits the StreamList objects in the StreamBatch using batch split.
        batchcombine(split_type, split_params): Combines the StreamList objects in the StreamBatch using batch combine.
        batchsort(sort_type, sort_params): Sorts the StreamList objects in the StreamBatch using batch sort.
    """

    def __init__(self, *streamlist: StreamList) -> None:
        """Initialize the StreamBatch object.

        Args:
            *streamlist: Variable number of StreamList objects.
        """
        if isinstance(streamlist, tuple) and len(streamlist) == 0:
            self.streambatch = []
        else:
            if isinstance(streamlist[0], list):
                raise ValueError("Do not pass a list of elements, pass each element as a separated argument")
            self.streambatch = list(streamlist)

    def __str__(self) -> str:
        """Return a string representation of the StreamBatch object."""
        return str(self.streambatch)

    def __repr__(self) -> str:
        """Return a string representation of the StreamBatch object."""
        return self.__str__()

    def __len__(self) -> int:
        """Return the number of StreamList objects in the StreamBatch."""
        return len(self.streambatch)

    def __getitem__(self, item):
        """Return the StreamList object at the specified index."""
        return self.streambatch[item]

    def to_list(self):
        """Convert the StreamBatch object to a list of lists."""
        streambatch = []
        for streamlist in self.streambatch:
            streambatch.append(streamlist.to_list())
        return streambatch

    def to_list_serializable(self):
        """Convert the StreamBatch object to a list of serializable lists."""
        streambatch = []
        for streamlist in self.streambatch:
            streambatch.append(streamlist.to_list_serializable())
        return streambatch

    def shape(self):
        """Return the shape of the StreamBatch object."""
        return (len(self), max([len(sl) for sl in self.streambatch]))

    def add(self, new_streamlist: StreamList) -> None:
        """Add a new StreamList object to the StreamBatch.

        Args:
            new_streamlist: The StreamList object to be added.
        """
        self.streambatch.append(new_streamlist)
        return self
    
    def retrieve(self, retrieve_type, retrieve_params) -> None:
        """Retrieve data from the StreamList objects in the StreamBatch.

        Args:
            retrieve_type: The type of retrieval operation to be performed.
            retrieve_params: Parameters for the retrieval operation.
        """
        sls = StreamList()
        sls.retrieve(retrieve_type, retrieve_params)
        self.streambatch.append(sls)
            
    def filter(self, filter_type, filter_params) -> None:
        """Filter the StreamList objects in the StreamBatch.

        Args:
            filter_type: The type of filter to be applied.
            filter_params: Parameters for the filter operation.
        """
        for streamlist in self.streambatch:
            streamlist.filter(filter_type, filter_params)

    def sort(self, sort_type, sort_params) -> None:
        """Sort the StreamList objects in the StreamBatch.

        Args:
            sort_type: The type of sort to be applied.
            sort_params: Parameters for the sort operation.
        """
        for streamlist in self.streambatch:
            streamlist.sort(sort_type, sort_params)
        
    def groupby(self, group_type, group_params) -> None:
        """Group the StreamList objects in the StreamBatch.

        Args:
            group_type: The type of grouping to be applied.
            group_params: Parameters for the grouping operation.
        """
        for streamlist in self.streambatch:
            streamlist.groupby(group_type, group_params)

    def rescore(self, rescore_type, rescore_params) -> None:
        """Rescore the StreamList objects in the StreamBatch.

        Args:
            rescore_type: The type of rescoring to be applied.
            rescore_params: Parameters for the rescoring operation.
        """
        for streamlist in self.streambatch:
            streamlist.rescore(rescore_type, rescore_params)

    def merge(self, merge_type, merge_params) -> None:
        """Merge the StreamList objects in the StreamBatch.

        Args:
            merge_type: The type of merge to be applied.
            merge_params: Parameters for the merge operation.
        """
        for streamlist in self.streambatch:
            streamlist.merge(merge_type, merge_params)

    def llm_action(self, summary_type, summary_params) -> None:
        """Perform an action on the StreamList objects in the StreamBatch.

        Args:
            summary_type: The type of action to be performed.
            summary_params: Parameters for the action.
        """
        for streamlist in self.streambatch:
            streamlist.llm_action(summary_type, summary_params)
    
    def filter_response(self, filter_type, filter_params):
        for streamlist in self.streambatch:
            streamlist.filter_response(filter_type, filter_params)

    def batchmerge(self, merge_type, merge_params) -> None:
        """Merge the StreamList objects in the StreamBatch using batch merge.

        Args:
            merge_type: The type of merge to be applied.
            merge_params: Parameters for the merge operation.
        """
        mbf = MergeBatchFactory(merge_type)
        self.streambatch = [mbf.process(self.streambatch, merge_params)]

    def batchsplit(self, split_type, split_params) -> None:
        """Split the StreamList objects in the StreamBatch using batch split.

        Args:
            split_type: The type of split to be applied.
            split_params: Parameters for the split operation.
        """
        sbf = SplitBatchFactory(split_type)
        self.streambatch = sbf.process(self.streambatch, split_params)

    def batchcombine(self, split_type, split_params) -> None:
        """Combine the StreamList objects in the StreamBatch using batch combine.

        Args:
            split_type: The type of combine to be applied.
            split_params: Parameters for the combine operation.
        """
        sbf = CombineBatchFactory(split_type)
        self.streambatch = [sbf.process(self.streambatch, split_params)]

    def batchsort(self, sort_type, sort_params) -> None:
        """Sort the StreamList objects in the StreamBatch using batch sort.

        Args:
            sort_type: The type of sort to be applied.
            sort_params: Parameters for the sort operation.
        """
        sbf = BatchSortFactory(sort_type)
        self.streambatch = [sbf.process(self.streambatch, sort_params)]
    

