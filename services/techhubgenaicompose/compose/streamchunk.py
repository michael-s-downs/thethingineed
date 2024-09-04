### This code is property of the GGAO ###


from statistics import mean
from common.errors.genaierrors import GenaiError


class StreamChunk:
    """
    Represents a stream chunk object.

    Attributes:
        content (str): The content of the stream chunk.
        meta (dict): The metadata associated with the stream chunk.
        scores (dict): The scores associated with the stream chunk.
        answer (str): The answer associated with the stream chunk.
    """

    def __init__(self, response_dict: dict) -> None:
        """
        Initializes a StreamChunk object.

        Args:
            response_dict (dict): A dictionary containing the response data.

        Returns:
            None
        """
        self.content = response_dict.get("content")
        self.meta = response_dict.get("meta")
        self.scores = response_dict.get("scores")
        self.answer = response_dict.get("answer")
        self.tokens = response_dict.get("tokens")
       

    def __str__(self) -> str:
        """
        Returns a string representation of the StreamChunk object.

        Returns:
            str: The string representation of the StreamChunk object.
        """
        return str(self)

    def __repr__(self) -> str:
        """
        Returns a string representation of the StreamChunk object.

        Returns:
            str: The string representation of the StreamChunk object.
        """
        return self.__str__()

    def get_mean_score(self):
        """
        Calculates and returns the mean score of the stream chunk.

        Returns:
            float: The mean score of the stream chunk.
        """
        return mean(self.scores.values())

    def get_metadata(self, param_name):
        """
        Retrieves the value of a specific metadata parameter.

        Args:
            param_name (str): The name of the metadata parameter.

        Returns:
            Any: The value of the specified metadata parameter.

        Raises:
            GenaiError: If the specified metadata parameter is not found.
        """
        param_name = param_name.lower()
        try:
            return self.meta[param_name]
        except:
            raise GenaiError(status_code=404, message=f"Param <{param_name}> not found in metadata")

    def get(self, param_name):
        """
        Retrieves the value of a specific parameter.

        Args:
            param_name (str): The name of the parameter.

        Returns:
            Any: The value of the specified parameter.

        Raises:
            GenaiError: If the specified parameter is not found.
        """
        if param_name == "content":
            return self.content

        if param_name == "answer":
            return self.answer

        if param_name == "metadata":
            return self.meta

        if param_name == "scores":
            return self.scores

        return self.get_metadata(param_name)

