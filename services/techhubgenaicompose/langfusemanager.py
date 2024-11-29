### This code is property of the GGAO ###


import os

from langfuse import Langfuse
from basemanager import AbstractManager

class LangFuseManager(AbstractManager):
    """
    A class that manages the LangFuse integration and manages the traces.
    """

    def __init__(self) -> None:
        """
        Initializes a new instance of the LangFuseManager class.

        Args:
            langfuse_config (dict): The configuration for LangFuse integration.
        """
        self.langfuse = None
        self.trace = None
    
    def parse(self, compose_config, session_id):
        """
        Parses the compose configuration and initializes the LangFuse integration.

        Args:
            compose_config (dict): The compose configuration.
            session_id (str): The session ID.

        Returns:
            LangFuseManager: The LangFuseManager instance.
        """
        if compose_config.get("langfuse") or os.getenv("LANGFUSE", False) == "true":
            langfuse_config = {
                "secret_key": os.getenv("LANGFUSE_SECRET_KEY", None),
                "public_key": os.getenv("LANGFUSE_PUBLIC_KEY", None),
                "host": os.getenv("LANGFUSE_HOST", None)
            }
            langfuse_params = compose_config.get("langfuse")
            if isinstance(langfuse_params, dict):
                langfuse_config = {
                    "secret_key": langfuse_params['secret_key'],
                    "public_key": langfuse_params['public_key'],
                    "host": langfuse_params['host']
                }
                
            self.langfuse = Langfuse(**langfuse_config)
            self.trace = self.langfuse.trace(
                session_id=session_id
            )

        return self


    def update_metadata(self, metadata):
        """
        Updates the metadata of the current trace.

        Args:
            metadata (dict): The metadata to update.
        """
        if self.langfuse is None:
            return

        self.trace.update(
            metadata = metadata
        )

    
    def update_input(self, input):
        """
        Updates the input of the current trace.

        Args:
            input (dict): The input to update.
        """
        if self.langfuse is None:
            return

        self.trace.update(
            input = input
        )

    
    def update_output(self, output):
        """
        Updates the output of the current trace.

        Args:
            output (dict): The output to update.
        """
        if self.langfuse is None:
            return

        self.trace.update(
            output = output
        )

    
    def add_span(self, name, metadata, input):
        """
        Adds a new span to the current trace.

        Args:
            name (str): The name of the span.
            metadata (dict): The metadata of the span.
            input (dict): The input of the span.

        Returns:
            span: The added span.
        """
        if self.langfuse is None:
            return

        span = self.trace.span(
            name = name,
            metadata=metadata,
            input = input
        )
    
        return span
    
    def add_span_output(self, span, output):
        """
        Adds the output to the specified span.

        Args:
            span_id (str): The ID of the span.
            output (dict): The output to add.
        """
        if self.langfuse is None:
            return

        span.end(
            output=output
        )
    
    def add_generation(self, name, metadata, input, model, model_params):
        """
        Adds a new generation to the current trace.

        Args:
            name (str): The name of the generation.
            metadata (dict): The metadata of the generation.
            input (dict): The input of the generation.
            model (str): The model used for the generation.
            model_params (dict): The parameters of the model.

        Returns:
            generation: The added generation.
        """
        if self.langfuse is None:
            return

        generation = self.trace.generation(
            name = name,
            metadata = metadata,
            input = input,
            model = model,
            model_parameters = model_params
        )

        return generation
    
    def add_generation_output(self, generation, output):
        """
        Adds the output to the specified generation.

        Args:
            generation_id (str): The ID of the generation.
            output (dict): The output to add.
        """
        if self.langfuse is None:
            return

        generation.end(
            output=output
        )
    

    def flush(self):
        """
        Flushes the LangFuse integration.
        """
        if self.langfuse is None:
            return

        self.langfuse.flush()