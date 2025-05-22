### This code is property of the GGAO ###


import os
import requests
from requests.auth import HTTPBasicAuth 

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
        if os.getenv("LANGFUSE", False) == "true":
            langfuse_config = {
                "secret_key": os.getenv("LANGFUSE_SECRET_KEY", None),
                "public_key": os.getenv("LANGFUSE_PUBLIC_KEY", None),
                "host": os.getenv("LANGFUSE_HOST", None)
            }
            self.langfuse = Langfuse(**langfuse_config)
            self.langfuse_config = langfuse_config
            
        self.trace = None
    
    def parse(self, compose_config):
        """
        Parses the compose configuration and initializes the LangFuse integration.

        Args:
            compose_config (dict): The compose configuration.
            session_id (str): The session ID.

        Returns:
            LangFuseManager: The LangFuseManager instance.
        """
        if self.langfuse:
            return self

        if compose_config.get("langfuse"):
            langfuse_params = compose_config.get("langfuse")
            if isinstance(langfuse_params, dict):
                langfuse_config = {
                    "secret_key": langfuse_params['secret_key'],
                    "public_key": langfuse_params['public_key'],
                    "host": langfuse_params['host']
                }
                
            self.langfuse = Langfuse(**langfuse_config)

        return self

    def create_trace(self, session_id):
        if self.langfuse is None:
            return

        self.trace = self.langfuse.trace(
            session_id=session_id
        )



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
    

    
    def load_template(self, template_name, label="compose_template"):
        prompt = self.langfuse.get_prompt(template_name, label=label)
        return prompt
    
    def upload_template(self, template_name, template_content, label):
        result = self.langfuse.create_prompt(name=template_name, prompt=template_content, type="text", labels=[label, "latest"])
        return result
    
    def get_list_templates(self, label):
        host = self.langfuse_config["host"]
        sk = self.langfuse_config["secret_key"]
        pk = self.langfuse_config["public_key"]
        x = requests.get(
            f"{host}/api/public/v2/prompts",
            auth = HTTPBasicAuth(pk, sk),
            params= {"limit": 50, "label": label}
        )
        if x.status_code == 200:
            return [item['name'] for item in  x.json()["data"]] 
        
        raise Exception()
    
    def delete_template(self, template_name, label="compose_template"):
        prompt = self.langfuse.get_prompt(template_name, label=label)

        host = self.langfuse_config["host"]
        sk = self.langfuse_config["secret_key"]
        pk = self.langfuse_config["public_key"]
        x = requests.patch(
            f"{host}/api/public/v2/prompts/{template_name}/versions/{prompt.version}",
            auth=HTTPBasicAuth(pk, sk),
            json={"newLabels": ["deleted"]}
        )
        
        