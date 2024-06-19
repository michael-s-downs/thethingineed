### This code is property of the GGAO ###


import os

from abc import ABC, abstractmethod

from common.logging_handler import LoggerHandler
from common.services import GENAI_COMPOSE_SERVICE
from common.errors.dolffiaerrors import PrintableDolffiaError, DolffiaError

class AbstractManager(ABC):
    logger = LoggerHandler(GENAI_COMPOSE_SERVICE, level=os.environ.get('LOG_LEVEL', "INFO")).logger

    def get_defaults(self, defaults_dict, param_name):
        """Loads the default value for a param from the default params value dictionary.

        Args:
            defaults_dict (dict): Dictionary with the default values.
            param_name (str): Name of the param to load.

        Returns:
            param_value
        """
        param_value = defaults_dict.get(param_name, "param_not_found_123")
        if param_value is "param_not_found_123":
            self.raise_Dolffiaerror(404, f"Default param not found. Key: <{param_name}>")

        self.logger.debug(f"Param <{param_name}> set to default value: <{param_value}>")
        return param_value

    def get_param(self, params_dict, param_name, param_type, defaults_dict, mandatory = False):
        """Tries to load the param from the params dictionary and checks if the param has the correct type.
        If the param does not exist, tries to load it from the default params dictioanry.

        Args:
            params_dict (dict): Dictionary containing the params readed.
            param_name (str): Param name.
            param_type (type_instance): The type that the param should be.
            defaults_dict (dict): Dictionary with the default values for the params.
            mandatory (bool): If the param is necessary for the execution.

        Returns:
            param_value 
        """
        param_value = params_dict.get(param_name, "param_not_found_123")
        if param_value == "param_not_found_123":
            self.logger.debug(f"Param <{param_name}> not found setting default value")
            if mandatory:
                self.raise_PrintableDolffiaerror(404, "Mandatory param <query> not found in template params")
            else:
                return self.get_defaults(defaults_dict, param_name)

        if isinstance(param_value, param_type):
            return param_value 

        self.logger.debug(f"Param <{param_name}> type: {type(param_value)} not valid, expected type: {param_type} trying to set default value")
        return self.get_defaults(defaults_dict, param_name)
    
    def get_param_starts_by(self, params_dict, param_name, param_type, defaults_dict, mandatory = False):
        """Tries to load the param from the params dictionary that starts by 'param_name' and checks if the param has the correct type.
        If the param does not exist, tries to load it from the default params dictioanry.

        Args:
            params_dict (dict): Dictionary containing the params readed.
            param_name (str): Param name.
            param_type (type_instance): The type that the param should be.
            defaults_dict (dict): Dictionary with the default values for the params.
            mandatory (bool): If the param is necessary for the execution.

        Returns:
            param_value 
        """
        param_values = {}
        for key, value in params_dict.items():
            if key.startswith(param_name):
                param_values[key] = value
        if len(param_values) == 0:
            self.logger.debug(f"Param <{param_name}> not found setting default value")
            if mandatory:
                self.raise_PrintableDolffiaerror(404, "Mandatory param <query> not found in template params")
            else:
                return self.get_defaults(defaults_dict, param_name)

        for key, value in param_values.items():
            if not isinstance(value, param_type):
                param_values.pop(key)
                
        if len(param_values) == 0:
            self.logger.debug(f"Param <{param_name}> type: {type(param_values)} not valid, expected type: {param_type} trying to set default value")
            return self.get_defaults(defaults_dict, param_name)
        
        return param_values 


    def raise_Dolffiaerror(self, status_code, message):
        """Raises Dolffiaerror with a specific status_code and message.

        Args:
            status_code (int): Status code to return in the response error.
            message (str): Error message to return in the response.

        Raises:
            DolffiaError
        """
        raise DolffiaError(status_code=status_code, message=message)


    def raise_PrintableDolffiaerror(self, status_code, message):
        """Raises PrintableDolffiaerror with a specific status_code and message.

        Args:
            status_code (int): Status code to return in the response error.
            message (str): Error message to return in the response.

        Raises:
            DolffiaError
        """
        raise PrintableDolffiaError(status_code=status_code, message=message)
