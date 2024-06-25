### This code is property of the GGAO ###


# Native imports
import json
from datetime import datetime

# Custom imports
from basemanager import AbstractManager
from common.errors.dolffiaerrors import PrintableDolffiaError
from common.dolffia_status_control import get_value, update_status
from common.genai_sdk_controllers import db_dbs


class PersistManager(AbstractManager):
    """Manages the persistence of data in the session.

    Attributes:
        logger: Logger instance.
        type (str): Type of persistence to be applied.
        params (dict): Parameters related to the persistence process.
    """

    def __init__(self):
        """Initializes the PersistManager with a logger.

        Args:
            logger: Logger instance
        """
        self.type = None
        self.params = None
        self.defaults_dict = {
            "persist": None,
            "type": "chat",
            "params": {},
            "max_persistence": 10
        }

    def parse(self, compose_config):
        """Parses the configuration from compose_conf json input.

        Args:
            compose_config (dict): Dictionary with compose configuration and params

        Returns:
            self, if a persistence configuration is found; otherwise, None.
        """
        self.logger.debug("Persit parse INIT")
        conf = self.get_param(compose_config, "persist", dict)
        if conf is None:
            return None

        self.type = self.get_param(conf, "type", str)
        self.params = self.get_param(conf, "params", dict)

        self.logger.debug("Persit parse END")
        return self

    def get_param(self, params:dict, param_name: str, param_type):
        return super().get_param(params, param_name, param_type, self.defaults_dict)

    def run(self, template, session_id, PD, reformulated):
        """Executes the persistence logic based on the provided template and session data.

        Args:
            template (dict): Template data to be persisted.
            session_id (string): Unique identifier for the session.
            PD (PersitDict): Instance of PersistDict for managing persistence data.

        Returns:
            Modified template with persistence data applied.
        """
        if self.type != "chat":
            return template

        for action in template:
            if action['action'] == 'summarize' or action['action'] == 'llm_action':
                action['action_params']['params']['session_id'] = session_id

        if reformulated:
            return template
        
        max_persistence = self.get_param(self.params, "max_persistence", int)

        # Initialize PD to fix the max_persistence
        PD.add({}, session_id=session_id, max_persistence=max_persistence)
        self.logger.debug(f"PersitDict init with {PD}")
        return template


class PersistDict():
    """Singleton class for managing persistence data across sessions.
    """
    _instance = None
    PD = None

    def __init__(self):
        """Initializes the PersistDict, ensuring only one instance exists.
        """
        if self.PD is None:
            self.PD = dict()
            self.REDIS_ORIGIN = db_dbs['session']

    def add(self, persistence:dict, session_id:str = None, max_persistence:int = None):
        """Adds a new session or adds persistance to an existing session
            If persistence is None we assume initialization.

        Args:
            persistence (dict): Persistence data to be added.
            session_id (string): Unique identifier for the session.
            max_persistence (int): Maximum number of persistent data entries.

        Returns:
            The session_id for which data is added or updated.
        """
        if session_id not in self.PD:
            self.PD[session_id] = Conversation(persistence, max_persistence=max_persistence)
        else:
            if max_persistence is not None: # When not defined, do not change it
                self.PD[session_id].max_persistence = max_persistence
            if persistence is not None:  # None is used for initialization purposes, do not add it
                self.PD[session_id].add(persistence)

        return session_id

    def get_from_redis(self, session_id, tenant):
        try:
            redis_session = get_value(self.REDIS_ORIGIN,f"session:{tenant}:{session_id}", format_json=False)[0]['values']
        except Exception as ex:
            raise PrintableDolffiaError(status_code=500, message=f"{ex}. \nError getting session from redis.")
    
        if redis_session:
            redis_session = json.loads(redis_session.decode())
            conv = Conversation(redis_session['conv'], max_persistence=redis_session['max_persistence'], context=redis_session['context'])
            self.PD[session_id] = conv
    
    def save_to_redis(self, session_id, tenant):
        try:
            conv = self.get_conversation(session_id)
            if not conv.is_response(): 
                return

            update_status(
                self.REDIS_ORIGIN,f"session:{tenant}:{session_id}",
                 json.dumps(
                    {"conv": self.get_conversation(session_id), 
                    "max_persistence": self.PD[session_id].max_persistence, 
                    "context": self.PD[session_id].context,
                    "last_update": datetime.today().strftime('%Y-%m-%d %H:%M:%S')
                    }
                 )
            )
            self.PD.clear()
        except Exception as ex:
            raise PrintableDolffiaError(status_code=500, message=f"{ex}. \nError saving session to redis.")
            
    def __getitem__(self, key):
        if not isinstance(key, str):
            raise PrintableDolffiaError(status_code=500, message="Session id must be a string")
        return self.PD.__getitem__(key)

    def get_conversation(self, session_id:str):
        """Retrieves the entire conversation data for a given session.

        Args:
            session_id (string): Unique identifier for the session.

        Returns:
            Conversation data for the specified session.
        """
        return self.PD.get(session_id)

    def update_last(self, persistence:dict, session_id:str):
        """Updates the most recent persistence data for a given session.

        Args:
            session_id (string): Unique identifier for the session.
            persistence: New persistence data to replace the old one.
        """
        self.PD[session_id].update_last(persistence)

    def remove_last(self, session_id:str):
        """Removes the most recent persistence data for a given session.

        Args:
            session_id (string): Unique identifier for the session.
        """
        self.PD[session_id].remove_last()

    def __new__(cls, *args, **kwargs):
        if not isinstance(cls._instance, cls):
            cls._instance = object.__new__(cls)

        return cls._instance

    def update_context(self, session_id, context):
        """Updates the context from the conversation.

        Args:
            session_id (string): Unique identifier for the session.
            context (str): Context used in the last llm call.
        """
        self.PD[session_id].update_context(context)


class Conversation(list):
    """Manages a list of persistence data entries for a session.
    """

    def __init__(self, persistence, max_persistence=10, context = ""):
        """Initializes the conversation with persistence data.

        Args:
            persistence (dict): Initial persistence data to be added.
            max_persistence (int): Maximum number of persistence entries.
        """
        if isinstance(persistence, list):
            super().__init__(persistence)
        else:
            super().__init__([persistence])
        self.max_persistence = max_persistence
        self.context = context

    def add(self, persistence):
        """Adds a new persistence data entry, maintaining the max limit.

        Args:
            persistence (dict): Persistence data to be added.
        """
        if not isinstance(persistence, dict):
            raise PrintableDolffiaError(status_code=500, message="Persistence must be a dict")
        self.append(persistence)

        if self.max_persistence is not None and len(self) > self.max_persistence:
            for _ in range(self.max_persistence, len(self)): # Delete elements till the length is equal to max persistence
                self.pop(0)

    def update_last(self, persistence):
        """Updates the most recent persistence data entry.

        Args:
            persistence (dict): New persistence data to replace the old one.
        """
        if not isinstance(persistence, dict):
            raise PrintableDolffiaError(status_code=500, message="Persistence must be a dict")
        self[-1] = persistence

    def remove_last(self):
        """Removes the most recent persistence data entry.
        """
        self.pop()
    
    def get_n_last(self, n):
        """Gets the last n items from the conversation.
        If the conversation length is less than the number requested returns all the conversation.

        Args:
            n (int): N last messages to get from the conversation

        Returns:
            conversation_list (Conversation): List of messages from the conversation
        """
        if n>len(self):
            return self
        return self[-n:]
    
    def update_context(self, context):
        """Update the context from the conversation.

        Args:
            context (str): Context used in the last llm call.
        """
        self.context = context
    
    def is_response(self):
        """Check if the last message is a response.

        Returns:
            bool: True if the last message is a response, False otherwise.
        """
        return "assistant" in list(self[-1].keys())

