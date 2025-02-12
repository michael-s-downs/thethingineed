### This code is property of the GGAO ###


from basemanager import AbstractManager

class OutputManager(AbstractManager):

    def __init__(self, compose_conf) -> None:
        self.session_id = None
        self.scores = None
        self.lang = None
        self.n_conversation = None
        self.n_retrieval = None
        self.defaults_dict = {
            "scores": False,
            "lang": False,
            "n_conversation": 0,
            "n_retrieval": 0
        }
        self.parse(compose_conf)

    def parse(self, compose_conf):
        """Parse the elements to return in the output from the json input.

        Default: answer, session_id

        Args:
            compose_config (dict): Dictionary with compose configuration and params
        """

        output_config = compose_conf.get('output')
        if not output_config:
            self.logger.debug("Output not found in query")
            return
        self.scores = self.get_param(output_config, "scores", bool)
        self.lang = self.get_param(output_config, "lang", bool)
        self.n_conversation = self.get_param(output_config, "n_conversation", int)
        self.n_retrieval = self.get_param(output_config, "n_retrieval", int)
        self.logger.debug("Output parsed")


    def get_param(self, params: dict, param_name: str, param_type):
        return super().get_param(params, param_name, param_type, self.defaults_dict)

    
    def get_scores(self, output, sb):
        """Updates the output dict with the scores of the answer if it's the param in the API call

        Args:
            output (dict): Dictionary with the elements to return in the response
            sb (streambatch): Streambatch from retrieval
        """
        if self.scores and len(sb[0])>0:
            output['scores'] = sb[0][-1].scores


    def get_lang(self, output, lang):
        """Updates the output dict with the used language if it's the param in the API call

        Args:
            output (dict): Dictionary with the elements to return in the response
            lang (str): Lang used in the query
        """
        if self.lang:
            output['lang'] = lang
            self.logger.debug("Getting language for output object")

    
    def get_n_conversation(self, output, conversation):
        """Updates the output dict with the number of previous conversations if it's the param in the API call

        Args:
            output (dict): Dictionary with the elements to return in the response
            conversation (Conversation): List with the messages from the conversation
        """
        if self.n_conversation:
            output['n_conversation'] = {'n': self.n_conversation, 'conversation': ""}
            if not conversation:
                self.logger.error("Persist not activated, there is no conversation saved")
                output['n_conversation']['conversation'] = "Error, conversation empty"
                return
            if self.n_conversation>len(conversation):
                self.n_conversation = len(conversation)
            output['n_conversation']['conversation'] = conversation[-self.n_conversation:]
            self.logger.debug(f"Getting conversation of length {self.n_conversation} for output object")


    def get_n_retrieval(self, output, sb):
        """Updates the output dict with the number of previous retrievals if it's the param in the API call

        Args:
            output (dict): Dictionary with the elements to return in the response
            sb (streambatch): Streambatch from retrieval
        """
        if self.n_retrieval:
            output['n_retrieval'] = {'n': self.n_retrieval, 'retrieve': ""}
            streaml = sb.to_list_serializable()[0][:-1]
            if self.n_retrieval>len(streaml):
                self.n_retrieval = len(streaml)
            output['n_retrieval']['retrieve'] = streaml[:self.n_retrieval]
            self.logger.debug(f"Getting retrieval of length {self.n_retrieval} for output object")


    def get_answer(self, output, sb):
        """Updates the output dict with the answer from the Streambatch

        Args:
            output (dict): Dictionary with the elements to return in the response
            sb (streambatch): Streambatch from retrieval
        """
        if len(sb)>0 and len(sb[0]):
            output['answer'] = sb[0][-1].answer
        return output