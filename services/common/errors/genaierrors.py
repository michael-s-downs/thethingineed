### This code is property of the GGAO ###

class GenaiError(Exception):
    def __init__(self, status_code, message):
        """Genai Error used for developers only"""

        self.status_code = status_code
        self.message = message
        super().__init__(f"Error {status_code}: {message}")


class PrintableGenaiError(Exception):
    def __init__(self, status_code, message):
        """Genai Error that can be seen by user"""

        self.status_code = status_code
        self.message = message
        super().__init__(f"Error {status_code}: {message}")
