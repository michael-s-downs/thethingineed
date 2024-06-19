### This code is property of the GGAO ###

class DolffiaError(Exception):
    def __init__(self, status_code, message):
        """Dolffia Error used for developers only"""

        self.status_code = status_code
        self.message = message
        super().__init__(f"Error {status_code}: {message}")

class PrintableDolffiaError(Exception):
    def __init__(self, status_code, message):
        """Dolffia Error that can be seen by user"""

        self.status_code = status_code
        self.message = message
        super().__init__(f"Error {status_code}: {message}")
