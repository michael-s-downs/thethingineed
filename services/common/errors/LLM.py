### This code is property of the GGAO ###

import logging

from .dolffiaerrors import DolffiaError, PrintableDolffiaError


class LLMParser():

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(logging.StreamHandler())

    def control_errors(self, r, async_bool=False):
        """Control errors from LLM API raising an error if necessary
        """

        status_code = r.status_code if not async_bool else r.status
        if status_code == 200:
            return None

        if status_code == 403:
            raise DolffiaError(status_code, f"Service is non reachable. Please check the url and apigw params. Error 403: {r.text}")

        try:
            error_message = r.json()['error_message'] if not async_bool else r.text()
        except Exception as e:
            raise DolffiaError(status_code, f"Error obtaining error json from llmapi. Error {status_code}: {r.text}") from e

        self.logger.error(f"Error {status_code}: {error_message}")
        if status_code == 400:
            raise PrintableDolffiaError(status_code, "The content was filtered out due to sensitive content. Please modify your request")
        elif status_code == 401:
            raise PrintableDolffiaError(status_code, "Credentials failed, please reach out to your administrator")
        elif status_code == 408:
            raise PrintableDolffiaError(status_code, "Request timed out, model might be overloaded, please try again")
        elif status_code == 503:
            raise PrintableDolffiaError(status_code, "Model is overloaded with requests. Please try again after a few minutes")
        elif status_code == 404:
            raise PrintableDolffiaError(status_code, "Deployed model is not available, check if the url is correct, otherwise please reach out to your administrator")
        elif status_code == 500:
            raise DolffiaError(status_code, error_message)
        else:
            self.logger.warning(f"Error not implemented for status code {status_code}")
            raise DolffiaError(status_code, error_message)

    def parse_response(self, r):
        """Parse response from LLM API returning the result if successful, otherwise raise an error
        """
        status_code = r.status_code
        self.control_errors(r)

        try:
            rjson = r.json()
        except Exception as e:
            self.logger.error(f"Error obtaining json: {e}")
            raise DolffiaError(status_code, "Error obtaining json from llmapi")

        try:
            result = rjson['result']
        except Exception as e:
            raise DolffiaError(status_code, "Error obtaining result from llmapi")

        return result
