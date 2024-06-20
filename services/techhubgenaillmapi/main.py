### This code is property of the GGAO ###


# Native imports
import os
import json
import logging
from typing import Dict, Tuple

# Installed imports
from flask import Flask, request

# Local imports
from common.genai_sdk_controllers import storage_containers, set_storage, set_queue, provider
from common.dolffia_json_parser import get_exc_info, get_project_config
from common.deployment_utils import BaseDeployment
from common.services import GENAI_LLM_SERVICE
from endpoints import ManagerPlatform, Platform
from generatives import ManagerModel, GenerativeModel, DalleModel
from common.genai_sdk_controllers import load_file, list_files


QUEUE_MODE = eval(os.getenv('QUEUE_MODE', "False"))


class LLMDeployment(BaseDeployment):
    def __init__(self):
        """ Creates the deployment"""
        super().__init__()
        if QUEUE_MODE:
            self.Q_IN = (provider, os.getenv('Q_GENAI_LLMQUEUE_INPUT'))
            self.Q_OUT = (provider, os.getenv('Q_GENAI_LLMQUEUE_OUTPUT'))
            set_queue(self.Q_IN)
        set_storage(storage_containers)

        self.workspace = storage_containers.get('workspace')
        self.available_models = json.loads(load_file(self.workspace, "src/compose/conf/models_config.json").decode()).get('LLMs', {})
        self.load_templates()
        self.load_secrets()
        self.logger.info(f"llmapi initialized")

    def load_secrets(self):
        """ Load secrets from files or environment variables """
        aws_keys_path = os.path.join(os.getenv('SECRETS_PATH', '/secrets'), "aws", "aws.json")
        aws_env_vars = ["AWS_ACCESS_KEY", "AWS_SECRET_KEY"]
        models_keys_path = os.path.join(os.getenv('SECRETS_PATH', '/secrets'), "models", "models.json")

        # Load AWS credentials
        if os.path.exists(aws_keys_path):
            with open(aws_keys_path, "r") as file:
                self.aws_credentials = json.load(file)
        elif os.getenv(aws_env_vars[0], ""):
            self.aws_credentials = {
                'access_key': os.getenv(aws_env_vars[0]),
                'secret_key': os.getenv(aws_env_vars[1])
            }
        else:
            self.logger.info(f"AWS credentials not found in {aws_keys_path} or in environment variables {aws_env_vars}.")

        # Load models credentials
        if os.path.exists(models_keys_path):
            with open(models_keys_path, "r") as file:
                self.models_credentials = json.load(file)
        else:
            raise FileNotFoundError(f"Credentials file not found {models_keys_path}.")

    def load_templates(self):
        """ Load templates from storage or s3 """
        s3_files = list_files(self.workspace, "src/LLM/prompts")
        self.TEMPLATES = {}
        for file in s3_files:
            if file.endswith(".json"):
                aux_dict = json.loads(load_file(self.workspace, file))
                for key in self.TEMPLATES:
                    if key in ".json":
                        raise KeyError(f"Two create query jsons cannot have the same key {key}.")

                self.TEMPLATES.update(aux_dict)
        self.TEMPLATES_NAMES = list(self.TEMPLATES.keys())

    @property
    def must_continue(self) -> bool:
        """ True if the output should be sent to next step """
        return False

    @property
    def service_name(self) -> str:
        """ Service name """
        return GENAI_LLM_SERVICE.replace("api", "queue") if QUEUE_MODE else GENAI_LLM_SERVICE

    @property
    def max_num_queue(self) -> int:
        """ Maximum number of elements in the queue """
        return 1

    @staticmethod
    def is_maximum_tokens_reached(json_input: dict, platform: Platform, model: GenerativeModel) -> bool:
        """ Check if the maximum number of tokens has been reached

        :param json_input: Input data
        :param platform: Platform
        :param model: Model

        :return: True if the maximum number of tokens has been reached false otherwise
        """
        project_conf = get_project_config(json_input)
        dict_tokens = json.loads(project_conf.get('x-limits'))
        if model.MODEL_MESSAGE == "dalle":
            model_key = f'llmapi/{platform.MODEL_FORMAT}/{model.model_type}/images'
        else:
            model_key = f'llmapi/{platform.MODEL_FORMAT}/{model.model_type}/tokens'
        if dict_tokens and len(dict_tokens.get(model_key, {})) > 0:
            count = dict_tokens.get(model_key, {}).get('Current', 1)
            limit = dict_tokens.get(model_key, {}).get('Limit', 0)
            if count >= limit:
                return True

        return False

    @staticmethod
    def check_input_tokens_limit(json_input: dict):
        """ Check if input tokens limit is ok

        :param json_input: Input data
        """
        # Checking max tokens parameters:
        dict_tokens = json.loads(json_input.get('generic').get('project_conf').get('x-limits'))
        for key, value in dict_tokens.items():
            if not isinstance(value, dict):
                raise ValueError(f"Internal error, unsupported operand type(s) for -: {type(value)}" +
                        f" in {key}, it should be {dict}")

            if len(value) > 0:
                if (value.get('Current') is not None) and (value.get('Limit') is not None):
                    if not isinstance(value.get('Current'), int):
                        raise ValueError("Internal error, unsupported operand type(s) for -: " +
                            f"{type(value.get('Current'))} in current, it should be {int}")

                    if not isinstance(value.get('Limit'), int):
                        raise ValueError("Internal error, unsupported operand type(s) for -: " +
                            f"{type(value.get('Limit'))} in limit, it should be {int}")
                else:
                    raise ValueError(f"Internal error, current and limit are mandatory in x-limits")

    @staticmethod
    def check_params(json_input: dict, correct_params_types: dict, input_key):
        """ Check if the input parameters are correct

        :param json_input: Input data
        :param correct_params_types: Correct parameters and types
        :param input_key: Input key
        """
        for key, value in correct_params_types.items():
            input_param = json_input.get(input_key).get(key, None)
            if value[1] == "optional":
                if input_param is not None:
                    if not isinstance(input_param, value[0]):
                        raise ValueError(f"Internal error, unsupported operand type(s) for -: {type(input_param)} in "
                                       f"{key}, it should be {str(value[0])}")

            elif value[1] == "mandatory":
                if input_param is None:
                    raise ValueError(f"Internal error, {key} is mandatory")
                else:
                    if not isinstance(input_param, value[0]):
                        raise ValueError(f"Internal error, unsupported operand type(s) for -: {type(input_param)} in "
                                       f"{key}, it should be {str(value[0])}")
            else:
                raise ValueError(f"Error checking dict: {key}")

        if input_key == "llm_metadata":
            functions = json_input.get('llm_metadata').get('functions')
            function_call = json_input.get('llm_metadata').get('function_call')
            ## Function and function_call, depend on each other
            if functions:
                if function_call:
                    if not isinstance(functions, list):
                        raise ValueError(f"Internal error, unsupported operand type(s) for -: {type(functions)} in "
                                         f"functions it should be {list}")

                    if not isinstance(function_call, str):
                        raise ValueError(f"Internal error, unsupported operand type(s) for -: {type(function_call)} in "
                                       f"function_call it should be {str}")
                else:
                    raise ValueError(f"Internal error, function_call is mandatory because you put the functions param")

    def check_input(self, json_input: dict):
        """ Check if the input is correct

        :param json_input: Input data
        """
        mandatory_params = ["query_metadata", "llm_metadata", "platform_metadata"]
        for param in mandatory_params:
            if json_input.get(param) is None:
                raise ValueError(f"Internal error, parameter: {param} is mandatory")
            else:
                if not isinstance(json_input.get(param), dict):
                    raise ValueError(f"Internal error, unsupported operand type(s) for -: {type(json_input.get('param'))}"
                                   f"in {param} it should be {dict}")

        # query_metadata
        params_query_metadata = {
            "query": ((str, list), "mandatory"),
            "context": (str, "optional"),
            "system": (str, "optional"),
            "template_name": (str, "optional"),
            "template": (str, "optional"),
            "persistence": (list, "optional"),
        }
        self.check_params(json_input, params_query_metadata, "query_metadata")

        # llm_metadata
        params_llm_metadata = {
            "model": (str, "optional"),
            "max_input_tokens": (int, "optional"),
            "max_tokens": (int, "optional"),
            "n": (int, "optional"),
            "temperature": (int, "optional"),
            "stop": (list, "optional"),
            "quality": (str, "optional"),
            "response_format": (str, "optional"),
            "size": (str, "optional"),
            "style": (str, "optional"),
            "user": (str, "optional"),
            "top_p": (int, "optional"),
            "seed": (int, "optional")
        }
        self.check_params(json_input, params_llm_metadata, "llm_metadata")

        # platform_metadata
        params_platform_metadata = {
            "platform": (str, "mandatory"),
            "timeout": (int, "optional"),
        }
        self.check_params(json_input, params_platform_metadata, "platform_metadata")

    def get_template(self, base_template_name: str, lang: str) -> dict:
        """ Get template given the name

        :param base_template_name: Base template name
        :param lang: Language to search the template with specific language
        :return: Template
        """
        ### Template selection by language
        if lang:
            template_name = f"{base_template_name}_{lang}"
        else:
            template_name = base_template_name

        ## Check if template exists
        if template_name not in self.TEMPLATES_NAMES:
            self.load_templates()

        if template_name not in self.TEMPLATES_NAMES:
            if base_template_name not in self.TEMPLATES_NAMES:
                raise KeyError(
                    f"Query must be one of the possible ones {self.TEMPLATES_NAMES}. "
                    f"Remember to add new queries to the query metadata json.")
            else:
                template_name = base_template_name

        return self.TEMPLATES[template_name]

    def get_template_and_name(self, query_metadata: dict, model: GenerativeModel) -> Tuple[dict, str]:
        """ Get template and template name

        :param query_metadata: Query metadata
        :param model: Model
        :return: Tuple with template and template name
        """
        if model.MODEL_MESSAGE in ["chatClaude3", "chatGPT-v"] and isinstance(query_metadata.get('query'), str):
            model.default_template_name = "system_query"
        template_name = query_metadata.get("template_name", model.default_template_name)
        lang = query_metadata.get("lang", "")
        template = query_metadata.get("template")
        if not template:
            template = self.get_template(template_name, lang)
        else:
            template = eval(query_metadata['template'])
            template_name = ""

        if ((isinstance(query_metadata.get('query'), str) and not isinstance(template.get('user'), str)) or
                (isinstance(query_metadata.get('query'), list) and not isinstance(template.get('user'), list))):
            if model.is_vision:
                error_type = "In vision models must be a list"
            else:
                error_type = "In non vision models must be a string"
            raise ValueError(f"In the template '{template_name}' query does not match model query structure. "
                             f"{error_type}")
        return template, template_name

    def error_message(self, status, message, status_code):
        """ Error message

        :param status: Status
        :param message: Message
        :param status_code: Status code
        :return: Error message
        """
        self.logger.info(message)
        return {
            'status': status,
            'error_message': message,
            'status_code': status_code
        }

    def get_data_from_file(self, json_input: dict) -> dict:
        """ Get data from local mount path and replace input key if configured

        :param json_input: Input data
        :return: Input data adapted
        """
        mount_path = os.getenv('DATA_MOUNT_PATH', "")
        mount_key = os.getenv('DATA_MOUNT_KEY', "")

        if mount_path and mount_key:
            file_path = json_input.setdefault('query_metadata', {}).get(mount_key, "")

            if mount_path in file_path:
                self.logger.debug(f"Getting document from mount path '{mount_path}'")

                if os.path.exists(file_path):
                    if os.path.isfile(file_path):
                        try:
                            self.logger.debug(f"Getting document from path '{file_path}'")
                            with open(file_path, "r") as f:
                                file_content = f.read()
                        except:
                            file_content = ""

                        if file_content:
                            json_input['query_metadata'][mount_key] = file_content
                        else:
                            self.logger.error(f"Unable to read file '{file_path}'")
                    else:
                        self.logger.warning(f"Document path must be a file '{file_path}'")
                else:
                    self.logger.warning(f"Document path not found '{file_path}'")
            else:
                self.logger.warning(f"Document path '{file_path}' not inside mounted path '{mount_path}'")

        return json_input

    def adapt_input_queue(self, json_input: dict) -> dict:
        """ Input adaptations for queue case

        :param json_input: Input data
        :return: Input data adapted
        """
        if QUEUE_MODE:
            apigw_params = json_input.get('headers', {})
            json_input.setdefault('generic', {}).update({"project_conf": apigw_params})

        json_input = self.get_data_from_file(json_input)

        return json_input

    def adapt_output_queue(self, output: dict) -> Tuple[bool, dict, str]:
        """ Output adaptations for queue case

        :param output: Output data
        :return: Tuple with output result
        """
        if QUEUE_MODE:
            must_continue = True
            next_service = self.Q_OUT[1]
        else:
            must_continue = False
            next_service = ""

            if output.get('status_code', 200) == 200:
                output = output.get('result', {})

        return must_continue, output, next_service

    def parse_input(self, json_input: dict) -> Tuple[dict, dict, dict, str]:
        """ Parse input

        :param json_input: Input data
        :return: Tuple with query metadata, llm metadata, platform metadata and report url
        """
        ### Check input and tokens limit
        self.check_input(json_input)
        self.check_input_tokens_limit(json_input)

        ### Parse input
        query_metadata = json_input['query_metadata']
        llm_metatadata = json_input['llm_metadata']
        platform_metadata = json_input['platform_metadata']
        project_conf = get_project_config(json_input)
        report_url = project_conf['x-reporting']

        return query_metadata, llm_metatadata, platform_metadata, report_url

    def process(self, json_input: dict) -> Tuple[bool, dict, str]:
        """ Entry point to the service

        :param json_input: Input data
        :return: Tuple with output result

        """
        self.logger.info(f"Request received. Data: {json_input}")
        exc_info = get_exc_info()

        try:
            # Adaptations for queue case
            json_input = self.adapt_input_queue(json_input)

            # Parse and check input
            query_metadata, llm_metatadata, platform_metadata, report_url = self.parse_input(json_input)

            # Instantiate platform
            platform_metadata['aws_credentials'] = self.aws_credentials
            platform_metadata['models_credentials'] = self.models_credentials.get('URLs')
            platform = ManagerPlatform.get_platform(platform_metadata)

            # Instantiate model
            llm_metatadata['models_credentials'] = self.models_credentials.get('LLMs').get(platform.MODEL_FORMAT, {})
            model = ManagerModel.get_model(llm_metatadata, platform.MODEL_FORMAT, self.available_models)
            platform.set_model(model)

            # Get template
            template, template_name = self.get_template_and_name(query_metadata, model)
            model.template_ok(template)

            # Set template
            query_metadata['template'] = template
            query_metadata['template_name'] = template_name
            query_metadata.pop('lang', None)

            if self.is_maximum_tokens_reached(json_input, platform, model):
                return self.adapt_output_queue(self.error_message('error', f"Maximum tokens exceeded for model: {platform.MODEL_FORMAT}/{model.model_name}", 429))

            # Set message in model
            model.set_message(query_metadata)

            # Call model
            response = platform.call_model()

            # Format result
            result = model.get_result(response)
            self.logger.info(f"Result: {result}")
            if result['status_code'] == 200:
                if model.MODEL_MESSAGE == "dalle":
                    resource = f"llmapi/{platform.MODEL_FORMAT}/{model.model_type}/images"
                    self.report_api(result['result']['n'], "", report_url,
                                    resource, GENAI_LLM_SERVICE, "IMAGES")
                else:
                    resource = f"llmapi/{platform.MODEL_FORMAT}/{model.model_type}/tokens"
                    self.report_api(result['result']['n_tokens'], "", report_url,
                                    resource, GENAI_LLM_SERVICE, "TOKENS")
                return self.adapt_output_queue(result)
            else:
                return self.adapt_output_queue(self.error_message('error', result['error_message'], result['status_code']))

        except ValueError as ex:
            self.logger.error(f"[Process] Error parsing JSON. Query error: {ex}.", exc_info=exc_info)
            return self.adapt_output_queue(self.error_message('error', str(ex), 400))
        except Exception as ex:
            self.logger.error(f"[Process] Error parsing JSON. Query error: {ex}.", exc_info=exc_info)
            raise ex


app = Flask(__name__)
deploy = LLMDeployment()


@app.route('/predict', methods=['POST'])
def sync_deployment() -> Tuple[str, int]:
    """ Deploy service in a sync way. """
    json_input = request.get_json(force=True)

    apigw_params = {
        'x-tenant': request.headers['x-tenant'],
        'x-department': request.headers['x-department'],
        'x-reporting': request.headers['x-reporting'],
        'x-limits': request.headers.get('x-limits', json.dumps({}))
    }
    json_input.setdefault('generic', {}).update({"project_conf": apigw_params})

    return deploy.sync_deployment(json_input)


@app.route('/reloadconfig', methods=['GET'])
def reloadconfig() -> Tuple[str, int]:
    deploy.load_templates()
    result = json.dumps({
        'status': "ok",
        'status_code': 200
    }), 200

    return result


@app.route('/healthcheck', methods=['GET'])
def healthcheck() -> Dict:
    return {"status": "Service available"}


@app.route('/get_models', methods=['GET'])
def get_available_models() -> Tuple[str, int]:
    dat = request.get_json(force=True)

    model_type_filter = dat.get('model_type')
    model_pool_filter = dat.get('model_pool')

    temp_models = {}
    models = {}
    pools = {}
    available_models = deploy.available_models

    for platform in available_models:
        temp_models[platform] = []
        for model_info in available_models[platform]:
            if (not model_type_filter or model_info["model_type"] == model_type_filter) and \
                    (not model_pool_filter or model_pool_filter in model_info.get("model_pool", [])):
                model_name = model_info["model"]
                temp_models[platform].append(model_name)
                if model_info.get("model_pool"):
                    for pool_name in model_info["model_pool"]:
                        if not model_pool_filter or pool_name == model_pool_filter:
                            if pool_name not in pools:
                                pools[pool_name] = []
                            pools[pool_name].append(model_name)
        if temp_models[platform]:
            models[platform] = temp_models[platform]

    if not model_pool_filter:
        result = json.dumps({"models": models, "pools": pools}), 200

    else:
        result = json.dumps({"pools": pools}), 200

    return result


if __name__ == "__main__":
    if QUEUE_MODE:
        deploy.async_deployment()
    else:
        app.run(host="0.0.0.0", debug=False, port=8888)
