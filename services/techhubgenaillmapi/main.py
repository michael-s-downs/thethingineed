### This code is property of the GGAO ###


# Native imports
import os
import json
import time
from typing import Dict, Tuple

# Installed imports
from flask import Flask, request
from pydantic import ValidationError

# Local imports
from common.genai_controllers import storage_containers, set_storage, set_queue, provider, upload_object, delete_file
from common.genai_json_parser import get_exc_info
from common.deployment_utils import BaseDeployment
from common.services import GENAI_LLM_SERVICE
from common.utils import load_secrets
from common.errors.genaierrors import PrintableGenaiError
from endpoints import ManagerPlatform, Platform
from generatives import ManagerModel, GenerativeModel
from common.indexing.loaders import ManagerLoader
from input_parsing import PlatformMetadata, LLMMetadata, QueryMetadata, ProjectConf


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
        self.origin = storage_containers.get('origin')
        self.loader = ManagerLoader.get_file_storage({"type": "LLMStorage", "workspace": self.workspace, "origin": self.origin})
        self.available_pools = self.loader.get_available_pools()
        self.available_models = self.loader.get_available_models()
        self.models_credentials, self.aws_credentials = load_secrets(vector_storage_needed=False)

        self.templates, self.templates_names = self.loader.get_templates()

        # Check if default templates are in the templates
        default_templates = set(model.DEFAULT_TEMPLATE_NAME for model in ManagerModel.MODEL_TYPES)
        if not default_templates.issubset(self.templates_names):
            raise PrintableGenaiError(400, f"Default templates not found: {default_templates}")
        self.logger.info("llmapi initialized")

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


    def error_message(self, status, message, status_code):
        """ Error message

        :param status: Status
        :param message: Message
        :param status_code: Status code
        :return: Error message
        """
        self.logger.info(message)
        return {'status': status, 'error_message': message, 'status_code': status_code}

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
            json_input["project_conf"] = apigw_params

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

    def get_template(self, template_name: str, template: str, lang: str, query: str, model: GenerativeModel):
        """ Get template

        :param template_name: Template name
        :param template: Template
        :return: Template
        """
        if template:
            # When both passed, template has preference
            return eval(template), ""

        elif template_name:
            if lang:
                template_name = f"{template_name}_{lang}"
            if template_name not in self.templates_names:
                raise ValueError(f"Invalid template name '{template_name}'. The valid ones are '{self.templates_names}'")
        else:
            if model.MODEL_MESSAGE in ["chatClaude3", "chatGPT-v"] and isinstance(query, str):
                model.DEFAULT_TEMPLATE_NAME = "system_query"
            template_name = model.DEFAULT_TEMPLATE_NAME
        return self.templates[template_name], template_name # When anyone passed, default_template_name is used

    def parse_platform(self, platform_metadata: dict):
        parsed_platform_metadata = PlatformMetadata(**platform_metadata).model_dump(exclude_unset=True, exclude_none=True)
        parsed_platform_metadata['aws_credentials'] = self.aws_credentials
        parsed_platform_metadata['models_urls'] = self.models_credentials.get('URLs')
        return ManagerPlatform.get_platform(parsed_platform_metadata)

    def parse_model(self, llm_metadata: dict, platform: Platform):
        parsed_llm_metadata = LLMMetadata(**llm_metadata).model_dump(exclude_unset=True, exclude_none=True)
        parsed_llm_metadata['models_credentials'] = self.models_credentials.get('api-keys').get(platform.MODEL_FORMAT,
                                                                                                {})
        model = ManagerModel.get_model(parsed_llm_metadata, platform.MODEL_FORMAT, self.available_models,
                                       self.available_pools)
        # Check max_tokens in dalle
        if model.model_type == "dalle3" and model.max_input_tokens > 4000:
            raise PrintableGenaiError(400, "Error, in dalle3 the maximum number of characters in the prompt is 4000")
        return model

    def parse_query(self, query_metadata: dict, model: GenerativeModel):
        query_metadata['is_vision_model'] = model.is_vision
        query_metadata['model_type'] = model.model_type
        query_metadata['template'], query_metadata['template_name'] = self.get_template(query_metadata.get('template_name'),
                                                       query_metadata.get('template'), query_metadata.get('lang'),
                                                       query_metadata.get('query'), model)
        parsed_query_metadata = QueryMetadata(**query_metadata).model_dump(exclude_unset=True, exclude_none=True)
        #Parameters passed to do pydantic checks now unused
        parsed_query_metadata.pop('is_vision_model')
        parsed_query_metadata.pop('model_type')

        return parsed_query_metadata

    def parse_project_conf(self, project_conf: dict, model: GenerativeModel, platform: Platform):
        project_conf['x_limits'] = json.loads(project_conf.get('x_limits', "{}"))
        project_conf['platform'] = platform.MODEL_FORMAT
        project_conf['model'] = model
        return ProjectConf(**project_conf).model_dump(exclude_unset=True, exclude_none=True)

    def parse_input(self, json_input: dict):
        """ Parse input

        :param json_input: Input data
        :return: Tuple with query metadata, llm metadata, platform metadata and report url
        """
        if not all(item in json_input.keys() for item in ['query_metadata', 'llm_metadata', 'platform_metadata']):
            raise ValueError("Missing mandatory fields ('query_metadata', 'llm_metadata' or 'platform_metadata')")

        platform = self.parse_platform(json_input.get('platform_metadata', {}))
        model = self.parse_model(json_input.get('llm_metadata', {}), platform)
        query_metadata = self.parse_query(json_input.get('query_metadata', {}), model)
        project_conf = self.parse_project_conf(json_input.get('project_conf', {}), model, platform)
        return query_metadata, model, platform, project_conf['x_reporting']

    def endpoint_response(self, status_code, result_message, error_message):
        response = {
            'status': "finished" if status_code == 200 else "error",
            'result': result_message,
            'status_code': status_code
        }
        if error_message:
            response = {
                'status': "finished" if status_code == 200 else "error",
                'error_message': error_message,
                'status_code': status_code
            }
            return json.dumps(response), status_code

        return json.dumps(response)

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
            query_metadata, model, platform, report_url = self.parse_input(json_input)

            # Set model
            platform.set_model(model)

            # Set message in model
            model.set_message(query_metadata)

            # Call model
            response = platform.call_model()

            # Format result
            result = model.get_result(response)
            self.logger.info(f"Result: {result}")
            if result['status_code'] == 200:
                if not eval(os.getenv('TESTING', "False")):
                    if model.MODEL_MESSAGE == "dalle":
                        reporting_type = "images"
                        n_tokens = result['result']['n']
                    else:
                        reporting_type = "tokens"
                        n_tokens = result['result']['n_tokens']
                    resource = f"llmapi/{platform.MODEL_FORMAT}/{model.model_type}/{reporting_type}"
                    self.report_api(n_tokens, "", report_url, resource, GENAI_LLM_SERVICE,
                                    reporting_type.upper())
                return self.adapt_output_queue(result)
            else:
                return self.adapt_output_queue(self.error_message('error', result['error_message'], result['status_code']))

        except ValidationError as ex:
            error = ex.errors()[0]
            location = error.get('loc')
            if error.get('type') == 'value_error' and len(location) > 0 and location[0] == 'x_limits':
                return self.adapt_output_queue(self.error_message('error', error.get("msg"), 429))
            error_depth_level = ''.join([f"['{el}']" for el in list(location)])
            output_msg = f"Error parsing JSON: '{error.get('msg')}' in parameter '{error_depth_level}' for value '{error.get('input')}'"
            return self.adapt_output_queue(self.error_message('error', output_msg, 400))
        except ValueError as ex:
            self.logger.error(f"[Process] Error parsing JSON. Query error: {ex}.", exc_info=exc_info)
            return self.adapt_output_queue(self.error_message('error', str(ex), 400))
        except Exception as ex:
            self.logger.error(f"[Process] Error parsing JSON. Query error: {ex}.", exc_info=exc_info)
            raise ex
    

    def upload_prompt_template(self, json_input):
        self.logger.info("Upload prompt template request received")
        name = ""
        content = {}
        try:
            name = json_input['name']
            content = json_input['content']

        except KeyError as ex:
            error_message = f"Error parsing JSON, Key: <{ex.args[0]}> not found"
            self.logger.error(error_message)
            return self.endpoint_response(404, "", error_message)

        except Exception as ex:
            self.logger.error(ex)
            return self.endpoint_response(500, "", str(ex))

        status_code = 200
        error_message = ""
        try:
            path = self.loader.prompts_path
            upload_object(self.workspace, content, path + name + ".json")
            time.sleep(0.5)
            self.templates, self.templates_names = self.loader.get_templates()

        except Exception as ex:
            error_message = f"Error uploading prompt file. {ex}"
            status_code = 500
            self.logger.error(ex)
            return self.endpoint_response(status_code, "", error_message)

        return self.endpoint_response(status_code, "Request finished", error_message)

    def delete_prompt_template(self, json_input):
        self.logger.info("Delete prompt template request received")
        name = ""
        status_code = 200
        try:
            name = json_input['name']

        except KeyError as ex:
            error_message = f"Error parsing JSON, Key: <{ex.args[0]}> not found"
            self.logger.error(error_message)
            return self.endpoint_response(404, "", error_message)

        except Exception as ex:
            self.logger.error(ex)
            return self.endpoint_response(500, "", str(ex))

        error_message = ""
        try:
            path = self.loader.prompts_path
            delete_file(self.workspace, path + name + ".json")
            time.sleep(0.5)
            self.templates, self.templates_names = self.loader.get_templates()


        except Exception as ex:
            error_message = f"Error deleting prompt file. {ex}"
            status_code = 500
            self.logger.error(ex)
            return self.endpoint_response(status_code, "", error_message)

        return self.endpoint_response(status_code, "Request finished", error_message)


app = Flask(__name__)
deploy = LLMDeployment()


@app.route('/predict', methods=['POST'])
def sync_deployment() -> Tuple[str, int]:
    """ Deploy service in a sync way. """
    json_input = request.get_json(force=True)

    apigw_params = {
        'x_tenant': request.headers['x-tenant'],
        'x_department': request.headers['x-department'],
        'x_reporting': request.headers['x-reporting'],
        'x_limits': request.headers.get('x-limits', "{}")
    }
    json_input["project_conf"] = apigw_params

    return deploy.sync_deployment(json_input)


@app.route('/reloadconfig', methods=['GET'])
def reloadconfig() -> Tuple[str, int]:
    deploy.logger.info("Reload config request received")
    deploy.templates, deploy.templates_names = deploy.loader.get_templates()
    result = json.dumps({
        'status': "ok",
        'status_code': 200
    }), 200

    return result


@app.route('/healthcheck', methods=['GET'])
def healthcheck() -> Dict:
    return {"status": "Service available"}


@app.route('/list_templates', methods=['GET'])
def list_available_templates() -> Tuple[str, int]:
    deploy.logger.info("List templates request received")
    return json.dumps({"status": "ok", "templates": deploy.templates_names, "status_code": 200}), 200

@app.route('/get_template', methods=['GET'])
def get_template() -> Tuple[str, int]:
    deploy.logger.info("Get template request received")
    template_name = request.args.get('template_name')
    if not template_name:
        return json.dumps({"status": "error", "error_message": "You must provide a 'template_name' param", "status_code": 400}), 400
    if template_name not in deploy.templates_names:
        return json.dumps({"status": "error", "error_message": f"Template '{template_name}' not found", "status_code": 404}), 404
    return json.dumps({"status": "ok", "template": deploy.templates[template_name], "status_code": 200}), 200



@app.route('/get_models', methods=['GET'])
def get_available_models() -> Tuple[str, int]:
    dat = request.args
    if len(dat) > 1:
        return json.dumps({"status": "error", "error_message":
            "You must provide only one parameter between 'platform', 'pool', 'zone' and 'model_type' param", "status_code": 400}), 400
    if dat.get("platform"):
        models = []
        pools = []
        for model in deploy.available_models.get(dat["platform"], []):
            models.append(model.get("model"))
            pools.extend(model.get("model_pool", []))
        return json.dumps({"status": "ok", "result": {"models": models, "pools": list(set(pools))}, "status_code": 200}), 200
    elif dat.get("pool"):
        models = [model.get("model") for model in deploy.available_pools.get(dat["pool"], [])]
        return json.dumps({"status": "ok", "models": models, "status_code": 200}), 200
    elif dat.get("model_type") or dat.get("zone"):
        key = "model_type" if dat.get("model_type") else "zone"
        response_models = []
        pools = []
        for platform, models in deploy.available_models.items():
            for model in models:
                if model.get(key) == dat[key]:
                    response_models.append(model.get("model"))
                    pools.extend(model.get("model_pool", []))
        return json.dumps({"status": "ok", "result": {"models": response_models, "pools": list(set(pools))}, "status_code": 200}), 200
    else:
        return json.dumps({"status": "error", "error_message": "You must provide a 'platform', 'pool', 'zone' or 'model_type' param", "status_code": 400}), 400


@app.route('/upload_prompt_template', methods=['POST'])
def upload_prompt_template() -> Tuple[str, int]:
    dat = request.get_json(force=True)
    return deploy.upload_prompt_template(dat)


@app.route('/delete_prompt_template', methods=['POST'])
def delete_prompt_template() -> Tuple[str, int]:
    dat = request.get_json(force=True)
    return deploy.delete_prompt_template(dat)


if __name__ == "__main__":
    if QUEUE_MODE:
        deploy.async_deployment()
    else:
        app.run(host="0.0.0.0", debug=False, port=8888)
