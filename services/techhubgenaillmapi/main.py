### This code is property of the GGAO ###


# Native imports
import os
import json
from typing import Dict, Tuple

# Installed imports
from flask import Flask, request
from pydantic import ValidationError

# Local imports
from common.genai_controllers import storage_containers, set_storage, set_queue, provider
from common.genai_json_parser import get_exc_info
from common.deployment_utils import BaseDeployment
from common.services import GENAI_LLM_SERVICE
from common.utils import load_secrets
from common.errors.genaierrors import PrintableGenaiError
from common.models_manager import ManagerModelsConfig
from endpoints import ManagerPlatform, Platform
from generatives import GenerativeModel
from models.managergeneratives import ManagerModel
from common.storage_manager import ManagerStorage
from io_parsing import PlatformMetadata, LLMMetadata, QueryMetadata, ProjectConf, QUEUE_MODE, ResponseObject, adapt_input_queue
from common.utils import get_models

class LLMDeployment(BaseDeployment):
    def __init__(self):
        """ Creates the deployment"""
        super().__init__()
        if QUEUE_MODE:
            self.Q_IN = (provider, os.getenv('Q_GENAI_LLMQUEUE_INPUT'))
            set_queue(self.Q_IN)
        set_storage(storage_containers)
        self.workspace = storage_containers.get('workspace')
        self.origin = storage_containers.get('origin')
        self.storage_manager = ManagerStorage.get_file_storage({"type": "LLMStorage", "workspace": self.workspace, "origin": self.origin})
        self.available_pools = self.storage_manager.get_available_pools()
        self.available_models = self.storage_manager.get_available_models()
        self.default_models = self.storage_manager.get_default_models()
        self.models_credentials, self.aws_credentials = load_secrets(vector_storage_needed=False)
        self.models_config_manager = ManagerModelsConfig().get_models_config_manager({"type": "llm", 
                                                                                      "available_pools": self.available_pools, 
                                                                                      "available_models":self.available_models,
                                                                                      "models_credentials": self.models_credentials})

        self.templates, self.templates_names, self.display_templates_with_files = self.storage_manager.get_templates(return_files=True)

        # Check if default templates are in the templates
        default_templates = set(model.DEFAULT_TEMPLATE_NAME for model in ManagerModel.MODEL_TYPES)
        if not default_templates.issubset(self.templates_names):
            raise PrintableGenaiError(400, f"Default templates not found: {default_templates}")
        if eval(os.getenv('QUEUE_MODE', "False")):
            self.logger.info("llmqueue initialized")
        else:
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
            if lang and f"{template_name}_{lang}" in self.templates_names:
                template_name = f"{template_name}_{lang}"
            if template_name not in self.templates_names:
                raise ValueError(f"Invalid template name '{template_name}'. The valid ones are '{self.templates_names}'")
        else:
            if model.MODEL_MESSAGE in ["chatClaude-v", "chatGPT-v", "chatNova-v", "chatGPT-o"] and isinstance(query, str):
                model.DEFAULT_TEMPLATE_NAME = "system_query"
            template_name = model.DEFAULT_TEMPLATE_NAME
        return self.templates[template_name], template_name # When anyone passed, default_template_name is used

    def parse_platform(self, platform_metadata: dict):
        parsed_platform_metadata = PlatformMetadata(**platform_metadata).model_dump(exclude_unset=True, exclude_none=True)
        parsed_platform_metadata['aws_credentials'] = self.aws_credentials
        parsed_platform_metadata['models_urls'] = self.models_credentials.get('URLs')
        parsed_platform_metadata['models_config_manager'] = self.models_config_manager
        return ManagerPlatform.get_platform(parsed_platform_metadata)

    def parse_model(self, llm_metadata: dict, platform: Platform):
        llm_metadata['default_model'] = self.default_models.get(platform.MODEL_FORMAT)
        parsed_llm_metadata = LLMMetadata(**llm_metadata).model_dump(exclude_unset=True, exclude_none=True)
        parsed_llm_metadata['models_credentials'] = self.models_credentials.get('api-keys').get(platform.MODEL_FORMAT,
                                                                                                {})
        model = ManagerModel.get_model(parsed_llm_metadata, platform.MODEL_FORMAT,
                                       self.available_pools, self.models_config_manager)
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

        query_metadata.pop('lang', None) # Once template has been obtained, lang is not necessary

        parsed_query_metadata = QueryMetadata(**query_metadata).model_dump(exclude_unset=True, exclude_none=True)
        #Parameters passed to do pydantic checks now unused
        parsed_query_metadata.pop('is_vision_model')
        parsed_query_metadata.pop('model_type')

        return parsed_query_metadata

    def parse_project_conf(self, project_conf: dict, model: GenerativeModel, platform: Platform):
        project_conf['x-limits'] = json.loads(project_conf.get('x-limits', "{}"))
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

    def get_validation_error_response(self, error):
        """ Get validation error response

        :param error: Error
        :return: Error response
        """
        location = error.get('loc')
        if error.get('type') == 'value_error' and len(location) > 0 and location[0] == 'x-limits':
            return {'status': 'error', 'error_message': error.get("msg"), 'status_code': 429}
        error_depth_level = ''.join([f"['{el}']" for el in list(location)])
        return {
            'status': 'error',
            'error_message': f"Error parsing JSON: '{error.get('msg')}' in parameter '{error_depth_level}' for value '{error.get('input')}'",
            'status_code': 400
        }

    def process(self, json_input: dict) -> Tuple[bool, dict, str]:
        """ Entry point to the service

        :param json_input: Input data
        :return: Tuple with output result

        """
        self.logger.info(f"Request received. Data: {json_input}")
        exc_info = get_exc_info()
        queue_metadata = None

        try:
            # Adaptations for queue case
            json_input, queue_metadata = adapt_input_queue(json_input)

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
            if result['status_code'] == 200 and not eval(os.getenv('TESTING', "False")):
                if model.MODEL_MESSAGE == "dalle":
                    reporting_type = "images"
                    n_tokens = 1
                else:
                    reporting_type = "tokens"
                    n_tokens = result['result']['n_tokens']
                resource = f"llmapi/{platform.MODEL_FORMAT}/{model.model_type}/{reporting_type}"
                self.report_api(n_tokens, "", report_url, resource, GENAI_LLM_SERVICE,
                                reporting_type.upper())

        except ValidationError as ex:
            result = self.get_validation_error_response(ex.errors()[0])
            self.logger.error(f"[Process] {result['error_message']}.", exc_info=exc_info)
        except ValueError as ex:
            self.logger.error(f"[Process] Error parsing JSON. Error: {ex}.", exc_info=exc_info)
            result = {'status': 'error', 'error_message': str(ex), 'status_code': 400}
        except PrintableGenaiError as ex:
            self.logger.error(f"[Process] Error while processing: {ex}.", exc_info=exc_info)
            result = {'status': 'error', 'error_message': str(ex), 'status_code': ex.status_code}
        except Exception as ex:
            self.logger.error(f"[Process] Error while processing: {ex}.", exc_info=exc_info)
            result = {'status': 'error', 'error_message': str(ex), 'status_code': 500}

        return ResponseObject(**result).get_response_predict(queue_metadata)

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
        'x-limits': request.headers.get('x-limits', "{}")
    }
    json_input["project_conf"] = apigw_params

    return deploy.sync_deployment(json_input)


@app.route('/reloadconfig', methods=['GET'])
def reloadconfig() -> Tuple[str, int]:
    deploy.logger.info("Reload config request received")
    deploy.templates, deploy.templates_names, deploy.display_templates_with_files = deploy.storage_manager.get_templates(return_files=True)
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
    return ResponseObject(**{"status": "finished", "result": deploy.display_templates_with_files, "status_code": 200}).get_response_base()

@app.route('/get_template', methods=['GET'])
def get_template() -> Tuple[str, int]:
    deploy.logger.info("Get template request received")
    template_name = request.args.get('template_name')
    if not template_name:
        return ResponseObject(**{"status": "error", "error_message": "You must provide a 'template_name' param", "status_code": 400}).get_response_base()
    if template_name not in deploy.templates_names:
        return ResponseObject(**{"status": "error", "error_message": f"Template '{template_name}' not found", "status_code": 404}).get_response_base()
    return ResponseObject(**{"status": "finished", "result": {"template": deploy.templates[template_name]}, "status_code": 200}).get_response_base()

@app.route('/get_models', methods=['GET'])
def get_available_models() -> Tuple[str, int]:
    deploy.logger.info("Get models request received")
    dat = request.args
    if len(dat) != 1 or list(dat.items())[0][0] not in ['platform', 'pool', 'zone', 'model_type']:
        return ResponseObject(**{"status": "error", "error_message":
            "You must provide only one parameter between 'platform', 'pool', 'zone' and 'model_type' param", "status_code": 400}).get_response_base()
    key, value = list(dat.items())[0]
    models, pools = get_models(deploy.available_models, deploy.available_pools, key, value)
    return ResponseObject(**{"status": "finished", "result":
        {"models": models, "pools": list(set(pools)) if pools else []}, "status_code": 200}).get_response_base()

@app.route('/upload_prompt_template', methods=['POST'])
def upload_prompt_template() -> Tuple[str, int]:
    deploy.logger.info("Upload prompt template request received")
    dat = request.get_json(force=True)
    response = deploy.storage_manager.upload_template(dat)
    if response.get('status_code') == 200:
        # Update the templates modification in the llmapi component
        deploy.templates, deploy.templates_names, deploy.display_templates_with_files = deploy.storage_manager.get_templates(return_files=True)
    return ResponseObject(**response).get_response_base()


@app.route('/delete_prompt_template', methods=['POST'])
def delete_prompt_template() -> Tuple[str, int]:
    deploy.logger.info("Delete prompt template request received")
    dat = request.get_json(force=True)
    response = deploy.storage_manager.delete_template(dat)
    if response.get('status_code') == 200:
        # Update the templates modification in the llmapi component
        deploy.templates, deploy.templates_names, deploy.display_templates_with_files = deploy.storage_manager.get_templates(return_files=True)
    return ResponseObject(**response).get_response_base()

if __name__ == "__main__":
    if QUEUE_MODE:
        deploy.async_deployment()
    else:
        app.run(host="0.0.0.0", debug=False, port=8888, use_reloader=False)