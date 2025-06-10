### This code is property of the GGAO ###

import sys
import os
import glob

# Native imports
import json

# Intalled imports
from flask import Flask, request
from datetime import datetime
from typing import Dict, Tuple

# Custom imports
from common.deployment_utils import BaseDeployment
from common.genai_controllers import storage_containers, db_dbs, set_storage, set_db, upload_object, delete_file, list_files, load_file
from common.errors.genaierrors import PrintableGenaiError
from common.services import GENAI_COMPOSE_SERVICE
from common.genai_json_parser import get_compose_conf, get_dataset_status_key, get_generic, get_project_config
from common.genai_status_control import update_status
from director import Director
from langfusemanager import LangFuseManager


TEMPLATES_PATH = "src/compose/templates/"
FILTER_TEMPLATES_PATH = "src/compose/filter_templates/"

class ComposeDeployment(BaseDeployment):
    def __init__(self):
        """ Creates the deployment"""
        super().__init__()
        self.load_secrets()
        set_storage(storage_containers)
        set_db(db_dbs)
        self.langfuse_m = LangFuseManager()

    @property
    def must_continue(self) -> bool:
        """ True if the output should be sent to next step """
        return False

    @property
    def service_name(self) -> str:
        return GENAI_COMPOSE_SERVICE

    @property
    def max_num_queue(self) -> int:
        return 1

    def load_secrets(self) -> None:
        secrets_path = os.getenv('SECRETS_PATH', "/secrets")

        for secret_path in glob.glob(secrets_path + "/**/*.json", recursive=True):
            try:
                self.logger.debug(f"Loading secret '{secret_path}'")
                secret = json.loads(open(secret_path, "r").read())

                for envvar in secret:
                    if type(secret[envvar]) in [str, int]:
                        os.environ[envvar.upper()] = secret[envvar]
            except Exception as _:
                self.logger.warning(f"Unable to load secret '{secret_path}'")
    
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
            return response, status_code

        return response


    def process(self, json_input: dict):
        """ Retrieve information

        :param : Json input of the processes
        """
        self.logger.info("Request received")

        try:
            generic = get_generic(json_input)
            dataset_status_key = get_dataset_status_key(json_input)
            compose_conf = get_compose_conf(generic=generic)
            project_conf = get_project_config(generic=generic)
            apigw_params = {
                'x-reporting': project_conf['x-reporting'],
                'x-department': project_conf['x-department'],
                'x-tenant': project_conf['x-tenant'],
                'x-limits': project_conf.get('x-limits', json.dumps({})),
                'user-token': request.headers.get('user-token', ""),
                'delegate-token': request.headers.get('delegate-token', "")
            }

        except KeyError as ex:
            error_message = f"Error parsing JSON, Key: <{ex.args[0]}> not found"
            raise PrintableGenaiError(404, error_message)
        except Exception as ex:
            raise PrintableGenaiError(500, ex)

        output = Director(compose_conf, apigw_params).run(self.langfuse_m)

        resource = "compose/process/"
        self.report_api(1, "", apigw_params['x-reporting'], resource, dataset_status_key)

        return self.must_continue, output, ""

    def load_session_redis(self, json_input: dict):
        """Loads conversation to redis

        :param : Json input of the processes
        """
        self.logger.info("REDIS Request received")
        try:
            project_conf = json_input.get('project_conf')
            apigw_params = {
                'x-reporting': project_conf['x-reporting'],
                'x-department': project_conf['x-department'],
                'x-tenant': project_conf['x-tenant'],
                'x-limits': project_conf.get('x-limits', json.dumps({})),
                'user-token': request.headers.get('user-token', "")
            }

        except KeyError as ex:
            error_message = f"Error parsing JSON, Key: <{ex.args[0]}> not found"
            self.logger.error(error_message)
            return self.endpoint_response(404, "", error_message)
        except Exception as ex:
            self.logger.error(ex)
            return self.endpoint_response(500, "", str(ex))

        session_id = json_input.get("session_id")
        status_code = 200
        error_message = ""
        try:
            update_status(db_dbs['session'], f"session:{apigw_params['x-tenant']}:{session_id}",
                          json.dumps({
                              "conv": json_input['conv'],
                              "max_persistence": json_input.get("max_persistence", 5),
                              "context": "",
                              "last_update": datetime.today().strftime('%Y-%m-%d %H:%M:%S')
                          })
                          )
        except Exception as ex:
            error_message = "Error saving session to redis"
            status_code = 500
            self.logger.error(error_message + str(ex))
            return self.endpoint_response(status_code, "", error_message)

        resource = "compose/load_session/"
        self.report_api(1, "", apigw_params['x-reporting'], resource, {})
        self.logger.info(f"REDIS session {session_id} saved")

        return self.endpoint_response(status_code, f"Session <{session_id}> saved in redis", error_message)

    
    def upload_template(self, json_input, template_filter = False):
        """Uploads a compose template to cloud storage.

        Args:
            json_input (dict): Json input call
            filters (bool, optional): Use template of filter_template. Defaults to False.

        Returns:
            endpoint response
        """
        self.logger.info("Upload template request received")
        name = ""
        content = {}
        try:
            project_conf = json_input.get('project_conf')
            apigw_params = {
                'x-reporting': project_conf['x-reporting'],
                'x-department': project_conf['x-department'],
                'x-tenant': project_conf['x-tenant'],
                'x-limits': project_conf.get('x-limits', json.dumps({})),
                'user-token': request.headers.get('user-token', "")
            }
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
            path = TEMPLATES_PATH
            if template_filter:
                path = FILTER_TEMPLATES_PATH
                if self.langfuse_m.langfuse:
                    self.langfuse_m.upload_template(name, content, "compose_filter_template")
                else:
                    upload_object(storage_containers['workspace'], content, path + name + ".json")
            else:
                if self.langfuse_m.langfuse:
                    self.langfuse_m.upload_template(name, content, "compose_template")
                else:
                    upload_object(storage_containers['workspace'], content, path + name + ".json")

        except Exception as ex:
            error_message = f"Error uploading template file. {ex}"
            status_code = 500
            self.logger.error(ex)
            return self.endpoint_response(status_code, "", "Error uploading template file.")

        resource = "compose/upload_template/"
        self.report_api(1, "", apigw_params['x-reporting'], resource, {})

        return self.endpoint_response(status_code, f"File <{name}> uploaded", error_message)


    def delete_template(self, json_input, template_filter = False):
        """Deletes a compose template from cloud.

        Args:
            json_input (dict): Json input call.
            filters (bool, optional): Use template of filter_template. Defaults to False.

        Returns:
            endpoint response
        """
        self.logger.info("Delete template request received")
        name = ""
        try:
            project_conf = json_input.get('project_conf')
            apigw_params = {
                'x-reporting': project_conf['x-reporting'],
                'x-department': project_conf['x-department'],
                'x-tenant': project_conf['x-tenant'],
                'x-limits': project_conf.get('x-limits', json.dumps({})),
                'user-token': request.headers.get('user-token', "")
            }
            name = json_input['name']

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
            path = TEMPLATES_PATH
            if template_filter:
                label = "compose_filter_template"
                path = FILTER_TEMPLATES_PATH
                if self.langfuse_m.langfuse:
                    self.langfuse_m.delete_template(name, label)
                else:
                    delete_file(storage_containers['workspace'], path + name + ".json")
            else:
                label = "compose_template"
                if self.langfuse_m.langfuse:
                    self.langfuse_m.delete_template(name, label)
                else:
                    delete_file(storage_containers['workspace'], path + name + ".json")

        except Exception as ex:
            error_message = f"Error deleteting template file. {ex}"
            status_code = 500
            self.logger.error(ex)
            return self.endpoint_response(status_code, "", error_message)

        resource = "compose/delete_template/"
        self.report_api(1, "", apigw_params['x-reporting'], resource, {})
        return self.endpoint_response(status_code, "Request finished", error_message)
    
    def list_flows_compose(self, filters=False):
        """Lists all the templates stored

        Args:
            filters (bool, optional): Use template of filter_template. Defaults to False.

        Returns:
            endpoint_response
        """
        if filters:
            label = "compose_filter_template"
            str_path = FILTER_TEMPLATES_PATH
        else:
            label = "compose_template"
            str_path = TEMPLATES_PATH

        self.logger.info("List templates request received")
        try:
            if self.langfuse_m.langfuse:
                flows_templates = self.langfuse_m.get_list_templates(label)
            else:
                flows_templates = list_files(storage_containers['workspace'], str_path)
                flows_templates = [file_name.replace(str_path, "") for file_name in flows_templates]

        except Exception as ex:
            self.logger.error(ex)
            return self.endpoint_response(500, "", str(ex))

        return self.endpoint_response(200, flows_templates, "")

    def get_compose_template(self, json_input, filters=False):
        """Gets the content of a compose template

        Args:
            json_input (dict): Json input call
            filters (bool, optional): Use template of filter_template. Defaults to False.

        Returns:
            endpoint_response
        """
        if filters:
            label = "compose_filter_template"
            str_path = FILTER_TEMPLATES_PATH
        else:
            label = "compose_template"
            str_path = TEMPLATES_PATH

        self.logger.info("Get template content received")
        try:
            template_name = json_input['name']

        except KeyError as ex:
            error_message = f"Error parsing JSON, Key: <{ex.args[0]}> not found"
            self.logger.error(error_message)
            return self.endpoint_response(404, "", error_message)

        try:
            if self.langfuse_m.langfuse:
                template_content = self.langfuse_m.load_template(template_name, label)
                template_content = template_content.prompt
            else:
                template_content = load_file(storage_containers['workspace'], f"{str_path}{template_name}.json").decode()
            template_content = template_content.replace("\r", "")
            template_content = template_content.replace("\n", "")
        
        except Exception as ex:
            self.logger.error(ex)
            if "NotFoundError" in str(ex):
                return self.endpoint_response(404, "", str(ex))
            
            else:
                return self.endpoint_response(500, "", str(ex))

        return self.endpoint_response(200, template_content, "")


app = Flask(__name__)
deploy = ComposeDeployment()


@app.route('/process', methods=['POST'])
def sync_deployment() -> Tuple[Dict, int]:
    """ Deploy service in a sync way. """
    dat = request.get_json(force=True)
    apigw_params = {
        'x-tenant': request.headers['x-tenant'],
        'x-department': request.headers['x-department'],
        'x-reporting': request.headers['x-reporting'],
        'x-limits': request.headers.get('x-limits', json.dumps({})),
        'user-token': request.headers.get('user-token', ""),
        'delegate-token': request.headers.get('delegate-token', "")
    }
    dat['generic'].update({"project_conf": apigw_params})
    return deploy.sync_deployment(dat)


@app.route('/healthcheck', methods=['GET'])
def healthcheck() -> Dict:
    return {"status": "Service available"}


@app.route('/load_session', methods=['PUT'])
def load_session() -> Tuple[Dict, int]:
    dat = request.get_json(force=True)
    apigw_params = {
        'x-tenant': request.headers['x-tenant'],
        'x-department': request.headers['x-department'],
        'x-reporting': request.headers['x-reporting'],
        'x-limits': request.headers.get('x-limits', json.dumps({}))
    }
    dat.update({"project_conf": apigw_params})

    return deploy.load_session_redis(dat)


@app.route('/upload_template', methods=['PUT'])
def upload_template() -> Tuple[Dict, int]:
    """ Uploads and checks a template """
    dat = request.get_json(force=True)
    apigw_params = {
        'x-tenant': request.headers['x-tenant'],
        'x-department': request.headers['x-department'],
        'x-reporting': request.headers['x-reporting'],
        'x-limits': request.headers.get('x-limits', json.dumps({}))
    }
    dat.update({"project_conf": apigw_params})

    return deploy.upload_template(dat)


@app.route('/upload_filter_template', methods=['PUT'])
def upload_filter_template() -> Tuple[Dict, int]:
    """ Uploads and checks a template """
    dat = request.get_json(force=True)
    apigw_params = {
        'x-tenant': request.headers['x-tenant'],
        'x-department': request.headers['x-department'],
        'x-reporting': request.headers['x-reporting'],
        'x-limits': request.headers.get('x-limits', json.dumps({}))
    }
    dat.update({"project_conf": apigw_params})

    return deploy.upload_template(dat, template_filter=True)

@app.route('/delete_filter_template', methods=['DELETE'])
def delete_filter_template() -> Tuple[Dict, int]:
    """ Deletes a template """
    dat = {}
    dat.update(request.args)
    apigw_params = {
        'x-tenant': request.headers['x-tenant'],
        'x-department': request.headers['x-department'],
        'x-reporting': request.headers['x-reporting'],
        'x-limits': request.headers.get('x-limits', json.dumps({}))
    }
    dat.update({"project_conf": apigw_params})

    return deploy.delete_template(dat, template_filter=True)

@app.route('/delete_template', methods=['DELETE'])
def delete_template() -> Tuple[Dict, int]:
    """ Deletes a template """
    dat = {}
    dat.update(request.args)
    apigw_params = {
        'x-tenant': request.headers['x-tenant'],
        'x-department': request.headers['x-department'],
        'x-reporting': request.headers['x-reporting'],
        'x-limits': request.headers.get('x-limits', json.dumps({}))
    }
    dat.update({"project_conf": apigw_params})

    return deploy.delete_template(dat, template_filter=False)

@app.route('/list_templates', methods=['GET'])
def list_templates() -> Tuple[Dict, int]:
    "List compose template flows"
    return deploy.list_flows_compose()

@app.route('/list_filter_templates', methods=['GET'])
def list_filter_templates() -> Tuple[Dict, int]:
    "List compose filter template flows"
    return deploy.list_flows_compose(filters=True)

@app.route('/get_template', methods=['GET'])
def get_template() -> Tuple[Dict, int]:
    "List compose filter template flows"
    dat = request.args
    return deploy.get_compose_template(dat, filters=False)

@app.route('/get_filter_template', methods=['GET'])
def get_filter_template() -> Tuple[Dict, int]:
    "List compose filter template flows"
    dat = request.args
    return deploy.get_compose_template(dat, filters=True)


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=False, port=8888, use_reloader=False)
