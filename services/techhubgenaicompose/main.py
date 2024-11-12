### This code is property of the GGAO ###

# Native imports
import json

# Intalled imports
from flask import Flask, request

# Custom imports
from common.deployment_utils import BaseDeployment
from common.genai_controllers import storage_containers, db_dbs, set_storage, set_db, upload_object, delete_file, list_files
from common.genai_json_parser import *
from common.errors.genaierrors import PrintableGenaiError
from common.services import GENAI_COMPOSE_SERVICE
from common.genai_json_parser import get_compose_conf, get_dataset_status_key, get_generic, get_project_config
from common.genai_status_control import update_status
from director import Director


class ComposeDeployment(BaseDeployment):
    def __init__(self):
        """ Creates the deployment"""
        super().__init__()
        set_storage(storage_containers)
        set_db(db_dbs)

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
                'user-token': request.headers.get('user-token', "")
            }

        except KeyError as ex:
            error_message = f"Error parsing JSON, Key: <{ex.args[0]}> not found"
            raise PrintableGenaiError(404, error_message)
        except Exception as ex:
            raise PrintableGenaiError(500, ex)

        output = Director(compose_conf, apigw_params).run()

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
            path = "src/compose/templates/"
            if template_filter:
                path = "src/compose/filter_templates/"
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
            path = "src/compose/templates/"
            if template_filter:
                path = "src/compose/filter_templates/"
            delete_file(storage_containers['workspace'], path + name + ".json")

        except Exception as ex:
            error_message = f"Error deleteting template file. {ex}"
            status_code = 500
            self.logger.error(ex)
            return self.endpoint_response(status_code, "", error_message)

        resource = "compose/delete_template/"
        self.report_api(1, "", apigw_params['x-reporting'], resource, {})
        return self.endpoint_response(status_code, "Request finished", error_message)
    
    def list_flows_compose(self):
        str_path = "src/compose/templates/"
        try:
            flows_templates = list_files(storage_containers['workspace'], str_path)
            flows_templates = [file_name.replace(str_path, "") for file_name in flows_templates]

        except Exception as ex:
            self.logger.error(ex)
            return self.endpoint_response(500, "", str(ex))

        return self.endpoint_response(200, flows_templates, "")




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
        'user-token': request.headers.get('user-token', "")
    }
    dat['generic'].update({"project_conf": apigw_params})
    return deploy.sync_deployment(dat)


@app.route('/healthcheck', methods=['GET'])
def healthcheck() -> Dict:
    return {"status": "Service available"}


@app.route('/load_session', methods=['POST'])
def load_session() -> Tuple[Dict, int]:
    """ Deploy service in a syprocesunc way. """
    dat = request.get_json(force=True)
    apigw_params = {
        'x-tenant': request.headers['x-tenant'],
        'x-department': request.headers['x-department'],
        'x-reporting': request.headers['x-reporting'],
        'x-limits': request.headers.get('x-limits', json.dumps({}))
    }
    dat.update({"project_conf": apigw_params})

    return deploy.load_session_redis(dat)


@app.route('/upload_template', methods=['POST'])
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


@app.route('/upload_filter_template', methods=['POST'])
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

@app.route('/delete_filter_template', methods=['POST'])
def delete_filter_template() -> Tuple[Dict, int]:
    """ Deletes a template """
    dat = request.get_json(force=True)
    apigw_params = {
        'x-tenant': request.headers['x-tenant'],
        'x-department': request.headers['x-department'],
        'x-reporting': request.headers['x-reporting'],
        'x-limits': request.headers.get('x-limits', json.dumps({}))
    }
    dat.update({"project_conf": apigw_params})

    return deploy.delete_template(dat, template_filter=True)

@app.route('/delete_template', methods=['POST'])
def delete_template() -> Tuple[Dict, int]:
    """ Deletes a template """
    dat = request.get_json(force=True)
    apigw_params = {
        'x-tenant': request.headers['x-tenant'],
        'x-department': request.headers['x-department'],
        'x-reporting': request.headers['x-reporting'],
        'x-limits': request.headers.get('x-limits', json.dumps({}))
    }
    dat.update({"project_conf": apigw_params})

    return deploy.delete_template(dat, template_filter=False)

@app.route('/list_flows', methods=['GET'])
def list_flows() -> Tuple[Dict, int]:
    "List compose template flows"

    return deploy.list_flows_compose()


if __name__ == "__main__":
    #Process(target=run_redis_cleaner).start()
    app.run(host="0.0.0.0", debug=False, port=8888, use_reloader=False)
