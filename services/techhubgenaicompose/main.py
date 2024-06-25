### This code is property of the GGAO ###


# Native imports
import json
from multiprocessing import Process

# Intalled imports
from flask import Flask, request

# Custom imports
from common.deployment_utils import BaseDeployment
from common.genai_sdk_controllers import storage_containers, db_dbs, set_storage, set_db
from common.dolffia_json_parser import *
from common.errors.dolffiaerrors import PrintableDolffiaError
from common.services import GENAI_COMPOSE_SERVICE
from common.dolffia_json_parser import get_compose_conf, get_dataset_status_key, get_generic, get_project_config
from common.dolffia_status_control import update_status
from redis_cleaner import run_redis_cleaner
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

    def process(self, json_input: dict):
        """ Retrieve information

        :param : Json input of Dolffia processes
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
            raise PrintableDolffiaError(404, f"Error parsing JSON, Key: {ex.args[0]} not found")
        except Exception as ex:
            raise ex

        output = Director(compose_conf, apigw_params).run()

        resource = "compose/process/"
        self.report_api(1, "", apigw_params['x-reporting'], resource, dataset_status_key)

        return self.must_continue, output, ""

    def load_session_redis(self, json_input: dict):
        """Loads conversation to redis

        :param : Json input of Dolffia processes
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
            raise PrintableDolffiaError(404, f"Error parsing JSON, Key: {ex.args[0]} not found")
        except Exception as ex:
            raise ex

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
            self.logger.error(ex)

        resource = "compose/load_session/"
        self.report_api(1, "", apigw_params['x-reporting'], resource, {})
        self.logger.info(f"REDIS session {session_id} saved")

        response = {
            'status': "finished" if status_code == 200 else "error",
            'result': f"Session <{session_id}> saved in redis",
            'status_code': status_code
        }
        if error_message:
            response = {
                'status': "finished" if status_code == 200 else "error",
                'error_message': error_message,
                'status_code': status_code
            }
            return json.dumps(response), status_code

        response = json.dumps(response)
        return response


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


if __name__ == "__main__":
    Process(target=run_redis_cleaner).start()
    app.run(host="0.0.0.0", debug=False, port=8888)
