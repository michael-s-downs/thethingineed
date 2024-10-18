### This code is property of the GGAO ###


# Native import
import importlib
import sys
import os
import inspect
import json

#Installed import
from genai_sdk_services import DIR_RESOURCES, DIR_ROOT
from genai_sdk_services.services.db import BaseDBService
from genai_sdk_services.services.queue_service import BaseQueueService
from genai_sdk_services.services.storage import BaseStorageService


try:
    with open(os.path.join(DIR_RESOURCES, "config.json"), "r") as f:
        config = json.load(f)
except:
    config = {'user_functions': []}

PATH = "user_functions"
classes = config['user_functions']
abstract_classes = [BaseStorageService, BaseQueueService, BaseDBService]


def check(x):
    check_ = False
    for abstract_class in abstract_classes:
        check_ = check_ or inspect.isclass(x) and issubclass(x, abstract_class) and x is not abstract_class

    return check_


def import_user_functions():
    modules = []
    path = os.path.join(DIR_ROOT, PATH)

    # List the user functions directory to search new functions
    for file_ in os.listdir(path):
        # Get filename
        file_ = file_.split(".py")[0]
        try:
            module = PATH.replace("/", ".")
            # Try to import the module to be able to search classes in it
            try:
                importlib.import_module(f"genai_sdk_services.{module}.{file_}")
            except:
                pass
            # Get all classes that implement the Abstract Class (To specify the abstract class)
            clsmembers = inspect.getmembers(sys.modules[f"genai_sdk_services.{module}.{file_}"], check)
            for cls in clsmembers:
                # cls is a Tuple(name, class)
                # If name of class is in the config file import it
                if cls[0] in classes:
                    modules.append(getattr(importlib.import_module(f"genai_sdk_services.{module}.{file_}"), cls[0]))
        except Exception as ex:
            print(f"Exception {str(ex)}")

    modules = list(set(modules))

    return modules
