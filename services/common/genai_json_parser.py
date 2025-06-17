### This code is property of the GGAO ###


"""
Functions to parse Genai configuration JSON
"""
# Native imports
import os
import logging
import string
from random import choice
from datetime import datetime
from typing import List, Optional, Dict, Union, Tuple

ProjectConfig = Dict[str, Union[bool, str, List[str], Dict[str, int]]]
OcrConfig = Dict[str, int]
TranslationConfig = Dict[str, int]
MultilabelDatasetConfig = Dict[str, Union[str, None, Dict[str, Dict[str, List[str]]]]]
DatasetGenericConfig = Dict[str, Union[str, Optional[int], Optional[MultilabelDatasetConfig]]]
PreprocessConfig = Dict[str, Union[int, List, Dict[str, Union[float, bool]]]]
OriginsConfig = Dict[str, Union[str, List[str], Dict[str, List[str]]]]
PredictConfig = Dict[str, Union[int, float, Dict[str, str]]]
AtomModelConfig = Dict[str, Union[str, Dict[str, Union[str, int, float, List[Union[str, int, float]]]]]]
HyperparamsConfig = Dict[str, Union[int, Dict[str, Union[int, float, str]], List[AtomModelConfig]]]
GenaiModelConfig = Dict[str, Union[str, HyperparamsConfig]]
TrainConfig = Dict[str, List[GenaiModelConfig]]
GenericConfig = Dict[str, Union[ProjectConfig, DatasetGenericConfig, PreprocessConfig,
                                OcrConfig, TranslationConfig, OriginsConfig,
                                Optional[PredictConfig], Optional[TrainConfig]]]

DatasetSpecificConfig = Dict[str, str]
DocumentSpecificConfig = Dict[str, Union[str, int]]
PathsSpecificConfig = Dict[str, Union[str, List[str]]]
SpecificConfig = Dict[str, Union[Dict, str, DatasetSpecificConfig, DocumentSpecificConfig, PathsSpecificConfig]]

CredentialsConfig = Dict[str, Dict[str, Union[Dict[str, str], Dict[str, Dict[str, str]]]]]

GenaiInput = Dict[str, Union[GenericConfig, SpecificConfig, CredentialsConfig]]


def get_credentials(json_input: GenaiInput) -> CredentialsConfig:
    """ Get the credentials config from the json input

    :param json_input: Json input of Genai processes
    :return: (dict) Credentials config
    """
    return json_input.get('credentials', {})


def get_generic(json_input: GenaiInput) -> GenericConfig:
    """ Get the generic config from the json input

    :param json_input: Json input of genai processes
    :return: (dict) Generic config
    """
    return json_input['generic']


def get_specific(json_input: GenaiInput) -> SpecificConfig:
    """ Get the specific config from the json input

    :param json_input: Json input of genai processes
    :return: (dict) Specific config
    """
    return json_input.get('specific', {})


def get_department(json_input: Optional[GenaiInput] = None, generic: Optional[GenericConfig] = None) -> str:
    """ Get department

    :param json_input: Json_input of genai processes
    :param generic: (optional) If defined, generic configuration of genai processes
    :return: (dict) Department
    """
    assert json_input or generic
    project_conf = get_project_config(json_input, generic)
    return project_conf['department']


def get_document(json_input: Optional[GenaiInput] = None, specific: Optional[SpecificConfig] = None) -> DocumentSpecificConfig:
    """ Get the dataset keys

    :param json_input: Json_input of genai processes
    :param specific: (optional) If defined, specific configuration of genai processes
    :return: (dict) Document config
    """
    assert json_input or specific
    if not specific:
        specific = get_specific(json_input)
    return specific.get('document', {})


def get_dataset_keys(json_input: Optional[GenaiInput] = None, specific: Optional[SpecificConfig] = None) -> DatasetSpecificConfig:
    """ Get the dataset keys

    :param json_input: Json_input of genai processes
    :param specific: (optional) If defined, specific configuration of genai processes
    :return: (dict) Dataset status keys
    """
    assert json_input or specific
    if not specific:
        specific = get_specific(json_input)
    return specific.get('dataset', {})


def get_exc_info(cutoff=10) -> bool:
    """ Get exception info by level of log

    :param cutoff: Cutoff level
    :return: Exception info
    """
    log_level = os.environ.get('LOG_LEVEL', "INFO")
    numeric_level = getattr(logging, log_level.upper(), 20)
    a = numeric_level <= cutoff
    return numeric_level <= cutoff


def get_dataset_status_key(json_input: Optional[GenaiInput] = None, specific: Optional[SpecificConfig] = None) -> str:
    """ Get the dataset status key

    :param json_input: Json_input of genai processes
    :param specific: (optional) If defined, specific configuration of genai processes
    :return: (str) Dataset status key
    """
    assert json_input or specific

    if json_input:
        json_input = json_input.get('request_json', json_input)

        if "dataset_status_key" in json_input:
            dataset_status_key = json_input.get("dataset_status_key")
        elif "dataset_conf" in json_input:
            dataset_id = json_input.get('dataset_conf', {}).get('dataset_id', "")
            dataset_status_key = ":".join([dataset_id, dataset_id])
        elif "specific" in json_input:
            dataset = get_dataset_keys(json_input=json_input, specific=specific)
            dataset_status_key = dataset.get('dataset_key')
        else:
            dataset_status_key = ""
    else:
        dataset = get_dataset_keys(json_input=json_input, specific=specific)
        dataset_status_key = dataset.get('dataset_key')
    return dataset_status_key


def generate_dataset_status_key(json_input: Optional[GenaiInput] = None) -> str:
    """ Generate the dataset status key

    :param json_input: Json_input of genai processes
    :return: (str) Dataset status key
    """
    assert json_input
    generic = get_generic(json_input)
    dataset_id = generic.get('dataset_conf', {}).get('dataset_id', "")

    if dataset_id:
        dataset_status_key = ":".join([dataset_id, dataset_id])
    else:
        process_type = generic.get('process_type', "ir_index")
        dataset_id = process_type + "_" + datetime.now().strftime("%Y%m%d_%H%M%S_%f_") + "".join([choice(string.ascii_lowercase + string.digits) for i in range(6)])
        dataset_status_key = ":".join([dataset_id, dataset_id])

    return dataset_status_key


def get_headers(json_input: Optional[GenaiInput] = None) -> Dict[str, str]:
    """ Get the headers

    :param json_input: Json_input of genai processes
    :return: (dict) Headers
    """
    assert json_input
    return json_input.get('headers', {})


def get_dataset_counter_key(json_input: Optional[GenaiInput] = None, specific: Optional[SpecificConfig] = None) -> str:
    """ Get the dataset status key

    :param json_input: Json_input of genai processes
    :param specific: (optional) If defined, specific configuration of genai processes
    :return: (str) Dataset counter key
    """
    assert json_input or specific
    dataset = get_dataset_keys(json_input=json_input, specific=specific)
    return dataset['dataset_counter_key']


def get_project_config(json_input: Optional[GenaiInput] = None, generic: Optional[GenericConfig] = None) -> ProjectConfig:
    """ Get config of project

    :param json_input: Json input of genai processes
    :param generic: (optional) If defined, generic configuration of genai processes
    :return: Config of project.
    """
    assert json_input or generic

    if json_input and not generic:
        generic = get_generic(json_input)

    return generic['project_conf']


def get_dataset_config(json_input: Optional[GenaiInput] = None, generic: Optional[GenericConfig] = None) -> DatasetGenericConfig:
    """ Get config of dataset

    :param json_input: Json input of genai processes
    :param generic: (optional) If defined, generic configuration of genai processes
    :return: Config of dataset.
    """
    assert json_input or generic

    if json_input and not generic:
        generic = get_generic(json_input)

    return generic['dataset_conf']


def get_dataset_id(json_input: Optional[GenaiInput] = None, generic: Optional[GenericConfig] = None) -> OriginsConfig:
    """ Get dataset_id for uhis controllers

    :param json_input: Json input of genai processes
    :param generic: (optional) If defined, generic configuration of genai processes
    :return: (dict) Origins conf
    """
    assert json_input or generic

    if json_input and not generic:
        generic = get_generic(json_input)

    return get_dataset_config(generic)['dataset_id']


def get_ocr_config(json_input: Optional[GenaiInput] = None, generic: Optional[GenericConfig] = None) -> OcrConfig:
    """ Get config of ocr

    :param json_input: Json input of genai processes
    :param generic: (optional) If defined, generic configuration of genai processes
    :return: Config of dataset.
    """
    assert json_input or generic

    if json_input and not generic:
        generic = get_generic(json_input)

    return generic['preprocess_conf']['ocr_conf']


def get_project_type(json_input: Optional[GenaiInput] = None, generic: Optional[GenericConfig] = None) -> str:
    """ Get type of project

    :param json_input: Json input of genai processes
    :param generic: (optional) If defined, generic configuration of genai processes
    :return: Type of project. One of: text, hybrid, vision.
    """
    assert json_input or generic

    if json_input and not generic:
        generic = get_generic(json_input)

    return generic['project_conf']['project_type']


def get_force_ocr(json_input: Optional[GenaiInput] = None, generic: Optional[GenericConfig] = None) -> str:
    """ Get force_ocr

    :param json_input: Json input of genai processes
    :param generic: (optional) If defined, generic configuration of genai processes
    :return: bool - True if the OCR must be forced.
    """
    assert json_input or generic

    if json_input and not generic:
        generic = get_generic(json_input)

    return generic['preprocess_conf']['ocr_conf'].get('force_ocr', False)


def get_languages(json_input: Optional[GenaiInput] = None, generic: Optional[GenericConfig] = None) -> List[str]:
    """ Get languages of the process

    :param json_input: Json input of genai processes
    :param generic: (optional) If defined, generic configuration of genai processes
    :return: List[str] - List of languages allowed.
    """
    assert json_input or generic

    if json_input and not generic:
        generic = get_generic(json_input)

    return generic['project_conf'].get('languages', ["*"])


def get_train_conf(json_input: Optional[GenaiInput] = None, generic: Optional[GenericConfig] = None) -> TrainConfig:
    """ Get Training config

    :param json_input: Json input of genai processes
    :param generic: (optional) If defined, generic configuration of genai processes
    :return: (TrainConfig) Training config of genai
    """
    assert json_input or generic

    if json_input and not generic:
        generic = get_generic(json_input)

    return generic['train_conf']


def get_models_config(json_input: Optional[GenaiInput] = None, generic: Optional[GenericConfig] = None, train_conf: Optional[TrainConfig] = None) -> List[GenaiModelConfig]:
    """ Get Models config

    :param json_input: (optional) Json input of genai processes
    :param generic: (optional) If defined, generic configuration of genai processes
    :param train_conf: (optional) Training configuration of genai process
    :return: (List[GenaiModelConfig]) List of models to train
    """

    assert json_input or generic or train_conf

    if json_input and not generic and not train_conf:
        train_conf = get_train_conf(json_input=json_input)
    elif generic and not train_conf:
        train_conf = get_train_conf(generic=generic)

    return train_conf['models']


def select_model(json_input: GenaiInput) -> Tuple[GenaiModelConfig, str]:
    """ Get model params to train

    :param json_input: Json input of queue config
    :return model:  Dict model params.
    :raise ValueError: If model_id in specific is not in ['train_conf']['models']
    """
    specific = get_specific(json_input)
    model_id = specific.get("model", {}).get("model_id", None)
    model_language = specific.get("model", {}).get("model_language", "*")
    if model_id is not None:
        models = get_models_config(json_input=json_input)
        for model in models:
            if model.get("model_id", "") == str(model_id):
                return model, model_language

    raise ValueError(f"{model_id} is not in config.")


def get_model_parameters(json_input: GenaiInput):
    """ Get model params from queue config

    :param json_input: Json input of queue config
    :return (string, dict, dict)  model type:          "SklearnModel", "...."
                                  parameters_pretext:  { "test_size": 0.2, "stratify": True }
                                  dic_model:            Model configuration
    """
    generic = get_generic(json_input)

    model_params, model_language = select_model(json_input)
    model_type = model_params.get('model_type')
    parameters_pretext = generic['preprocess_conf']['parameters_pretext']
    model_conf = model_params["hyperparams"]
    model_params = dict(preprocess=parameters_pretext, model_conf=model_conf)

    return model_type, model_language, model_params


def get_index_conf(json_input: Optional[GenaiInput] = None, generic: Optional[GenericConfig] = None) -> dict:
    """ Get index config

    :param json_input: Json input of genai processes
    :param generic: (optional) If defined, generic configuration of genai processes
    :return: Index Config
    """
    assert json_input or generic

    if json_input and not generic:
        generic = get_generic(json_input)

    return generic['indexation_conf']

def get_metadata_conf(json_input: Optional[GenaiInput] = None) -> dict:
    """ Get metadata indexing
    
    :param json_input: Json input of genai processes
    :return: Metadata values
    """
    assert json_input
    
    generic = get_generic(json_input)
    document = get_document(json_input)
    
    if generic.get('project_conf', {}).get('process_type') == "preprocess":
        return document.get('metadata', {})
    else:
        return document.get('metadata', {f"metadata_{i}": "" for i in range(get_index_conf(json_input).get('n_metadata', 0))})

def get_compose_conf(json_input: Optional[GenaiInput] = None, generic: Optional[GenericConfig] = None) -> dict:
    """ Get compose config

    :param json_input: Json input of genai processes
    :param generic: (optional) If defined, generic configuration of genai processes
    :return: Index Config
    """
    assert json_input or generic

    if json_input and not generic:
        generic = get_generic(json_input)

    return generic['compose_conf']


def get_elastic_params(json_input: Optional[GenaiInput] = None, generic: Optional[GenericConfig] = None) -> dict:
    """ Get elastic_params

    :param json_input: Json input of genai processes
    :param generic: (optional) If defined, generic configuration of genai processes
    :return: elastic_params
    """
    assert json_input or generic

    if json_input and not generic:
        generic = get_generic(json_input)

    return generic['elastic_params']


def get_layout_conf(json_input: Optional[GenaiInput] = None, generic: Optional[GenericConfig] = None) -> dict:
    """ Get layout configuration for preprocess

    :param json_input: Json input of genai processes
    :param generic: (optional) If defined, generic configuration of genai processes
    :return: dict
    """
    assert json_input or generic

    if json_input and not generic:
        generic = get_generic(json_input)

    return generic['preprocess_conf'].get('layout_conf', {})


def get_do_cells_text(json_input: Optional[GenaiInput] = None, generic: Optional[GenericConfig] = None) -> bool:
    """ Get param to extract boxes in text

    :param json_input: Json input of genai processes
    :param generic: (optional) If defined, generic configuration of genai processes
    :return: True or False
    """
    assert json_input or generic

    if json_input and not generic:
        generic = get_generic(json_input)

    return generic['preprocess_conf'].get('do_cells_text', True)


def get_do_lines_text(json_input: Optional[GenaiInput] = None, generic: Optional[GenericConfig] = None) -> bool:
    """ Get param to extract boxes in text

    :param json_input: Json input of genai processes
    :param generic: (optional) If defined, generic configuration of genai processes
    :return: True or False
    """
    assert json_input or generic

    layout_conf = get_layout_conf(json_input) if json_input and not generic else get_layout_conf(generic=generic)

    return layout_conf.get('do_lines_text', False)


def get_do_cells_ocr(json_input: Optional[GenaiInput] = None, generic: Optional[GenericConfig] = None) -> bool:
    """ Get param to extract boxes in images

    :param json_input: Json input of genai processes
    :param generic: (optional) If defined, generic configuration of genai processes
    :return: True or False
    """
    assert json_input or generic

    if json_input and not generic:
        generic = get_generic(json_input)

    return generic['preprocess_conf'].get('do_cells_ocr', True)


def get_do_lines_ocr(json_input: Optional[GenaiInput] = None, generic: Optional[GenericConfig] = None) -> bool:
    """ Get param to extract boxes in images

    :param json_input: Json input of genai processes
    :param generic: (optional) If defined, generic configuration of genai processes
    :return: True or False
    """
    assert json_input or generic

    layout_conf = get_layout_conf(json_input) if json_input and not generic else get_layout_conf(generic=generic)

    return layout_conf.get('do_lines_ocr', False)


def get_do_tables(json_input: Optional[GenaiInput] = None, generic: Optional[GenericConfig] = None) -> bool:
    """ Get param to process tables of document

    :param json_input: Json input of genai processes
    :param generic: (optional) If defined, generic configuration of genai processes
    :return: True or False
    """
    assert json_input or generic

    layout_conf = get_layout_conf(json_input) if json_input and not generic else get_layout_conf(generic=generic)

    return layout_conf.get('do_tables', False)


def get_do_titles(json_input: Optional[GenaiInput] = None, generic: Optional[GenericConfig] = None) -> bool:
    """ Get param to detect section titles of document

    :param json_input: Json input of genai processes
    :param generic: (optional) If defined, generic configuration of genai processes
    :return: True or False
    """
    assert json_input or generic

    layout_conf = get_layout_conf(json_input) if json_input and not generic else get_layout_conf(generic=generic)

    return layout_conf.get('do_titles', False)


def get_prediction_multilabel(json_input: Optional[GenaiInput] = None, generic: Optional[GenericConfig] = None) -> dict:
    """ Get params to prediction multilabel

    :param json_input: Json input of genai processes
    :param generic: (optional) If defined, generic configuration of genai processes
    :return: Params of prediction multilabel
    """
    assert json_input or generic

    if json_input and not generic:
        generic = get_generic(json_input)

    return generic['predict_conf']


def get_do_lines_conf(json_input: Optional[GenaiInput] = None, generic: Optional[GenericConfig] = None) -> dict:
    """ Get param to extract lines of process

    :param json_input: Json input of genai processes
    :param generic: (optional) If defined, generic configuration of genai processes
    :return: Configuration of lines
    """
    assert json_input or generic

    layout_conf = get_layout_conf(json_input) if json_input and not generic else get_layout_conf(generic=generic)

    return layout_conf.get('lines_conf', {})


def get_tables_conf(json_input: Optional[GenaiInput] = None, generic: Optional[GenericConfig] = None) -> dict:
    """ Get configuration for tables processing

    :param json_input: Json input of genai processes
    :param generic: (optional) If defined, generic configuration of genai processes
    :return: Configuration of tables
    """
    assert json_input or generic

    layout_conf = get_layout_conf(json_input) if json_input and not generic else get_layout_conf(generic=generic)

    return layout_conf.get('tables_conf', {})


def get_segmentation_conf(json_input: Optional[GenaiInput] = None, generic: Optional[GenericConfig] = None) -> dict:
    """ Get config for document segmentation

    :param json_input: Json input of genai processes
    :param generic: (optional) If defined, generic configuration of genai processes
    :return: Dict with configuration for documents segmentation
    """
    assert json_input or generic

    if json_input and not generic:
        generic = get_generic(json_input)

    return generic['preprocess_conf'].get('segmentation_conf', {})


def get_do_segments(json_input: Optional[GenaiInput] = None, generic: Optional[GenericConfig] = None) -> bool:
    """ Get param to segment documents

    :param json_input: Json input of genai processes
    :param generic: (optional) If defined, generic configuration of genai processes
    :return: True or False
    """
    assert json_input or generic

    segmentation_conf = get_segmentation_conf(json_input) if json_input and not generic else get_segmentation_conf(generic=generic)

    return segmentation_conf.get('do_segments', False)


def get_segmenters(json_input: Optional[GenaiInput] = None, generic: Optional[GenericConfig] = None) -> list:
    """ Get param to select document segmenter

    :param json_input: Json input of genai processes
    :param generic: (optional) If defined, generic configuration of genai processes
    :return: list of desired segmenters
    """
    assert json_input or generic

    segmentation_conf = get_segmentation_conf(json_input) if json_input and not generic else get_segmentation_conf(generic=generic)

    return segmentation_conf.get('segmenters', [])
