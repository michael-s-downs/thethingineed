# Native imports
import json, os
from typing import Literal, Optional, Union, Tuple, List
import requests

# Installed imports
from pydantic import BaseModel, field_validator, PositiveInt, FieldValidationInfo, model_validator, Field, confloat

# Local imports
from generatives import GenerativeModel
from common.logging_handler import LoggerHandler
from common.genai_controllers import storage_containers, load_file, upload_object

# Create logger
logger_handler = LoggerHandler("io_parsing", level=os.environ.get('LOG_LEVEL', "INFO"))
logger = logger_handler.logger
QUEUE_MODE = eval(os.getenv('QUEUE_MODE', "False"))

#############################################################################################
####################################### INPUT PARSING #######################################
#############################################################################################

class UrlImage(BaseModel):
    url: str
    detail: Optional[Literal['high', 'low', 'auto']] = None

    class Config:
        extra = 'forbid' # To not allow extra fields in the object


class Base64Image(BaseModel):
    base64: str
    detail: Optional[Literal['high', 'low', 'auto']] = None

    class Config:
        extra = 'forbid' # To not allow extra fields in the object


class MultimodalObject(BaseModel):
    type: Literal['text', 'image_url', 'image_b64']
    text: Optional[str] = None
    image: Optional[dict] = None
    n_tokens: Optional[PositiveInt] = None

    class Config:
        extra = 'forbid' # To not allow extra fields in the object

    @field_validator('image')
    def validate_image(cls, v, values: FieldValidationInfo):
        image_type = values.data.get('type')
        if image_type == "image_url":
            UrlImage(**v)
        elif image_type == "image_b64":
            Base64Image(**v)
        else:
            raise ValueError("'image' must be for 'image_url' or 'image_b64' types")
        return v

    @field_validator('text')
    def validate_text(cls, v, values: FieldValidationInfo):
        if values.data.get('type') == "text":
            return v
        else:
            raise ValueError("'text' parameter must be for 'text' type")


class Template(BaseModel):
    user: Union[str, list]
    system: str

    class Config:
        extra = 'forbid' # To not allow extra fields in the object

    @field_validator('user')
    def validate_user(cls, v):
        if isinstance(v, list):
            for element in v:
                if "$query" not in element:
                    MultimodalObject(**element)
            return v
        elif isinstance(v, str):
            return v
        else:
            raise ValueError("User must be a string or a list of strings")


class PersistenceElement(BaseModel):
    # Mandatory params passed by main (to generate propertly the platform)
    is_vision_model: bool

    role: Literal['user', 'assistant']
    content: Union[str, list]
    n_tokens: Optional[Union[PositiveInt, list]] = None

    class Config:
        extra = 'forbid' # To not allow extra fields in the object

    @field_validator('content')
    def validate_content(cls, v, values: FieldValidationInfo):
        role = values.data.get('role')
        is_vision = values.data.get('is_vision_model')

        if role == "assistant" and not isinstance(v, str):
            raise ValueError("'assistant' role must have string format for 'content'")
        else:
            if isinstance(v, str):
                return v
            # Must be a list because only str and list availables for content
            if not is_vision:
                raise ValueError("persistence content must be a string for non vision models")
            for el in v:
                if isinstance(el, dict):
                    MultimodalObject(**el)
                else:
                    raise ValueError("Elements of the content must be dict {} for vision models")
        return v


class QueryMetadata(BaseModel):
    # Mandatory params passed by main (to generate propertly the platform)
    is_vision_model: bool
    model_type: str

    # Query metadata
    query: Union[str, list]
    context: Optional[str] = None
    system: Optional[str] = None
    persistence: Optional[list] = None
    template_name: str
    template: dict

    class Config:
        extra = 'forbid'

    @field_validator('query')
    def validate_query(cls, v, values: FieldValidationInfo):
        is_vision = values.data.get('is_vision_model')
        if isinstance(v, str):
            return v
        # Must be a list because only str and list availables for query
        if not is_vision:
            raise ValueError("query must be a string for non vision models")
        for el in v:
            if isinstance(el, dict):
                MultimodalObject(**el)
            else:
                raise ValueError("Elements of the query must be dict {}")
        return v

    @field_validator('persistence')
    def validate_persistence(cls, v, values: FieldValidationInfo):
        is_vision = values.data.get('is_vision_model')
        for pair in v:
            if not isinstance(pair, list):
                raise ValueError("Persistence must be a list containing lists")
            if len(pair) != 2:
                raise ValueError("Content must contain pairs of ['user', 'assistant']")

            user = PersistenceElement(**{"is_vision_model": is_vision, **pair[0]})
            assistant = PersistenceElement(**{"is_vision_model": is_vision, **pair[1]})
            if user.role != 'user' or assistant.role != 'assistant':
                raise ValueError("In persistence, first role must be 'user' and second role must be 'assistant'")
        return v

    @field_validator('template')
    def validate_template(cls, v, values: FieldValidationInfo):
        query = values.data.get('query')
        is_vision_model = values.data.get('is_vision_model')
        if ((isinstance(query, str) and not isinstance(v.get('user'), str)) or
                (isinstance(query, list) and not isinstance(v.get('user'), list))):
            if is_vision_model:
                error_type = "In vision models must be a list"
            else:
                error_type = "In non vision models must be a string"
            raise ValueError(f"In the template '{v}' query does not match model query structure. {error_type}")
        Template(**v)
        return v

    @model_validator(mode='after')
    def validate_max_characters_dalle(self):
        if self.model_type == "dalle3":
            if len(self.query + str(self.persistence)) > 4000:
                raise ValueError("Error, in dalle3 the maximum number of characters in the prompt is 4000 (query + persistence is longer)")


class LLMMetadata(BaseModel):
    # Model metadata
    max_input_tokens: Optional[PositiveInt] = None
    temperature: Optional[confloat(ge=0.0, le=2.0)] = None
    max_tokens: Optional[PositiveInt] = None
    stop: Optional[list] = None
    functions: Optional[list] = None
    function_call: Optional[str] = None
    seed: Optional[PositiveInt] = None
    response_format: Literal['url', 'bs64_json', 'json_object'] = None
    quality: Literal['standard', 'hd'] = None
    size: Literal['1024x1024', '1792x1024', '1024x1792'] = None
    style: Literal['vivid', 'natural'] = None
    user: Optional[str] = None
    top_p: Optional[confloat(ge=0.0, le=1.0)] = None
    # Nova model parameter
    top_k: Optional[int] = Field(None, ge=0, le=500) # Optional int with range constraint
    model: Optional[str] = None
    default_model: Optional[str] = None
    tools: Optional[list] = None

    class Config:
        extra = 'forbid' # To not allow extra fields in the object

    @field_validator('tools')
    def validate_tools(cls, v, values: FieldValidationInfo):
        tool_names = set()
        for tool in v:
            if tool['name'] in tool_names:
                raise ValueError(f"Duplicate tool name '{tool.name}' found.")
            tool_names.add(tool['name'])
            Tool(**tool)
        return v
    @model_validator(mode='after')
    def validate_functions_and_functions_call(self):
        if self.functions and not self.function_call:
            raise ValueError("Internal error, function_call is mandatory because you put the functions param")
        elif self.function_call and not self.functions:
            raise ValueError("Internal error, functions is mandatory because you put the function_call param")

    @model_validator(mode='before')
    def validate_default_model(cls, values):
        if not values.get('model'):
            if not values.get('default_model'):
                raise ValueError("Internal error, default model not founded")
            values['model'] = values.get('default_model')
        values.pop('default_model', None)
        return values

class Tool(BaseModel):
    name: str = None
    description: str = None
    input_schema: dict = None

    class Config:
        extra = 'forbid'

    @field_validator('input_schema')
    def validate_input_schema(cls, v, values: FieldValidationInfo):
        Input_schema(**v)
        return v

class Input_schema(BaseModel):
    type: str = None
    properties: dict
    required: List[str] = None

    class Config:
        extra = 'forbid'

    @field_validator('properties')
    def validate_properties(cls, v, values:FieldValidationInfo):
        for key, prop in v.items():
            Property(**prop)
        return v

    @model_validator(mode='after')
    def validate_input_schema(cls, values):
        if not values.properties:
            raise ValueError("The 'properties' field is required and cannot be empty.")
        if not values.required:
            raise ValueError("The 'required' field is required and cannot be empty.")
        if not values.type:
            raise ValueError("The 'required' field is required and cannot be empty.")
        required_fields = set(values.required)
        defined_fields = set(values.properties.keys())
        missing_fields = required_fields - defined_fields
        if missing_fields:
            raise ValueError(f"The required fields {missing_fields} are missing in properties.")
        return values

class Property(BaseModel):
    type: str = None
    description: Optional[str] = None
    enum: Optional[List[object]] = None
    class Config:
        extra = 'forbid'

class PlatformMetadata(BaseModel):
    # Platform metadata
    timeout: Optional[PositiveInt] = None
    platform: Literal['azure', 'bedrock', 'openai']
    num_retries: Optional[PositiveInt] = None

    class Config:
        extra = 'forbid' # To not allow extra fields in the object


class ModelLimit(BaseModel):
    Current: int
    Limit: int

    class Config:
        extra = 'forbid' # To not allow extra fields in the object


class ProjectConf(BaseModel):
    # Mandatory params passed by main (to check the limits)
    platform: str
    model: GenerativeModel

    class Config:
        arbitrary_types_allowed = True  # Allow custom types (GenerativeModel)
        extra = 'forbid' # To not allow extra fields in the object
        allow_population_by_field_name = True # Allow match with alias

    # Project config metadata
    x_tenant: str = Field(alias="x-tenant")
    x_department: str = Field(alias="x-department")
    x_reporting: str = Field(alias="x-reporting")
    x_limits: dict = Field(alias="x-limits")

    @staticmethod
    def parse_x_limits_reporting(x_limits: str):
        x_limits = json.loads(x_limits)
        x_limits_formatted = {}
        for limit in x_limits['limits']:
            if limit['limit'] != 0:
                x_limits_formatted[limit['resource']] = {
                    "Current": limit['current'],
                    "Limit": limit['limit']
                }
        return x_limits_formatted


    @field_validator('x_limits')
    def validate_x_limits(cls, v, values: FieldValidationInfo):
        if not v and not eval(os.getenv('TESTING', "False")):
            url = values.data.get('x_reporting') + "/list"
            logger.debug(f"No x-limits found, proceeding to get them from reporting service: {url}")
            response = requests.get(url=url)
            if response.status_code != 200:
                raise ValueError(f"Error getting limits from reporting service: {response.reason}")
            v = cls.parse_x_limits_reporting(response.text)
        # check if input is correct
        for model, limits in v.items():
            if limits:
                ModelLimit(**limits)

        # Check limits for the model
        platform = values.data.get('platform')
        model = values.data.get('model')
        if model.MODEL_MESSAGE == "dalle":
            model_key = f'llmapi/{platform}/{model.model_type}/images'
        else:
            model_key = f'llmapi/{platform}/{model.model_type}/tokens'
        model_usage = v.get(model_key, {})
        if len(model_usage) > 0:
            count = model_usage.get('Current', 1)
            limit = model_usage.get('Limit', 0)
            if count >= limit:
                raise ValueError(f"Model '{model_key}' has reached the limit of: '{limit}'")
        return v


class QueueMetadata(BaseModel):
    # Queue metadata
    input_file: str
    output_file: str
    location_type: Literal['cloud', 'local']

    class Config:
        extra = 'forbid' # To not allow extra fields in the object
    
    @property
    def workspace(self) -> str:
        return storage_containers.get('workspace')

    @field_validator('input_file')
    def validate_input_file(cls, v, values: FieldValidationInfo):
        if not v.lower().endswith('.json'):
            raise ValueError("Input file must be a json file")
        return v
        
    @field_validator('output_file')
    def validate_output_file(cls, v, values: FieldValidationInfo):
        if not v.lower().endswith('.json'):
            raise ValueError("Output file must be a json file")
        return v

    def load_json_input(self) -> dict:
        """ Load json input from file or cloud (must be a json file as it's a llmapi request)
        
        :return: Json input
        """
        if self.location_type == "cloud":
            try:
                json_input = json.loads(load_file(self.workspace, self.input_file))
            except:
                raise ValueError(f"Unable to read json file '{self.input_file}' from {self.workspace}")
        else:
            try:
                with open(self.input_file, "r", encoding="utf-8") as file:
                    json_input = json.load(file)
            except:
                raise ValueError(f"Unable to read json file '{self.input_file}'")
        return json_input
    
    def upload_json_output(self, json_output: dict) -> None:
        """ Upload json output to file or cloud (must be a json file as it's a llmapi response)
        
        :param json_output: Json output
        """
        if self.location_type == "cloud":
            try:
                upload_object(self.workspace, json.dumps(json_output), self.output_file)
            except:
                raise ValueError(f"Unable to write json file '{self.output_file}' to {self.workspace} with content: {json_output}")
        else:
            try:
                with open(self.output_file, "w", encoding="utf-8") as file:
                    json.dump(json_output, file)
            except:
                raise ValueError(f"Unable to write json file '{self.output_file}' with content {json_output}")


def adapt_input_queue(json_input: dict) -> dict:
    """ Input adaptations for queue case

    :param json_input: Input data
    :return: Input data adapted
    """
    if not QUEUE_MODE:
        return json_input, None
    
    apigw_params = json_input.get('headers', {})
    queue_metadata = None

    if 'queue_metadata' in json_input:
        queue_metadata = QueueMetadata(**json_input.pop('queue_metadata'))
        json_input = queue_metadata.load_json_input()
    else:
        mount_path = os.getenv('DATA_MOUNT_PATH', "")
        mount_key = os.getenv('DATA_MOUNT_KEY', "")
        
        if not (mount_path and mount_key):
            if QUEUE_MODE:
                logger.warning("Mount path or mount key not found in environment variables")
            return json_input, queue_metadata

        file_path = json_input.setdefault('query_metadata', {}).get(mount_key, "")

        if mount_path not in file_path:
            logger.warning(f"Document path '{file_path}' not inside mounted path '{mount_path}'")
            return json_input, queue_metadata
        logger.debug(f"Getting document from mount path '{mount_path}'")
        if not os.path.exists(file_path):
            logger.warning(f"Document path not found '{file_path}'")
            return json_input, queue_metadata
        if not os.path.isfile(file_path):
            logger.warning(f"Document path must be a file '{file_path}'")
            return json_input, queue_metadata
        logger.debug(f"Getting document from path '{file_path}'")

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                file_content = f.read()
        except:
            file_content = ""
        if file_content:
            try:
                # Vision queries
                json_input['query_metadata'][mount_key] = json.loads(file_content)
            except Exception:
                # Non-vision queries
                json_input['query_metadata'][mount_key] = file_content
        else:
            logger.error(f"Unable to read file '{file_path}'")
    
    json_input["project_conf"] = apigw_params
    return json_input, queue_metadata

##############################################################################################
####################################### OUTPUT PARSING #######################################
##############################################################################################


class ResponseObject(BaseModel):
    status_code: int
    error_message: Optional[str] = None
    result: Optional[Union[str, dict]] = None
    status: Literal['finished', 'error']

    @field_validator('status')
    def validate_status(cls, v, values: FieldValidationInfo):
        status_code = values.data.get('status_code')
        if v == "error" and status_code == 200:
            raise ValueError("If status is 'error', status_code must be different from 200")
        if v == "finished" and status_code != 200:
            raise ValueError("If status is 'finished', status_code must be 200")
        return v

    def get_response_predict(self, queue_metadata: QueueMetadata) -> Tuple[bool, dict, str]:
        output, status_code = self.get_response_base()
        output = json.loads(output)
        if QUEUE_MODE:
            must_continue = True
            next_service = os.getenv('Q_GENAI_LLMQUEUE_OUTPUT')
        else:
            must_continue = False
            next_service = ""
            if status_code == 200:
                output = output.get('result', {})

        # Case when the output is written in a file
        if queue_metadata:
            queue_metadata.upload_json_output(output)            
            output['result'] = queue_metadata.output_file

        return must_continue, output, next_service

    def get_response_base(self) -> Tuple[str, int]:
        response = {
            'status': self.status,
            'status_code': self.status_code
        }
        if self.status_code == 200 and self.result:
            response['result'] = self.result
        elif self.status_code != 200 and self.error_message:
            response['error_message'] = self.error_message
        else:
            raise ValueError("Internal error, response object must have a result or an error_message")
        return json.dumps(response), self.status_code
