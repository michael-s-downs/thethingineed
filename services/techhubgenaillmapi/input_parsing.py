# Native imports
import json
from typing import Literal, Optional, Union

# Installed imports
from pydantic import BaseModel, field_validator, PositiveInt, FieldValidationInfo, model_validator

# Local imports
from generatives import GenerativeModel


class UrlImage(BaseModel):
    url: str
    detail: Optional[Literal['high', 'low', 'auto']] = None


class Base64Image(BaseModel):
    base64: str
    detail: Optional[Literal['high', 'low', 'auto']] = None


class MultimodalObject(BaseModel):
    type: Literal['text', 'image_url', 'image_b64']
    text: Optional[str] = None
    image: Optional[dict] = None
    n_tokens: Optional[PositiveInt] = None

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
    n_tokens: Optional[Union[str, list]] = None

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

    # Query metadata
    query: Union[str, list]
    context: Optional[str] = None
    system: Optional[str] = None
    persistence: Optional[list] = None
    template_name: str
    template: dict

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


class LLMMetadata(BaseModel):
    # Model metadata
    max_input_tokens: Optional[PositiveInt] = None
    temperature: Optional[PositiveInt] = None
    max_tokens: Optional[PositiveInt] = None
    stop: Optional[str] = None
    functions: Optional[list] = None
    function_call: Optional[str] = None
    seed: Optional[PositiveInt] = None
    response_format: Literal['url', 'bs64_json', 'json_object'] = None
    quality: Literal['standard', 'hd'] = None
    size: Literal['1024x1024', '1792x1024', '1024x1792'] = None
    style: Literal['vivid', 'natural'] = None
    user: Optional[str] = None
    model: str

    @model_validator(mode='after')
    def validate_functions_and_functions_call(self):
        if self.functions and not self.function_call:
            raise ValueError("Internal error, function_call is mandatory because you put the functions param")
        elif self.function_call and not self.functions:
            raise ValueError("Internal error, functions is mandatory because you put the functions param")


class PlatformMetadata(BaseModel):
    # Platform metadata
    timeout: Optional[PositiveInt] = None
    platform: Literal['azure', 'bedrock', 'openai']


class ModelLimit(BaseModel):
    Current: int
    Limit: int


class ProjectConf(BaseModel):
    # Mandatory params passed by main (to check the limits)
    platform: str
    model: GenerativeModel

    class Config:
        arbitrary_types_allowed = True  # Allow custom types (GenerativeModel)

    # Project config metadata
    x_tenant: str
    x_department: str
    x_reporting: str
    x_limits: dict

    @field_validator('x_limits')
    def validate_x_limits(cls, v, values: FieldValidationInfo):
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
