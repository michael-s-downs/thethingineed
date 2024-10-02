# GENAI LLM API

## Index

- [GENAI LLM API](#genai-llm-api)
  - [Index](#index)
  - [Overview](#overview)
    - [Key Features](#key-features)
  - [Get started](#get-started)
  - [Configuration](#configuration)
    - [Api json body parameters](#api-json-body-parameters)
    - [Query types](#query-types)
    - [Models](#models)
    - [Environment variables](#environment-variables)
  - [Examples](#examples)
    - [Azure GPT with max\_input tokens and persistence](#azure-gpt-with-max_input-tokens-and-persistence)
    - [Image generation with Dalle3](#image-generation-with-dalle3)
    - [Model vision](#model-vision)
  - [Deployment](#deployment)
  - [Documentation](#documentation)

## Overview

The GENAI LLM API is an advanced solution designed to seamlessly integrate with large language models (LLMs) for enhanced query processing and functionality. It leverages cutting-edge AI capabilities to deliver sophisticated queries, provide contextual information, and generate detailed responses efficiently. Whether deploying models on Azure or AWS, the GENAI LLM API offers a robust and flexible interface for interacting with state-of-the-art LLMs such as GPT-4o, Claude3, or DALL-E.

### Key Features

- Multi-platform Support: Seamlessly integrate with major cloud providers like Azure and AWS, ensuring scalability, reliability, and high availability for mission-critical applications.
- Comprehensive Query Handling: Efficiently process queries in various formats, including text and images, to deliver accurate and contextually relevant responses tailored to your business needs.
- Customizable Parameters: Fine-tune model behavior with adjustable parameters such as token limits, temperature, and response formatting to meet specific organizational requirements.
- Persistence and Context Management: Maintain conversation context and history to enable more coherent and context-aware interactions, ensuring continuity and improving user experience.
- Versatile Model Selection: Access a wide range of models across different platforms, optimized for various use cases and geographical regions to support global operations.
- Image Generation: Leverage DALL-E to generate high-quality images based on textual descriptions, with customizable options for style and resolution to fit diverse visual content needs.

### Component Architecture

The GENAI LLM API is an advanced solution designed to seamlessly integrate with large language models (LLMs) for enhanced query processing and functionality. It leverages cutting-edge AI capabilities to deliver sophisticated queries, provide contextual information, and generate detailed responses efficiently. Whether deploying models on Azure or potentially AWS, the GENAI LLM API offers a robust and flexible interface for interacting with state-of-the-art LLMs such as GPT-4o, or DALL-E.

This is a single component that uses the cognitive services of Azure OpenAI.

![alt text](https://satechhubproeastus001.blob.core.windows.net/workflows/llmapi-architecture.png "Process flow")

## Get started
The LLM API supports the following usage methods:
- Automatic deployment to the TechHub Sandbox environment
- Deployment to a Kubernetes environment with Azure pipeline (see Deployment section)
- Running the bare application in a Python runtime environment

### On the TechHub Sandbox
  - Pressing the "On Gao Sandbox" button at the top left of this page will automatically configure an individual environment for each user. After pressing the button, you will need to configure the settings for the Engine Configuration. Please overwrite with the following settings and press Save. (For the details about the settings, refer to the "Engine Configuration" section below)

To get started with GENAI LLM service on your local machine, you need to have a model deployed in azure or aws. In this first example we will be using Azure - gpt-3.5.

#### Sandbox Deployment restriction
  - The Sandbox environment will be automatically deleted 24 hours after deployment. Currently, early termination or extension is not supported.
  - Each user can configure only one Sandbox environment per resource at a time. It is not possible to deploy more than one simultaneously.
  - By default, access to the Sandbox environment is allowed only via UDS proxy under the assumption that it will be used from the UDC(NTT DATA Unified Development Cloud). If you wish to request additional access sources such as your environment, please apply through the support page. Currently, access from zscaler (Secure FAT/BXO), which does not have a specific source IP address, is not supported.

#### Sample Request for deployed sandbox
In this section, we will describe the procedure for using the LLM API deployed in the Sandbox environment. We will provide an example using Postman for the API requests.

Example of a request to the LLM Service:
~~~
import requests
import json
url = "$URL/llm/predict"
payload = {
   "query_metadata":{
      "system":"You are a chatbot assistant.",
      "query":"What is the France capital?",
      "template_name":"system_query"
   },
   "llm_metadata":{
      "model":"gpt-3.5-16k-pool-techhub-japan",
      "temperature":0
   },
   "platform_metadata":{
      "platform":"azure"
   }
}
headers = {
  "Content-type": "application/json",
  "x-api-key": "$APIKEY"
}
response = requests.request("POST", url, headers=headers, json=payload)
~~~
Response from the LLM service:
~~~
{''status'': ''finished'',
''result'': {''answer'': ''The capital of France is Paris.'',
  ''logprobs'': [],
  ''n_tokens'': 33,
  ''query_tokens'': 6,
  ''input_tokens'': 26,
  ''output_tokens'': 7},
''status_code'': 200}
~~~

In this case, we are using the LLM template_name "system_query", which is the following:

```json
{
    "system_query": {
        "system": "$system",
        "user": "$query"
    }
}
```

#### Sample Request for deployed sandbox
In this section, we will describe the procedure for using the LLM API deployed in the Sandbox environment. We will provide an example using Postman for the API requests.

Example of a request to the LLM Service:
~~~
import requests
import json
url = "$URL/llm/predict"
payload = {
   "query_metadata":{
      "system":"You are a chatbot assistant.",
      "query":"What is the France capital?",
      "template_name":"system_query"
   },
   "llm_metadata":{
      "model":"gpt-3.5-16k-pool-techhub-japan",
      "temperature":0
   },
   "platform_metadata":{
      "platform":"azure"
   }
}
headers = {
  "Content-type": "application/json",
  "x-api-key": "$APIKEY"
}
response = requests.request("POST", url, headers=headers, json=payload)
~~~
Response from the LLM service:
~~~
{''status'': ''finished'',
''result'': {''answer'': ''The capital of France is Paris.'',
  ''logprobs'': [],
  ''n_tokens'': 33,
  ''query_tokens'': 6,
  ''input_tokens'': 26,
  ''output_tokens'': 7},
''status_code'': 200}
~~~

In this case, we are using the LLM template_name "system_query", which is the following:

```json
{
    "system_query": {
        "system": "$system",
        "user": "$query"
    }
}
```

## Api specification

- **query_metadata** (required):
  - **query** (required): Question or task that you want to ask the model (now can be messages to read or images to analyze passed on a list in a new format). For a more detailed information: [Query types](#query-types)
  - **context** (optional): Context on which to base the question. By default, the model marks the field as empty.
  - **system** (optional): Variable for chat-based models. By default “You are a helpful assistant” is set.
  - **template_name** (optional): Name of the template to use. By default “system_query” template is selected.
  - **template** (optional): Template that will be used. Must be an string with dict format
  - **persistence** (optional): **_Automatically generated with RAG toolkit._** If a personalized version is required, see persistence format section in [documentation](documentation_llm.md)


- **llm_metadata** (required):
  - **model** (optional): Name of the model to be used on each platform, if not indicated “gpt-3.5-pool-europe” is used by default. These are the models available on each platform and their corresponding tokens limit.
  - **max_input_tokens** (optional): Maximum number of tokens to be sent in the request. If the maximum size is exceeded, it will be cut from the context, leaving space for the model response.
  - **max_tokens** (optional): Maximum number of tokens to generate in the response.
  - **temperature** (optional): Temperature to use. Value between 0 and 2 (in Bedrock 0-1). Higher values like 0.8 make the output more random. Lower values like 0.2 make it more deterministic. By default 0.
  - **stop** (optional): Up to 4 strings where the API will stop generating more tokens.
  - **functions (*Warning!*):** Deprecated by OpenAI but still working. List of functions the model may generate JSON inputs for. Only in OpenAI and Azure.
  - **function_call (*Warning!*):**  Deprecated by OpenAI but still working. Required if functions is sent. Possible values: “auto”, “none”, or {"name": "my_function"}. For full information: <https://platform.openai.com/docs/api-reference/chat/create#chat-create-function_call>
  - **seed**: Only in GPT models, used to replicate the same output from the model (not always the same). This param is in beta **_(only in azure platform)_**.
  - **response_format** (optional): The values available to manage the output format of the image generation models are [url, bs64_json] and for text generation models (only avaliable in selected ones by Azure OpenAI) is [json_object].
  - For image generation:
  
    - **quality** (optional): Quality of the output image [“standard”, “hd”] default as standard.
    - **size** (optional): Output size format [“1024x1024”, “1792x1024”, “1024x1792”] default as “1024x1024”.
    - **style** (optional): Output style of the image [vivid, natural], default as vivid.

- **platform_metadata** (required):
  - **platform** (required): Name of the desired platform. Possible values: “azure”, “openai”, or “bedrock”.
  - **timeout** (optional): Maximum time to response. By default is 30s if this value is not passed. 

## Endpoints

- /predict (POST): This is the main endpoint, used to call the LLM.
- /reloadconfig (GET): Used to reload the configuration readed from the files like the models and prompt templates availables. Returns the following json:

```json
{
    "status": "ok",
    "status_code": 200
}
```

- /healthcheck (GET): Used to check if the component is available. Returns:

```json
{
    "status": "Service available"
}
```

- /get_models (GET): Used to get the list with the available models. In the url we can send the model_type, pool, platform or zone. An example with platform could be: https://techhubapigw.app.techhubnttdata.com/llm/get_models?platform=azure


Response:

```json
{
    "models": {
        "azure": [
            "genai-gpt4o-EastUs",
            "genai-gpt35-4k-france",
            "genai-gpt35-16k-france",
            "genai-gpt4-32k-france",
            "genai-gpt4-8k-france",
            "genai-gpt4o-Sweden",
            "genai-gpt35-16k-sweden",
            "genai-gpt35-4k-sweden",
            "genai-gpt4-32k-sweden",
            "genai-gpt4-8k-sweden",
            "genai-gpt35-4k-westeurope"
        ],
        "pools": [
            "gpt-3.5-pool-america",
            "gpt-4-pool-ew-europe",
            "gpt-3.5-16k-pool-europe",
            "gpt-4-pool-europe",
            "gpt-4o-pool-world",
            "gpt-3.5-16k-pool-uk",
            "gpt-4-32k-pool-ew-europe"
        ]
    }
}
```

- /upload_prompt_template (POST): Used to upload a prompt template json file to the cloud storage the content value must be a json converted to string.

```json
{
  "name": "example_filter_template",
  "content": "{\r\n    \"emptysystem_query\": {\r\n        \"system\": \"\",\r\n        \"user\": \"$query\"\r\n    },\r\n    \"system_query\": {\r\n        \"system\": \"$system\",\r\n        \"user\": \"$query\"\r\n    },\r\n    \"system_context\": {\r\n        \"system\": \"$system\",\r\n        \"user\": \"$context\"\r\n    },\r\n    \"fixed_system_query\": {\r\n        \"system\": \"You are a football player\",\r\n        \"user\": \"$query\"\r\n    }\r\n}"
}
```

- /delete_prompt_template (POST): Used to delete a prompt template json file from cloud storage.

```json
{
  "name": "example_template"
}
```

- /list_templates (GET): Used to get all the available templates
```json
{
    "templates": [
        "emptysystem_query",
        "system_query",
        "system_context",
        "fixed_system_query",
        "system_query_and_context",
        "system_query_and_context_plus",
        "system_query_and_context_summary",
        "system_query_and_context_plus_v1",
        "system_query_and_context_plus_v0",
        "query_and_context_es",
        "system_query_and_botcontext_es"
    ]
}
```

- /get_template (GET): Used to get how is a template/prompt: https://techhubapigw.app.techhubnttdata.com/llm/get_template?template_name=system_query_and_context_summary

```json
{
    "template": {
        "system": "$system",
        "user": "Answer the following task based on the following 'context' or the history of the conversation.'. \nTask: '$query' \n\n######\n\nContext:\n$context \n\n######\n\nAnswer:"
    },
}
```


## Configuration
### Query types

Non-vision query:

```json
"query": "How old is OpenAI?"
```

Vision query:

```json
"query": [
   {
     "type": "text",
     "text": "How old is OpenAI?"
   }
]
```

- Type (mandatory): The type of query we are going to send. This can be just text or an image in format url or base64. The options for this key are: text/image_url/image_b64
  - Text: If the type is “text” this key is mandatory and it contains a string with the question/text to send to the llm.
  - Url: If the type is “image_url” this key is mandatory, and it contains a string with the url to the image.
  - Base64: If the type is “image_b64” this key is mandatory and it contains a base64 string encoding a image.
- Text (mandatory when type = text): The text to send to the llm.
- Image (mandatory when type = image_url or image_b64): Dictionary with the image content ('url' for image_url and 'base64' for image_b64) and another optional parameters:
  - detail (optional for gpt vision models): The quality of the image analysis. Possible values: 'low', 'high' and 'auto'. Default is 'auto'.

The following would be an example of a query containing the three types:

```json
"query": [
   {
     "type": "text",
     "text": "How old is OpenAI?"
   },
   {
     "type": "image_url",
     "image": {
        "url": "https://imagelink.jpg"
     }
   },
   {
     "type": "image_b64",
     "image": {
        "base64": "base64stringencodingimage"
     } 
  }
]
```

*Images formats allowed: jpeg, png, gif and webp*

### Models

The availabe models with an example pool are:

| Model | Pools       | Platform |
|-------|-------------|----------|
| GPT 3.5 |  gpt-3.5-pool-techhub-europe | azure/openai|
| GPT 3.5 16k |  gpt-3.5-16k-pool-techhub-europe | azure/openai|
| GPT 4 |  gpt-4-pool-techhub-world | azure/openai|
| GPT 4 32k |  gpt-4-32k-pool-techhub-world | azure/openai|
| GPT 4 o |  gpt-4o-pool-techhub-world | azure/openai|
| Dalle 3 |  gpt-dalle3-pool-techhub-europe | azure/openai|
| Claude 1 |  claude-instant-v1-pool-europe | bedrock|
| Claude 2 |  claude-v2-pool-world | bedrock|
| Claude 2.1 |  claude-v2.1-pool-america | bedrock|
| Claude 3 haiku |  claude-v3-haiku-pool-europe | bedrock|
| Claude 3 sonnet |  claude-v3-sonnet-pool-europe | bedrock|

*A pool of models is a group of the same models allocated in different servers from a specific region, such as Europe or the US, that allows a more balanced deployment of models.*

### Environment variables

```json
"PROVIDER": "azure/aws",
"STORAGE_BACKEND": "tenant-backend",
"AZ_CONN_STR_STORAGE": "", // if not secrets
"SECRETS_PATH": "path to secrets",
"AWS_ACCESS_KEY": "", // if not secrets
"AWS_SECRET_KEY": "" // if not secrets
```



## Examples

### Azure GPT with max_input tokens and persistence

In this example we are using the model gpt 3.5 turbo from azure with a max input tokens value of 1000 tokens and setting the persistence with an old conversation.

```json
{
    "query_metadata": {
        "query": "Where am i moving to?",
        "persistence": [
            [
                {"role": "user", "content": "I am moving to Zaragoza"},
                {"role": "assistant", "content": "When moving to Zaragoza, a good place to start would be the city center. This area is known for its historic buildings, lively atmosphere, and many attractions. You can visit the iconic Basilica del Pilar, explore the Aljafería Palace, or take a stroll along the Ebro River. Additionally, the Plaza del Pilar is a popular meeting point and a great spot to begin your exploration of the city."}
            ]
        ]
    },
    "llm_metadata": {
        "max_input_tokens": 1000,
        "model": "gpt-3.5-pool-techhub-europe"
    },
    "platform_metadata": {
        "platform":"azure"
    }
}
```

Response:

```json
{
    "status": "finished",
    "result": {
        "answer": "You mentioned that you are moving to Zaragoza. Zaragoza is a city located in northeastern Spain, in the region of Aragon. It is the fifth-largest city in Spain and is known for its rich history, cultural heritage, and vibrant atmosphere. Zaragoza is home to many historical landmarks, such as the Basilica del Pilar and the Aljafería Palace, as well as modern attractions like the Expo area and the Ebro Riverfront.",
        "logprobs": [],
        "n_tokens": 218,
        "query_tokens": 6,
        "input_tokens": 124,
        "output_tokens": 94
    },
    "status_code": 200
}
```

### Image generation with Dalle3

In this example we are using Dalle3 to generate an image of a motorbike and the result will be an url to the image.

```json
{
 "query_metadata": {
        "query": "An orange motorbike"
 },
 "llm_metadata": {
        "model": "gpt-dalle3-pool-techhub-europe",
        "response_format": "url"
 },
 "platform_metadata": {
        "platform":"azure",
        "timeout":30
    }
}
```

Response:

```json
{
    "status": "finished",
    "result": {
        "answer": "**link to the generated image",
        "logprobs": [],
        "n_tokens": 0,
        "input_tokens": 4,
        "query_tokens": 4,
        "output_tokens": 0,
        "n": 1
    },
    "status_code": 200
}
```

Generated image:

![alt text](imgs/generated_00.png "Image generated")

### Model vision

In this example, we send a text type query and an image query type to the model Claude3 Haiku hosted in AWS Bedrock.

```json
{
 "query_metadata": {
  "query": [
            {
                "type": "text",
                "text": "Whan can you see in the image?"
            },
            {
                "type": "image_url",
                "image":{
                    "url": "https://cdn.britannica.com/16/75616-050-14C369D3/dolphins-mammals-fish-water.jpg"
                }
      }
        ]
 },
 "llm_metadata": {
  "max_input_tokens": 10000,
  "max_tokens": 3000,
  "model": "gpt-4o-pool-techhub-world"
 },
 "platform_metadata": {
     "platform": "azure"
 }
}
```

Response:

```json
{
    "status": "finished",
    "result": {
        "answer": "The image shows two dolphins swimming in the ocean. The dolphins are leaping out of the water, creating splashes and sprays around them. The water appears to be a deep blue color, and the sunlight is reflecting off the waves, creating a sparkling, shimmering effect. The dolphins appear to be moving quickly and gracefully through the water.",
        "n_tokens": 1639,
        "query_tokens": [8, 2621],
        "output_tokens": 76,
        "input_tokens": 1563
    },
    "status_code": 200
}
```

## Deployment

### Files overview

Each component has the following files and folders structure:

- **Helm**: This folder contains the templates of helm to build the charts for deploy microservices.

- **IaC**: Infrastructure as Code. This folders contains all the required files to set and deploy the services like cloud storage, queues, etc with Terraform. Besides this folder contain scripts to create, copy or other utilities to prepare the deployment of the service. Finally, it contain an azure-pipeline.yml to create ''Artifact'' in Azure Devops that will later be used in the release responsible for generating all the resources.

- **imagesbase**: Contains the base images required for the component, used to accelerate creation of images dockers of microservices. This folder contain azure-pipeline.yml to create the pipeline that it create and upload imagen docker to ACR with different tags. This images download and install the libraries generated in the folder **libraries**.

- **libraries**: Contains the specific libraries and SDKs for the component and its azure-pipeline.yml. This library is saved within Azure Devops artifact feed. 

- **services**: Contains the component code files.

### Requirements

- Azure suscription
- Cluster Kubernetes
- Globals Resources

### Resources Azure Devops

#### 1. Variable groups (Library)

All azure-pipeline.yml of the **imagesbase** and **libraries** contains variables that these variables are set by groups of variables, in Azure Devops this groups are set in Library menu.

This way, the azure-pipeline.yml does not have to be modified, in case or these variables need to modified, modify only the values of variables within group variables of Library if you must update something param.

#### 2. Pipelines

This section is used to generate both the docker images and upload them to the corresponding acr as well as to package and generate Artifact, which will later be necessary to upload in the deployment releases.


#### 3. Releases

This section is used to create two releases, one for IaC and another for service deployment. 

This will be the final step with which everything necessary to have the component, toolkit or solution available is created or deployed.

### Stages to deploy

#### 1. Create library

Microservices GenAI need to connect with different cloud resources. To connect to these resources they use a library that implements the connectors.

So you have to compile the library and save it in a feed. You have to create the pipeline of azure within the folder **libraries** and run in a branch master to compile and stored.

Before you have to create the *Library* necesary with the correct values. You can check the name of library and values in yml.

#### 2. Create images base

Microservices GenAI are created about imagenes base. This saves time when generating the final docker image of the microservice.

You have to create the pipeline with its corresponding yml within of folder **imagesbase**.

Before you have to create the *Library* necesary with the correct values. You can check the name of library and values in yml.

#### 3. Create image microservice and artifact helm

Each microservices GenAI have own docker image.

To create image, in the folder **services** there is azure-pipeline.yml with configuration of pipeline to create and storage this image.

Before you have to create the *Library* necesary with the correct values.

This pipeline create docker image and artifact with helm code to deploy in the release.

#### 4. Create artifact IaC

Each microservices GenAI have own IaC code in Terraform and Scripts.

To create artifact, in the folder **IaC** there is azure-pipeline.yml with configuration of pipeline to create this artifact that later you need to create release.

Before you have to create the *Library* necesary with the correct values.

#### 5. Create releases

Finally, to create cloud resources and deploy services you have to create two releases. These releases can have one or more stages.

1. Release IaC: This release is in charge of creating cloud and kubernetes resources. In IaC release you have two stages:

   - Terraform: Create all resources cloud in Azure and finally it create namespace and secrets of kubernetes recover of creation resources cloud.
   - Scripts: Copy configuration files, if this resource need load files of configuration, of the one storage of administration.

2. Release Service: This release is in charge of creating the services by docker images and deploy in cluster as microservices. This steep create all resources necesary, this resources can to be elasticsearch, microservices genAI, Keda objects...

The configuration of these releases ara attached to the release folder within of repository.

### Running the bare application in a Python runtime environment

To get started with GENAI LLM service on your local machine, you need to have a model deployed in Azure or AWS. In this first example we will be using Azure - gpt-3.5.

Set the [environment variables](#environment-variables).

Create an Azure or AWS cloud storage and upload to the path "src/LLM/prompts" the json file with the system/user template. We are using the following file:

```json
{
    "system_query": {
        "system": "$system",
        "user": "$query"
    }
}
```

### Environment variables

```json
"PROVIDER": "azure",
"STORAGE_BACKEND": "tenant-backend",
"AZ_CONN_STR_STORAGE": "",
"SECRETS_PATH": "path to secrets",
"AWS_ACCESS_KEY": "",
"AWS_SECRET_KEY": ""
```

#### Python environment

Create a Python 3.8 environment and install the required libraries from requirements.

```sh
pip install -r requirements_info.txt
```


#### Launch services

Execute the main.py file:

```sh
python main.py"
```

#### Call services


Call the /predict api endpoint with the following body and headers:

```json
"body": {
    "query_metadata": {
        "query": "Where is Paris?",
    },
    "llm_metadata":{
        "model": "gpt-3.5-16k-pool-techhub-japan"
    },
    "platform_metadata":{
         "platform": "azure"
    }
}

"headers": {
    "x-tenant": "",
    "x-department": "",
    "x-reporting": ""
}
```

If you are calling the pod use:

```json
"x-api-key": "apikey123example"
```

Example using python requests:

```python
import requests
import json

url = "http://localhost:8888/predict"

payload = {
    "query_metadata": {
        "query": "Where is Paris?",
    },
    "llm_metadata":{
        "model": "gpt-3.5-16k-pool-techhub-japan"
    },
    "platform_metadata":{
         "platform": "azure"
    }
}

headers = {
    ''x-tenant'': ''develop'',
    ''x-department'': ''main'',
    ''x-reporting'': '''',
    ''Content-Type'': ''application/json''
}

response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)
```

If the response looks like this, you are good to go.

```json
{
    "status": "finished",
    "result": {
        "answer": "Paris is the city and capital of France, situated in the north-central part of the country.",
        "logprobs": [],
        "n_tokens": 50,
        "query_tokens": 5,
        "input_tokens": 21,
        "output_tokens": 29
    },
    "status_code": 200}
```
## Documentation

Link to full documentation comming soon.

## Process Flow
![alt text](https://satechhubproeastus001.blob.core.windows.net/workflows/genai-llmapi-decision-flow.png "Process flow")