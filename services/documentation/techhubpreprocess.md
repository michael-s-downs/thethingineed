# PREPROCESS COMPONENT

## Index

- [PREPROCESS COMPONENT](#preprocess-component)
  - [Index](#index)
  - [Overview](#overview)
    - [Key Features](#key-features)
  - [Get started](#get-started)
  - [Configuration](#configuration)
    - [API JSON body parameters](#api-json-body-parameters)
    - [Environment variables](#environment-variables)
  - [Deployment](#deployment)
  - [Documentation](#documentation)

## Overview

The preprocess component is designed to extract text and geospatial information from multiple document formats, including PDFs, PowerPoint presentations and Excel spreadsheets, among others. It streamlines the text extraction process, making content from different sources easy to handle and analyze. This component provides a reliable solution for obtaining structured text data, facilitating seamless integration into diverse data processing workflows and supporting a wide range of applications, from data analysis to information retrieval.

### Key Features

- Multi-platform Support: Seamlessly integrates with major cloud providers like Azure and AWS, ensuring scalability, reliability, and high availability for mission-critical applications.
- Customizable Parameters: Provides fine-tuned control over the text extraction process by adjusting parameters such as OCR engine, language, and desired document pages, ensuring an optimal configuration for specific use cases.
- Flexible OCR Integration: Utilizes several OCR engines, including Amazon Textract and Azure AI Document Intelligence, to accommodate different document types and conditions.
- Language Support: Extracts text from documents in multiple languages, such as English, Spanish or Japanese, enabling global usability and application.
- Document Format Compatibility: Supports  for a wide array of document formats including PDF, DOCX, PPTX, XLSX, TXT, PNG, JPEG, and more.

## Get started
To use the preprocess component, you need to have the integration component, designed to receive the API call, adapt the input message and inject it into the corresponding queue, among other functions. In addition to that, a queue messaging service  and a cloud storage service will be needed, either Azure or AWS ones.

The first step you need to take to use the preprocess component on your local machine is to set the [environment variables](#environment-variables).

After that, you need to create an environment with Python 3.11 and install the required packages listed in the "requirements.txt" file:

```sh
pip install -r "**path to the requirements.txt file**"
```
Once everything above is configured, you need to run the main.py file from the integration-receiver sub-component, and call the /process API endpoint with body and headers similar to the following example:

```python
import requests
import json

url = "http://localhost:8888/process"

payload = {
  "index": "index_name",
  "operation": "indexing",
  "documents_metadata": {
    "doc1.pdf": {"content_binary": "doc encoded as base64"}
  },
  "response_url": "http://"
}

headers = {
  "x-api-key": ""
}

response = requests.request("POST", url, headers=headers, data=payload)
```

## Configuration

### Api json body parameters

- **operation** (required): Operation to perform. Currently, it must always have the value "indexing".
- **documents_metadata** (required): Content of the documents. The expected format is a JSON with each document name as key and another JSON as value with the key 'content_binary' and the document serialized in base64 as value. This value can be plain text or file binary but the extension must be consistent.
- **force_ocr** (optional): Parameter to force the document to through an OCR engine even if document text has been extracted with other methods. Defaults to False.

### Environment variables

```json
"PROVIDER": "azure/aws", //Cloud storage and queue service provider
"STORAGE_DATA": "tenant-data", //Cloud storage bucket or blob to store datasets
"STORAGE_BACKEND": "tenant-backend", //Cloud storage bucket or blob to store preprocess results
"SECRETS_PATH": "secrets/", //Path to the secrets file
"REDIS_DB_STATUS": int, //Identifier of redis database to save process status
"REDIS_DB_TIMEOUT": int, //Identifier of redis database to track process timeout
"Q_PREPROCESS_START": "" //Name of the queue of the start-preprocess subcomponent
"Q_PREPROCESS_EXTRACT": "" //Name of the queue of the preprocess-extract subcomponent
"Q_PREPROCESS_OCR": "" //Name of the queue of the preprocess-ocr subcomponent
"Q_PREPROCESS_END": "" //Name of the queue of the start-preprocess subcomponent
```

## Deployment

To deploy the component on your own cloud use [this guide](deploy-guide-link).

## Documentation

For further information follow the [link](documentation.md).
