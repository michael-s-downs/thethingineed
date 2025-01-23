# Preprocess Services Documentation

## Index

- [Preprocess Services Documentation](#preprocess-services-documentation)
  - [Index](#index)
  - [Overview](#overview)
    - [Key Features](#key-features)
  - [Getting started](#getting-started)
  - [Concepts and definitions](#concepts-and-definitions)
  - [Preprocess component distribution](#preprocess-component-distribution)
  - [Configuration](#configuration)
    - [Input json parameters](#input-json-parameters)
    - [Environment variables](#environment-variables)
  - [Deployment](#deployment)
  - [Documentation](#documentation)

## Overview

The preprocess component is designed to extract text and geospatial information from multiple document formats, including PDFs, PowerPoint presentations and Excel spreadsheets, among others. It streamlines the text extraction process, making content from different sources easy to handle and analyze. This component provides a reliable solution for obtaining structured text data, facilitating seamless integration into diverse data processing workflows and supporting a wide range of applications, from data analysis to information retrieval.

### Key Features

* Multi-platform Support: Seamlessly integrates with major cloud providers like Azure and AWS, ensuring scalability, reliability, and high availability for mission-critical applications.
* Customizable Parameters: Provides fine-tuned control over the text extraction process by adjusting parameters such as OCR engine, language, and desired document pages, ensuring an optimal configuration for specific use cases.
* Flexible OCR Integration: Utilizes several OCR engines, including Amazon Textract and Azure AI Document Intelligence, to accommodate different document types and conditions.
* Language Support: Extracts text from documents in multiple languages, such as English, Spanish or Japanese, enabling global usability and application.
* Document Format Compatibility: Supports  for a wide array of document formats including PDF, DOCX, PPTX, XLSX, TXT, PNG, JPEG, and more.

## Getting started
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

## Concepts and definitions
* <b>Preprocess:</b> Set of techniques applied to a text to extract the data in a proper way to index it.
* <b>OCR:</b> Optical Character Recognition (OCR) is the process by which an image of text is converted into a machine-readable text format

## Preprocess component distribution
The preprocess flow, is a complex flow that needs the communication between different individuals by uploading the files in different folders of the cloud storage ('STORAGE_BACKEND') and queues. The task for each individual in the whole preprocess flow is:

* <b>preprocess-start</b>: Receives the input from the <b>'integration-sender'</b> component and formats it to the right format by adding some aditional information to the message. Finally creates the key in redis for the whole process. If the process goes smoothly, writes the message in the next process (queue)
* <b>preprocess-extract</b>: Manages the documents operations like text extraction or pages count. Depending on the input and the document features, it has different flows:
  - If the text extraction goes right, directly goes to <b>'preprocess-end'</b> component
  - If there is a problem during text extraction, converts the document pages into separated images to pass them through the OCR (selected by the user)
  - If <b>'force_ocr'</b>  passed it will do the ocr always
    + If <b>'llm-ocr'</b>  selected with the <b>'query'</b> parameter passed in the input, the text extraction will not be done
    + If <b>'query'</b> parameter not passed, text extraction will be done just to get the language of the text (to adapt the query to it). (the text of the ocr will be lost as is not useful)
* <b>preprocess-ocr</b> (optional): Deals with the text extraction from the images generated by the <b>'preprocess-extract'</b> component by sending it to the supported ocr's:  
  - FormRecognizer
  - Amazon Textract
  - TesseractOCR 
  - LLMOCR: LLM vision based OCR using the 'genai-LLMAPI' component
* <b>preprocess-end</b>: This component, is the previous step before indexing. Its function is to check in redis if the process went right to write the message in the 'genai-infoindexing' component.

Finally, the supported document formats for this process are: pdf, jpeg, jpg, png, txt, docx, xls, xlsx, pptx

## Configuration

### Input json parameters
As preprocess_start, manages the organization of the json that goes over all preprocess components and indexing, here is the explanation of each parameter (in the correct output transformed by <b>'preprocess-start'</b>):

Then an example with the following key data could be:
* <b>department</b>: documentation_test
* <b>index_id</b>: ir_index_20250123_133146_371261_t6s48u
* <b>tenant</b>: test
* <b>request_id</b>: request_20250123_133139_485292_1hjs11
* <b>input filename</b>: documentation.pdf

```json
{
    "generic": {
        "project_conf": {
            "laparams": "none",
            "process_id": "ir_index_20250123_133146_371261_t6s48u",
            "timeout_id": "timeout_id_test:ir_index_20250123_133146_371261_t6s48u",
            "process_type": "ir_index",
            "department": "documentation_test",
            "report_url": "http://techhubapigw.techhubadminconfiguration/apigw/license/report/52fb565179ff4e7cb98bd6776d2fc62a",
            "tenant": "test",
            "project_type": "text",
            "url_sender": "test--q-integration-sender",
            "timeout_sender": 60
        },
        "dataset_conf": {
            "dataset_path": "documentation_test/request_20250123_133139_485292_1hjs11",
            "dataset_csv_path": "documentation_test/request_20250123_133139_485292_1hjs11/datasets/-2854053896596469679.csv",
            "path_col": "Url",
            "label_col": "CategoryId",
            "dataset_id": "ir_index_20250123_133146_371261_t6s48u"
        },
        "preprocess_conf": {
            "num_pag_ini": 0,
            "page_limit": 1000,
            "ocr_conf": {
                "force_ocr": false,
                "extract_tables": false,
                "batch_length": 32,
                "files_size": 30000000,
                "calls_per_minute": 400,
                "ocr": "azure-ocr"
            },
        },
        "indexation_conf": {
            "vector_storage_conf": {
                "index": "agents_test_2",
                "vector_storage": "elastic-test",
                "modify_index_docs": {}
            },
            "chunking_method": {
                "method": "simple",
                "window_overlap": 100,
                "window_length": 1000
            },
            "models": [
                {
                    "alias": "techhub-pool-world-ada-3-small",
                    "embedding_model": "text-embedding-3-small",
                    "platform": "azure"
                }
            ],
            "metadata": {
                "year": 2025,
                "category": "health"
            }
        }
    },
    "specific": {
        "path_txt": "documentation_test/ir_index_20250123_133146_371261_t6s48u/txt",
        "path_text": "documentation_test/ir_index_20250123_133146_371261_t6s48u/text",
        "path_img": "documentation_test/ir_index_20250123_133146_371261_t6s48u/imgs",
        "path_cells": "documentation_test/ir_index_20250123_133146_371261_t6s48u/cells",
        "path_tables": "documentation_test/ir_index_20250123_133146_371261_t6s48u/tables",
        "dataset": {
            "dataset_key": "ir_index_20250123_133146_371261_t6s48u:ir_index_20250123_133146_371261_t6s48u"
        },
        "document": {
            "filename": "documentation_test/request_20250123_133139_485292_1hjs11/documentation.pdf",
            "label": 0,
            "metadata": {
                "year": 2025,
                "category": "health"
            },
            "n_pags": 13,
            "language": "en"
        },
        "paths": {
            "images": [
              {
                "filename": "albertoperezblasco/ir_index_20250123_075355_174425_9yd5ki/imgs/albertoperezblasco/request_20250123_075340_335302_yc4gfc/paginablanca/pags/paginablanca_pag_0.jpeg", 
                "number": 0
              }
              . . .
              {
                "filename": "albertoperezblasco/ir_index_20250123_075355_174425_9yd5ki/imgs/albertoperezblasco/request_20250123_075340_335302_yc4gfc/paginablanca/pags/paginablanca_pag_0.jpeg", 
                "number": 13
              }
            ],
            "text": "documentation_test/ir_index_20250123_133146_371261_t6s48u/txt/documentation_test/request_20250123_133139_485292_1hjs11/documentation.txt",
            "cells": "documentation_test/ir_index_20250123_133146_371261_t6s48u/cells/txt/documentation_test/request_20250123_133139_485292_1hjs11/documentation"
        }
    },
    "integration": {}, //integration sender output
    "tracking": {
        "request_id": "documentation_test/request_20250123_133139_485292_1hjs11",
        "pipeline": [
            {
                "ts": 1737639106.371,
                "step": "INTEGRATION_SENDER",
                "type": "INPUT"
            },
            . . .
            {
                "ts": 1737639304.222,
                "step": "GENAI_INFOINDEXING",
                "type": "INPUT"
            }
        ]
    }
}
```



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
