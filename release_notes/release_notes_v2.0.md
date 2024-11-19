<!-- ### This code is property of the GGAO ### -->

# Release Notes

## LLMAPI

- Added pydantic to improve input and output parsing params and error handling

- Added temperature range by llm

```json
"llm_metadata": {
    "model": "gpt-3.5-pool-techhub-europe",
    "max_input_tokens": 600,
    "max_tokens": 1000,
    "temperature": 0
}
```

- Added new endpoint to get the content of a template. (get_template, GET)

Params:
    - template_name: system_query

Response:

```json
{
    "template": {
        "system": "$system",
        "user": "$query"
    },
}
```

- Added new endpoint to list prompt templates. (list_templates, GET)

Response:

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

- Bug fixes and code improvement

## Inforetrieval

- Added new endpoint to lists all indexes in the Elasticsearch database, their names and the models associated with each one. (list_indices, GET)

Response:

```json
{
    "indices": [
        {
            "models": [
                "text-embedding-ada-002"
            ],
            "name": "index_1"
        },
        {
            "models": [
                "text-embedding-ada-002",
                "text-embedding-large",
                "cohere.embed-multilingual-v3"
            ],
            "name": "index_2"
        }
    ],
    "status": "ok",
    "status_code": 200
}
```

- Added new endpoint to list the models available. (get_models, GET)

Params:
    - zone: Filters by the zone of the model.
    - pool: Filters by the pool of the model.
    - embedding_model: Filters by the embedding model.

Response:

```json
{
    "result": {
        "models": [
            "cohere-english-eastus",
            "cohere-multilingual-eastus",
            "titan-v1-eastus",
            "titan-v2-eastus"
        ],
        "pools": [
            "cohere-multilingual-v3-pool-world",
            "titan-v1-pool-world",
            "cohere-english-v3-pool-world",
            "titan-v2-pool-world"
        ]
    },
    "status": "ok",
    "status_code": 200
}
```

- Elasticsearch index generation without uppercase in the name to avoid errors

- New models added:
  - Ada small
  - Ada large
  - Titan
  - Cohere english
  - Cohere multilingual

  *Refer to the readme file to see the full list of models*

- New param 'strategy_mode' to select type of strategy while retrieving.
  - genai_retrieval: The old one used, default.
  - llamaindex_fusion: Uses LLamaIndex QueryFusion.

- Bug fixes and code improvement

## Infoindexing

- Elasticsearch index generation without uppercase in the name to avoid errors

- New models added:
  - Ada small
  - Ada large
  - Titan
  - Cohere english
  - Cohere multilingual

  *Refer to the readme file to see the full list of models*

## Compose

- Removed param 'queryfilter_template', 'responsefilter_template' and 'reformulate'.

- New action added 'filter_query' to replace 'queryfilter_template' from api call.
The only type for the moment is 'llm'. Calls an LLM using LLMAPI to classify and filter the query. This action must go before retrieve and llm_action.

Action example:

```json
{
    "action": "filter_query",
    "action_params":{
        "params": {
            "template" : "query_filter"
        },
        "type": "llm"
    }
}
```

- New action added 'filter_response' to replace 'responsefilter_template' from api call.

Action example:

```json
{
    "action": "filter_response",
    "action_params":{
        "params": {
            "template" : "response_filter"
        },
        "type": "llm"
    }
}
```

- New action add 'reformulate_query' to replace 'reformulate' from api call. This action uses previuos queries and context to refomulate the new one.

Params:
    - Max_persistence: Sets the number of old queries to use.
    - Template_name: Prompt template name to use.
    - Save_mod_query: Saves the non reformulated query or not.
  
Action example:

```json
{
    "action": "reformulate_query",
    "action_params":{
        "params":{
            "max_persistence": 3,
            "template_name": "reformulate",
            "save_mod_query": false
        },
        "type": "mix_queries"
    }
}
```

- Langfuse params can be set in the api call.

```json
{
    "langfuse": {
        "secret_key": "example sk",
        "public_key": "example pk",
        "host": "https://host example.com"
    }
}
```

- New action 'expansion' to expand the query into multiple ones. The only type available is "lang". It expands the query into multiple languages to retrieve information from different languages.

Params:
    - langs: List with the desired languages with its full name or an abbreviations.
    The available abbreviations are:

        - "ja": "japanese",
        - "es": "spanish",
        - "en": "english",
        - "fr": "french",
        - "de": "german",
        - "zh": "chinese",
        - "it": "italian",
        - "ko": "korean",
        - "pt": "portuguese",
        - "ru": "russian",
        - "ar": "arabic",
        - "hi": "hindi",
        - "tr": "turkish",
        - "nl": "dutch",
        - "sv": "swedish",
        - "pl": "polish",
        - "el": "greek",
        - "he": "hebrew",
        - "vi": "vietnamese",
        - "th": "thai",
        - "ca": "catalan"

```json
{
    "action": "expansion",
    "action_params":{
        "params": {
            "langs" : "$langs"
        },
        "type": "lang"
    }
}
```

- New environment variable 'DEFAULT_LLM_MODEL' to set the default llm to use in the default templates.

- Bug fixes and code improvement and added unittest with pytest
