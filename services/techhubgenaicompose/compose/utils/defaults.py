### This code is property of the GGAO ###


EMPTY_STREAM = {
    "content": "", 
    "meta": {},
    "score": 0, 
    "answer": ""
}

TIMEOUT = 30

SUM_TEMPLATE = {
    "query_metadata": {
        "query": "Do a summary of the next text: ",
        "template_name": "system_query_and_context_techsummary"
    },
    "llm_metadata": {
        "max_tokens": 10000,
        "model": "techhubinc-pool-world-gpt-3.5-turbo-16k"
    },
    "platform_metadata": {
        "platform":"azure", 
        "timeout": TIMEOUT
    }
}

FILTER_TEMPLATE = {
    "query_metadata": {
        "query": "",
        "template_name": "emptysystem_query"
    },
    "llm_metadata": {
        "model": "techhubinc-pool-world-gpt-3.5-turbo-16k"
    },
    "platform_metadata": {
        "platform":"azure", 
        "timeout": TIMEOUT
    }
}

REFORMULATE_TEMPLATE = {
    "query_metadata": {
        "query": "",
        "template_name": "reformulate"
    },
    "llm_metadata": {
        # "max_tokens": 10000,
        "model": "techhubinc-pool-world-gpt-3.5-turbo-16k"
    },
    "platform_metadata": {
        "platform":"azure", 
        "timeout": TIMEOUT
    }
}

TRANSLATE_TEMPLATE = {
    "query_metadata": {
        "query": "",
        "template": "{\"system\": \"You are a profesional translator and only answer with the translated sentence.\",\"user\": \"$query\"}"
    },
    "llm_metadata": {
        "model": "techhubinc-pool-world-gpt-3.5-turbo-16k"
    },
    "platform_metadata": {
        "platform":"azure", 
        "timeout": TIMEOUT
    }
}

FILTERED_ACTIONS = [
    {
		"action": "retrieve",
		"action_params": {
			"type": "streamlist",		
            "params": {
				"streamlist":[
					{
						"content": "",
						"meta": {
							"field1": ""
						},
						"scores": {
							"bm25": 1,
							"sim-example": 1
						}
					}
				],
                "generic": {
                    "index_conf": {
                        "query": ""
                    }
                }		
            }	
		}
	}
    ,{
        "action": "llm_action",
        "action_params": {
            "params": {
                "llm_metadata": {
                    "model": "techhubinc-pool-world-gpt-3.5-turbo-16k",
					"max_input_tokens":5000
                },
                "platform_metadata": {
                    "platform": "azure"
                },
                "query_metadata": {
                    "query": "",
					"system":"You are a helpful assistant",
                    "template_name": "system_query"
                }
            },
            "type": "llm_content"
        }
    }]