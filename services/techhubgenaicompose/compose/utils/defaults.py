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
        "model": "gpt-3.5-turbo"
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
        "model": "gpt-3.5-16k-pool-europe"
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
        "model": "gpt-3.5-16k-pool-europe"
    },
    "platform_metadata": {
        "platform":"azure", 
        "timeout": TIMEOUT
    }
}
