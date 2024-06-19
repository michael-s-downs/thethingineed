RG_NAME = "kisco"
RG_LOCATION = "westeurope"

SA_NAME             = "sakisco01"
SA_TIER             = "Standard"
SA_REPLICATION_TYPE = "LRS"
SA_CONTAINER_NAMES  = ["backend", "data"]

SB_NAME = "sbkisco01"
SB_TIER = "Basic"
SB_CAPACITY = 0
SB_QUEUES = [
    {
        "name": "flowmgmt-checkend",
        "enable_partitioning": false,
        "requires_session": false,
        "max_size_in_megabytes": 1024
    },
    {
        "name": "flowmgmt-infodelete",
        "enable_partitioning": false,
        "requires_session": false,
        "max_size_in_megabytes": 1024
    },
    {
        "name": "flowmgmt-genai-infoindexing",
        "enable_partitioning": false,
        "requires_session": false,
        "max_size_in_megabytes": 1024
    },
    {
        "name": "integration-sender",
        "enable_partitioning": false,
        "requires_session": false,
        "max_size_in_megabytes": 1024
    },
    {
        "name": "preprocess-end",
        "enable_partitioning": false,
        "requires_session": false,
        "max_size_in_megabytes": 1024
    },
    {
        "name": "preprocess-ocr",
        "enable_partitioning": false,
        "requires_session": false,
        "max_size_in_megabytes": 1024
    },
    {
        "name": "preprocess-extract",
        "enable_partitioning": false,
        "requires_session": false,
        "max_size_in_megabytes": 1024
    },
    {
        "name": "preprocess-start",
        "enable_partitioning": false,
        "requires_session": false,
        "max_size_in_megabytes": 1024
    }
]
