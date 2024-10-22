# Global
RG_NAME="ragkiscotest"
RG_LOCATION="westeurope"
RG_LOCATION_VNET="japaneast"
RG_NAME_DNS="Techhub"
VNET_NAME="vnet_techhub_dev_japaneast_001"
RG_SUBNET="subnet_techhub_dev_japaneast_002"
# Storage
SA_TIER="Standard"
SA_REPLICATION_TYPE="LRS"
SA_CONTAINER_NAMES=["backend","data"]
# Queues
SB_CAPACITY="0"
SB_QUEUES=[{"name": "flowmgmt-checkend","enable_partitioning": false,"requires_session": false,"max_size_in_megabytes": 1024},{"name": "flowmgmt-infodelete","enable_partitioning": false,"requires_session": false,"max_size_in_megabytes": 1024},{"name": "genai-infoindexing","enable_partitioning": false,"requires_session": false,"max_size_in_megabytes": 1024},{"name": "integration-sender","enable_partitioning": false,"requires_session": false,"max_size_in_megabytes": 1024},{"name": "preprocess-end","enable_partitioning": false,"requires_session": false,"max_size_in_megabytes": 1024},{"name": "preprocess-ocr","enable_partitioning": false,"requires_session": false,"max_size_in_megabytes": 1024},{"name": "preprocess-extract","enable_partitioning": false,"requires_session": false,"max_size_in_megabytes": 1024},{"name": "preprocess-start","enable_partitioning": false,"requires_session": false,"max_size_in_megabytes": 1024}]
SB_TIER="Basic"
CREATE_SB=false