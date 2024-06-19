### This code is property of the GGAO ###


# Generic Codes
ERROR = 500  # Error
ERROR_REPORTING = 599  # Couldn't report to api
ERROR_PROCESS_TYPE = 410  # Couldn't get process type
ERROR_PROJECT_TYPE = 411  # Couldn't get project type
ERROR_NO_DOCUMENT = 412  # Couldn't get document
ERROR_NO_PAGE_LIMIT = 413  # Couldn't get number pages limit
ERROR_NO_DATASET_ID = 414  # Couldn't get dataset id
OK = 201  # No errors
TIMEOUT = 408  # Exceeded timeout

# Dataset Status Codes
BEGIN_LIST = 0  # List documents started
END_LIST = 1  # List documents ended successfully
START_PROCESS = 2  # List documents preprocess started
NO_DOCS = 404  # No docs in dataset
PROCESS_FINISHED = 200  # Process finished successfully
REDIS_DATASET_STATUS = 3  # Model created
DATASET_PROCESSED = 4  # Dataset processed successfully
REDIS_MODEL_STATUS = 4  # Model processed successfully
REDIS_MODEL_STATUS_ERROR = 500  # Model with errors

# Extractti Errors
ERROR_NO_DOWNLOADED = 521  # Document file couldn't be downloaded
ERROR_NUM_PAGES = 522  # Couldn't extract number of pages of the document
ERROR_NO_IMG = 523  # Images of the document couldn't be extracted
ERROR_NO_EXTRACTED = 524  # Nor image nor text could be extracted
ERROR_NO_UPLOADED = 525  # Nor image nor text could be extracted
ERROR_GETTING_LAYOUT = 526  # Couldn't get type lines
ERROR_SEGMENTATION = 527  # Couldn't segment document

# Training Errors
ERROR_MODEL_PARAMS = 571  # Couldn't get model and preprocess params
ERROR_READING_DATASETS = 572  # Couldn't read datasets
ERROR_MERGING_DATASETS = 573  # Couldn't merge datasets
ERROR_FILTER_LANG = 574  # Couldn't filter by language
ERROR_CS = 575  # Couldn't create classification service
ERROR_TRAINING = 576  # Couldn't train model
ERROR_UPLOAD_MODEL = 577  # Couldn't upload model

# Document Status Codes
BEGIN_DOCUMENT = 0  # Init document processing
EXTRACTED_DOCUMENT = 2  # Extractti finished successfully
LAYOUT_DOCUMENT = 3  # LayoutLM finished successfully
SEGMENTED_DOCUMENT = 5  # Segmentation finished successfully
TRANSLATED_DOCUMENT = 4  # Transcli finished successfully
STORED_DOCUMENT = 4  # Document stored
DOCUMENT_COUNT_CREATED = 0  # Created document count

# Batch Status Codes
BATCH_CREATED = 0  # Created batches
BATCH_PROCESSED = 1  # Batch processed
BATCH_VALID_PROCESSED = 1  # Batch valid successfully
BATCH_VALID_ERROR = 530  # Error batch
DOCUMENT_PROCESSED = 3  # Document processed
DOCUMENT_BATCHED = 300  # Document batched
ERROR_NO_BATCH_IMAGE = 531  # Nor image in batch

# Transcli Status Codes
ERROR_NO_TRANSLATED = 541  # Error to translate

# Images Status Codes
IMAGE_PROCESSED = 5  # Image process successfully
IMAGE_NO_PROCESSED = 81  # Image not process
EXTRACT_IMG_FEATURES = 5  # Extract features of image
IMAGE_MANAGER_PROCESS = 6  # Image processed by manager

# Extract info Codes
END_EXTRACTION = 2  # End extraction generic
