### This code is property of the GGAO ###


from elasticsearch.exceptions import TransportError

IR_INDICES = "src/ir/index/"
INDEX_S3 = lambda index: IR_INDICES + index + ".json"


def modify_index_documents(document_store, modify_index_docs: dict, docs: list, logger) -> list:
    """
    modify_index_docs must be one of two types, Delete and Update:

    Example =>
            modify_index_docs = {
                "delete": {
                    "metadata_key_1": ["match", ...]
                },
                "update": {
                    "metadata_key_2": True,
                    "metadata_key_3": False,
                }
            }
    - To delete exact matches: {key_to_filter: [exact_matches_to_delete, ...]}
    - To update documents with same key_to_filter than the ones in the document store {"key_to_filter": True}
    - To avoid updating documents with same key_to_filter than the ones in the document store {key_to_filter: False}

    Congruences are not controlled due to the inefficiency of the process. Be careful when updating fields.

    :param document_store: Access to Elastic Search
    :param modify_index_docs: Configuration to modify
    :param docs: Documents to indexes
    :return: List of documents indexes
    """
    delete_key = "delete"
    update_key = "update"

    for key in modify_index_docs:
        if key not in [delete_key, update_key]:
            raise ValueError(f"'modify_index_docs' only admits two keys: {delete_key} and {update_key}.")

    # Delete given documents
    deletion_dict = modify_index_docs.get(delete_key, {})
    for key_to_filter in deletion_dict:
        deletion_values = deletion_dict[key_to_filter]
        if not isinstance(deletion_values, list) or not all([isinstance(val, str) for val in deletion_values]):
            raise ValueError("All values in 'Delete' field must be of type list and must contain only strings.")

        document_store.delete_documents(filters={key_to_filter: deletion_values})
        logger.info(f"Deleting documents whose '{key_to_filter}' are in: {deletion_values}.")

    # Update documents given the boolean conditions
    update_dict = modify_index_docs.get(update_key, {})
    if update_dict:
        try:
            ds_documents = document_store.get_all_documents()
        except TransportError as exc:
            logger.error("All documents could not be retrieved and thus update won't work. Alternativelly you could use deletion")
            raise exc

        for key_to_filter in update_dict:
            ds_documents_keys = [doc.meta[key_to_filter] for doc in ds_documents if key_to_filter in doc.meta]
            new_documents_keys = [doc['meta'][key_to_filter] for doc in docs if key_to_filter in doc['meta']]
            keys_in_document_store = [f for f in new_documents_keys if f in ds_documents_keys]
            if len(new_documents_keys) != len(docs):
                frac = f"{len(new_documents_keys)}/{len(docs)}"
                raise ValueError(f"Only {frac} documents had the {key_to_filter} key in metadata. "
                                f"All documents must have the key defined when filtering that key.")

            if len(ds_documents_keys) != len(ds_documents):
                logger.warning(f"Not all document-store documents had the {key_to_filter} key in metadata. "
                            f"Consider reindexing with stable metadata keys.")

            update_bool = update_dict[key_to_filter]
            if not isinstance(update_bool, bool):
                raise ValueError(f"All values in 'Update' field must be of type bool. Error in {key_to_filter}.")

            if update_bool:
                logger.info(f"{len(keys_in_document_store)} docs will be updated due to "
                            f"having the same {key_to_filter} as index ones.")
                document_store.delete_documents(filters={key_to_filter: keys_in_document_store})
            else:
                n_docs = len(docs)
                docs = [doc for doc in docs if doc['meta'][key_to_filter] not in keys_in_document_store]
                logger.info(f"{n_docs - len(docs)} docs will not be updated due to "
                            f"having the same {key_to_filter} as the index ones.")
    return docs
