### This code is property of the GGAO ###

# Native imports

# Installed imports

# Custom imports



def is_available_metadata(metadata: dict, metadata_primary_keys: str, chunking_method: str) -> bool:
    # Metadata always present on every document
    mandatory_metadata = ["filename", "uri", "document_id", "snippet_number", "snippet_id",  "_node_content", "_node_type" "doc_id", "ref_doc_id"]
    # Metadata that can be present or not depending on the chunking method
    surrounding_context_window_metadata = ["window", "original_text"]
    recursive_metadata = ["index_id"]
    

    if chunking_method == "surrounding_context_window":
        all_metadata = mandatory_metadata + surrounding_context_window_metadata + list(metadata.keys())
    elif chunking_method == "recursive":
        all_metadata = mandatory_metadata + recursive_metadata + list(metadata.keys())
    else:
        all_metadata = mandatory_metadata + list(metadata.keys())
    return metadata_primary_keys in all_metadata