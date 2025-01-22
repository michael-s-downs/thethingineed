### This code is property of the GGAO ###


# Native imports
from typing import List
from abc import ABC, abstractmethod
import uuid
import os
import re
import json
import logging


# Installed imports
from llama_index.core.node_parser import SentenceSplitter, SentenceWindowNodeParser
from llama_index.core.schema import IndexNode
from llama_index.core import Document
import mmh3


# Custom imports
from common.errors.genaierrors import PrintableGenaiError
from common.genai_controllers import load_file
from common.logging_handler import LoggerHandler
from common.services import CHUNKING_SERVICE


class ChunkingMethod(ABC):
    CHUNKING_FORMAT = "ChunkingMethod"

    def __init__(self, window_length: int, window_overlap: int, origin: tuple, workspace: tuple):
        self.window_length = window_length
        self.window_overlap = window_overlap
        self.origin = origin
        self.workspace = workspace
        log = logging.getLogger('werkzeug')
        log.disabled = True

        logger_handler = LoggerHandler(CHUNKING_SERVICE, level=os.environ.get('LOG_LEVEL', "INFO"))
        self.logger = logger_handler.logger


    @abstractmethod
    def get_chunks(self, docs: list, encoding, index_metadata):
        """Method to get the chunks"""


    @classmethod
    def is_method_type(cls, model_type):
        """Checks if a given model type is equel to the model format and thus it must be the one to use.
        """
        return model_type == cls.CHUNKING_FORMAT

    def _add_nodes_metadata(self, nodes, origin, index_metadata):
        final_nodes = []
        sections = ""
        mandatory_metadata_keys = [
            "uri", "document_id", "snippet_number", "snippet_id",
            "_node_content", "_node_type", "doc_id", "ref_doc_id", "window",
            "original_text", "index_id", "sections_headers", "tables"
        ]

        for counter, node in enumerate(nodes):
            titles_tables_node, sections = self._add_titles_and_tables(node, sections, origin)
            # Must be added here as in titles_tables text is changed
            ids_node = self._add_ids(titles_tables_node, counter)
            # Exclude when embedding generation

            if isinstance(index_metadata, list):
                ids_node.excluded_embed_metadata_keys = list(set(ids_node.metadata.keys()) - set(index_metadata))
            elif index_metadata:
                ids_node.excluded_embed_metadata_keys = mandatory_metadata_keys
            else:
                ids_node.excluded_embed_metadata_keys = list(ids_node.metadata.keys())
            ids_node.excluded_llm_metadata_keys = list(ids_node.metadata.keys())
            final_nodes.append(ids_node)
        return final_nodes

    @staticmethod
    def _add_ids(node, counter: int):
        node.metadata['snippet_number'] = counter
        id = "{:02x}".format(mmh3.hash128(str(node.text), signed=False))
        node.metadata['snippet_id'] = "{:02x}".format(mmh3.hash128(str(node.text), signed=False))
        node.id_ = id
        return node

    def _add_titles_and_tables(self, node, sections: str, origin):
        text = node.text
        meta = node.metadata
        mapping_path = meta.pop('_header_mapping', "")
        if mapping_path:
            headers_mapping = json.loads(load_file(origin, mapping_path))
            titles = re.findall(r"<(pag_\d+_header_\d+)>", text)
            if not titles:
                meta['sections_headers'] = sections.split("||")[-1]
            else:
                sections = "||".join([headers_mapping[t] for t in titles])
                meta['sections_headers'] = sections
            for t in titles:
                text = text.replace(f"<{t}>", "")
        else:
            meta['sections_headers'] = ""

        csv_path = meta.pop('_csv_path', "")
        if csv_path:
            tables = re.findall(r"<(pag_\d+_table_\d+)>", text)
            for t in tables:
                csv = load_file(origin, csv_path + t + ".csv").decode()
                text = text.replace(f"<{t}>", csv)
            meta['tables'] = True if tables else False
        else:
            meta['tables'] = ""

        node.text = text
        return node, sections


class Simple(ChunkingMethod):
    CHUNKING_FORMAT = "simple"

    def __init__(self, window_length: int, window_overlap: int, origin: tuple, workspace: tuple):
        super().__init__(window_length, window_overlap, origin, workspace)

    def get_chunks(self, docs: list, encoding, index_metadata) -> list:
        """Get nodes from documents

        :param docs: list of documents
        :param encoding: encoding to use

        :return: list of nodes
        """
        nodes_per_doc = []
        for doc in docs:
            # If not added, does not appear in inforetrieval (value substituted by llama_index in indexation)
            doc.metadata.setdefault('document_id', str(uuid.uuid4()))
            # Exclude when splitting (chunk size dependent on the metadata)
            doc.excluded_llm_metadata_keys = list(doc.metadata.keys())
            doc.excluded_embed_metadata_keys = list(doc.metadata.keys())
            nodes = SentenceSplitter(chunk_size=self.window_length, chunk_overlap=self.window_overlap,
                                     tokenizer=encoding.encode, paragraph_separator="\\n\\n").get_nodes_from_documents([doc], show_progress=True)

            if eval(os.getenv('TESTING', "False")):
                final_nodes = self._add_nodes_metadata(nodes, self.origin, index_metadata)
            else:
                final_nodes = self._add_nodes_metadata(nodes, self.workspace, index_metadata)
            nodes_per_doc.append(final_nodes)
        return nodes_per_doc


class Recursive(ChunkingMethod):
    CHUNKING_FORMAT = "recursive"

    def __init__(self, window_length: int, window_overlap: int, origin: tuple, workspace: tuple, sub_window_length: int,
                 sub_window_overlap: int,):
        super().__init__(window_length, window_overlap, origin, workspace)
        self.sub_window_length = sub_window_length
        self.sub_window_overlap = sub_window_overlap

    @staticmethod
    def _add_ids(node, counter: int):
        node.metadata['snippet_number'] = counter
        id = "{:02x}".format(mmh3.hash128(str(node.text), signed=False))
        node.metadata['snippet_id'] = id
        node.id_ = id
        # This one is the reference to parent chunk (extended while children created)
        node.metadata['index_id'] = id
        return node
    
    @staticmethod
    def _add_subnode_metadata(node, parent_node, id, counter):
        node.metadata['index_id'] = parent_node.node_id
        node.metadata['snippet_number'] = float(f"{parent_node.metadata['snippet_number']}.{counter+1}")
        node.metadata['snippet_id'] = id
        return node


    def get_chunks(self, docs: list, encoding, index_metadata) -> list:
        """Get nodes from documents

        :param docs: list of documents
        :param encoding: encoding to use

        :return: list of nodes
        """
        nodes_per_doc = []
        for doc in docs:
            # If not added, does not appear in inforetrieval (value substituted by llama_index in indexation)
            doc.metadata.setdefault('document_id', str(uuid.uuid4()))
            # Exclude when splitting (chunk size dependent on the metadata)
            doc.excluded_llm_metadata_keys = list(doc.metadata.keys())
            doc.excluded_embed_metadata_keys = list(doc.metadata.keys())

            base_nodes = SentenceSplitter(chunk_size=self.window_length, chunk_overlap=self.window_overlap,
                                                 tokenizer=encoding.encode, paragraph_separator="\\n\\n").get_nodes_from_documents([doc], show_progress=True)
            
            # The metadata and configurations are passed when splitting recursively
            if eval(os.getenv('TESTING', "False")):
                base_nodes = self._add_nodes_metadata(base_nodes, self.origin, index_metadata)
            else:
                base_nodes = self._add_nodes_metadata(base_nodes, self.workspace, index_metadata)
            
            sub_node_splitter = SentenceSplitter(chunk_size=self.sub_window_length, chunk_overlap=self.sub_window_overlap,
                                                 tokenizer=encoding.encode, paragraph_separator="\\n\\n")
            nodes = []
            for i, base_node in enumerate(base_nodes):
                self.logger.debug(f"Doing recursive children of node {i}")
                sub_nodes = sub_node_splitter.get_nodes_from_documents([base_node])
                for child_number, sub_node in enumerate(sub_nodes):
                    # Manage ids from sub_nodes (id=hashed content and index_id must be parent_node id)
                    id = "{:02x}".format(mmh3.hash128(str(sub_node.text), signed=False))
                    sub_node.id_ = id
                    sub_inode = IndexNode.from_text_node(sub_node, base_node.node_id)
                    nodes.append(self._add_subnode_metadata(sub_inode, base_node, id, child_number))

                # add original node to node (IndexNode format mandatory)
                nodes.append(IndexNode.from_text_node(base_node, base_node.node_id))

            nodes_per_doc.append(nodes)
        return nodes_per_doc

class SurroundingContextWindow(ChunkingMethod):
    CHUNKING_FORMAT = "surrounding_context_window"

    def __init__(self, window_length: int, window_overlap: int, origin: tuple, workspace: tuple, windows: int):
        super().__init__(window_length, window_overlap, origin, workspace)
        self.windows = windows

    def get_chunks(self, docs: list, encoding, index_metadata) -> list:
        """Get nodes from documents

        :param docs: list of documents
        :param encoding: encoding to use

        :return: list of nodes
        """
        nodes_per_doc = []
        for doc in docs:
            # If not added, does not appear in inforetrieval (value substituted by llama_index in indexation)
            doc.metadata.setdefault('document_id', str(uuid.uuid4()))
            # Exclude when splitting (chunk size dependent on the metadata)
            doc.excluded_llm_metadata_keys = list(doc.metadata.keys())
            doc.excluded_embed_metadata_keys = list(doc.metadata.keys())

            sentence_splitter = SentenceSplitter(chunk_size=self.window_length, chunk_overlap=self.window_overlap,
                                                 tokenizer=encoding.encode, paragraph_separator="\\n\\n")
            nodes = SentenceWindowNodeParser.from_defaults(sentence_splitter=sentence_splitter.split_text,
                                                           window_size=self.windows, window_metadata_key="window",
                                                           original_text_metadata_key="original_text").get_nodes_from_documents([doc], show_progress=True)

            if eval(os.getenv('TESTING', "False")):
                final_nodes = self._add_nodes_metadata(nodes, self.origin, index_metadata)
            else:
                final_nodes = self._add_nodes_metadata(nodes, self.workspace, index_metadata)
            nodes_per_doc.append(final_nodes)
        return nodes_per_doc

class ManagerChunkingMethods(object):
    CHUNKING_METHODS = [Simple, Recursive, SurroundingContextWindow]

    @staticmethod
    def get_chunking_method(conf: dict) -> ChunkingMethod:
        """ Method to instantiate the parsers class: [Simple, Recursive, SurroundingContextWindow]

        :param conf: Chunking method configuration. Example:  {"type":"simple", ...}
        """
        for chunking_method in ManagerChunkingMethods.CHUNKING_METHODS:
            chunking_method_type = conf.get('method')
            if chunking_method.is_method_type(chunking_method_type):
                conf.pop('method')
                return chunking_method(**conf)
        raise PrintableGenaiError(400, f"Chunking method type doesnt exist {conf}. "
                         f"Possible values: {ManagerChunkingMethods.get_possible_chunking_methods()}")

    @staticmethod
    def get_possible_chunking_methods() -> List:
        """ Method to list the methods types: [Simple, Recursive, SurroundingContextWindow]"""
        return [cm.CHUNKING_FORMAT for cm in ManagerChunkingMethods.CHUNKING_METHODS]
