### This code is property of the GGAO ###

import os
import pytest
from unittest.mock import patch, MagicMock, call
from pdfminer.pdfparser import PDFSyntaxError
from pdfminer.pdfdocument import PDFEncryptionError

from main import PreprocessExtractDeployment


# Setup constants
PREPROCESS_END_SERVICE = "preprocess_end"
PREPROCESS_OCR_SERVICE = "preprocess_ocr"
PREPROCESS_LAYOUT_SERVICE = "preprocess_layout"
PREPROCESS_SEGMENTATION_SERVICE = "preprocess_segmentation"
PREPROCESS_TRANSLATION_SERVICE = "preprocess_translation"
EXTRACTED_DOCUMENT = "extracted_document"
ERROR = "error"

class TestPreprocessExtractDeployment:
    @pytest.fixture(autouse=True)
    def setup(self):
        """Fixture for setting up deployment instance"""
        self.deployment = PreprocessExtractDeployment()

    def test_init_sets_proper_attributes(self):
        """Test if the init method correctly sets up attributes"""
        assert isinstance(self.deployment, PreprocessExtractDeployment)

    def test_service_name(self):
        """Test service name is correctly returned"""
        assert self.deployment.service_name == "preprocess_extract"

    def test_max_num_queue(self):
        """Test max_num_queue returns 1"""
        assert self.deployment.max_num_queue == 1

    @patch("main.Process")
    @patch("main.update_status")
    @patch("main.upload_object")
    @patch("main.download_file")
    @patch("main.get_num_pages")
    @patch("main.extract_text")
    @patch("os.remove")
    @patch("main.remove_local_files")
    def test_process_successful_path(
        self,
        mock_remove_local_files,
        mock_os_remove,
        mock_extract_text,
        mock_get_num_pages,
        mock_download_file,
        mock_upload_object,
        mock_update_status,
        mock_process,
    ):
        """Test the process method happy path where text extraction is successful"""
        json_input = {
            "generic": {"preprocess_conf": {"page_limit": 10, "num_pag_ini": 1}},
            "specific": {
                "path_txt": "/txt",
                "path_cells": "/cells",
                "path_text": "/text",
                "document": {},
            },
            "document": {"filename": "example.pdf"},
        }

        mock_get_num_pages.return_value = 5
        mock_extract_text.return_value = {
            "lang": "en",
            "text": "Extracted text",
            "extraction": {},
            "boxes": [],
            "cells": [],
            "lines": [],
        }
        mock_proc = MagicMock()
        mock_proc.start = MagicMock()
        mock_proc.join = MagicMock()
        mock_proc.is_alive = MagicMock()
        mock_proc.terminate = MagicMock()
        mock_process.return_value = mock_proc
        mock_proc.is_alive.return_value = True

        with patch("main.get_generic", return_value=json_input["generic"]), patch(
            "main.get_specific", return_value=json_input["specific"]
        ), patch("main.get_document", return_value={"filename": "example.pdf"}), patch(
            "main.get_project_type", return_value="text"
        ), patch("main.get_force_ocr", return_value=False), patch(
            "main.get_languages", return_value=["en"]
        ), patch("main.get_do_cells_text", return_value=False), patch(
            "main.get_do_lines_text", return_value=False
        ), patch("main.get_do_segments", return_value=False):
            must_continue, message, next_service = self.deployment.process(json_input)

            assert must_continue is True
            assert next_service == PREPROCESS_END_SERVICE

    @patch("main.update_status")
    @patch("main.download_file")
    @patch("main.get_generic", side_effect=KeyError)
    def test_process_keyerror_exception(
        self, mock_get_generic, mock_download_file, mock_update_status
    ):
        """Test process method for KeyError exception during JSON parsing"""
        json_input = {"generic": {}, "specific": {}, "document": {}}

        self.deployment.process(json_input)
        must_continue, message, next_service = self.deployment.process(json_input)

        assert must_continue is True
        assert next_service == PREPROCESS_END_SERVICE

    @patch("main.download_file", side_effect=Exception("Download error"))
    @patch("main.update_status")
    def test_process_download_error(self, mock_update_status, mock_download_file):
        """Test process method for Exception during file download"""
        json_input = {"generic": {}, "specific": {}, "document": {}}

        must_continue, message, next_service = self.deployment.process(json_input)

        assert must_continue is True
        assert next_service == PREPROCESS_END_SERVICE

    @patch("main.download_file")
    @patch("main.get_num_pages", side_effect=PDFSyntaxError("PDF Error"))
    @patch("main.update_status")
    def test_process_pdf_syntax_error(
        self, mock_update_status, mock_get_num_pages, mock_download_file
    ):
        """Test process method handles PDFSyntaxError gracefully"""
        json_input = {
            "generic": {"preprocess_conf": {"page_limit": 10}},
            "specific": {
                "path_txt": "/txt",
                "path_cells": "/cells",
                "path_text": "/text",
            },
            "document": {"filename": "example.pdf"},
        }

        must_continue, message, next_service = self.deployment.process(json_input)

        assert must_continue is True
        assert next_service == PREPROCESS_END_SERVICE

    @patch("main.download_file")
    @patch("main.get_num_pages", side_effect=PDFEncryptionError("PDF Encrypted"))
    @patch("main.update_status")
    def test_process_pdf_encryption_error(
        self, mock_update_status, mock_get_num_pages, mock_download_file
    ):
        """Test process method handles PDFEncryptionError gracefully"""
        json_input = {
            "generic": {"preprocess_conf": {"page_limit": 10}},
            "specific": {
                "path_txt": "/txt",
                "path_cells": "/cells",
                "path_text": "/text",
            },
            "document": {"filename": "example.pdf"},
        }

        must_continue, message, next_service = self.deployment.process(json_input)

        assert must_continue is True
        assert next_service == PREPROCESS_END_SERVICE

    @patch("main.download_file")
    @patch("main.get_num_pages", side_effect=Exception("Unknown Error"))
    @patch("main.update_status")
    def test_process_generic_exception(
        self, mock_update_status, mock_get_num_pages, mock_download_file
    ):
        """Test process method handles generic exceptions gracefully"""
        json_input = {
            "generic": {"preprocess_conf": {"page_limit": 10}},
            "specific": {
                "path_txt": "/txt",
                "path_cells": "/cells",
                "path_text": "/text",
            },
            "document": {"filename": "example.pdf"},
        }

        must_continue, message, next_service = self.deployment.process(json_input)

        assert must_continue is True
        assert next_service == PREPROCESS_END_SERVICE

    @patch("main.Process")
    @patch("main.update_status")
    @patch("main.upload_object")
    @patch("main.download_file")
    @patch("main.get_num_pages", side_effect=Exception("Unknown Error"))
    @patch("main.extract_text")
    @patch("os.remove")
    @patch("main.remove_local_files")
    def test_pdf_syntax_error_2(
        self,
        mock_remove_local_files,
        mock_os_remove,
        mock_extract_text,
        mock_get_num_pages,
        mock_download_file,
        mock_upload_object,
        mock_update_status,
        mock_process,
    ):
        """Test the process method happy path where text extraction is successful"""
        json_input = {
            "generic": {"preprocess_conf": {"page_limit": 10, "num_pag_ini": 1}},
            "specific": {
                "path_txt": "/txt",
                "path_cells": "/cells",
                "path_text": "/text",
                "document": {},
            },
            "document": {"filename": "example.pdf"},
        }

        mock_get_num_pages.return_value = 5
        mock_extract_text.return_value = {
            "lang": "en",
            "text": "Extracted text",
            "extraction": {},
            "boxes": [],
            "cells": [],
            "lines": [],
        }
        mock_proc = MagicMock()
        mock_proc.start = MagicMock()
        mock_proc.join = MagicMock()
        mock_proc.is_alive = MagicMock()
        mock_proc.terminate = MagicMock()
        mock_process.return_value = mock_proc
        mock_proc.is_alive.return_value = True

        with patch("main.get_generic", return_value=json_input["generic"]), patch(
            "main.get_specific", return_value=json_input["specific"]
        ), patch("main.get_document", return_value={"filename": "example.pdf"}), patch(
            "main.get_project_type", return_value="text"
        ), patch("main.get_force_ocr", return_value=False), patch(
            "main.get_languages", return_value=["en"]
        ), patch("main.get_do_cells_text", return_value=False), patch(
            "main.get_do_lines_text", return_value=False
        ), patch("main.get_do_segments", return_value=False):
            must_continue, message, next_service = self.deployment.process(json_input)

            assert must_continue is True
            assert next_service == PREPROCESS_END_SERVICE

    @patch("main.update_status")
    def test_process_keyerror_storage(self, mock_update_status):
        """Test process method for Exception during file download"""
        json_input = {"generic": {}, "specific": {}, "document": {}}

        with patch.dict(
            "main.storage_containers", side_effect=Exception("Storage container error")
        ) as mock_storage_containers:
            mock_storage_containers.clear()
            must_continue, message, next_service = self.deployment.process(json_input)

            assert must_continue is True
            assert next_service == PREPROCESS_END_SERVICE

    @patch("main.get_document")
    @patch("main.update_status")
    def test_get_project_type_error(self, mock_update_status, mock_get_document):
        """Test process method for Exception during file download"""
        json_input = {
            "generic": {},
            "specific": {},
            "document": {"filename": "example.pdf"},
        }

        with patch(
            "main.get_project_type", side_effect=KeyError("Storage container error")
        ) as mock_storage_containers:
            mock_storage_containers.clear()
            must_continue, message, next_service = self.deployment.process(json_input)

            assert must_continue is True
            assert next_service == PREPROCESS_END_SERVICE

    @patch("main.get_project_type")
    @patch("main.get_document")
    @patch("main.update_status")
    def test_get_force_error(
        self, mock_update_status, mock_get_document, mock_get_project_type
    ):
        """Test process method for Exception during file download"""
        json_input = {
            "generic": {},
            "specific": {},
            "document": {"filename": "example.pdf"},
        }

        with patch(
            "main.get_force_ocr", side_effect=KeyError("Storage container error")
        ) as mock_storage_containers:
            mock_storage_containers.clear()
            must_continue, message, next_service = self.deployment.process(json_input)

            assert must_continue is True
            assert next_service == PREPROCESS_END_SERVICE

    @patch("main.get_force_ocr")
    @patch("main.get_project_type")
    @patch("main.get_document")
    @patch("main.update_status")
    def test_get_languages(
        self,
        mock_update_status,
        mock_get_document,
        mock_get_project_type,
        mock_get_force_ocr,
    ):
        """Test process method for Exception during file download"""
        json_input = {
            "generic": {},
            "specific": {},
            "document": {"filename": "example.pdf"},
        }

        with patch(
            "main.get_languages", side_effect=KeyError("Storage container error")
        ) as mock_storage_containers:
            mock_storage_containers.clear()
            must_continue, message, next_service = self.deployment.process(json_input)

            assert must_continue is True
            assert next_service == PREPROCESS_END_SERVICE

    @patch("main.get_languages")
    @patch("main.get_force_ocr")
    @patch("main.get_project_type")
    @patch("main.get_document")
    @patch("main.update_status")
    def test_get_cells_error(
        self,
        mock_update_status,
        mock_get_document,
        mock_get_project_type,
        mock_get_force_ocr,
        mock_get_languages,
    ):
        """Test process method for Exception during file download"""
        json_input = {
            "generic": {},
            "specific": {},
            "document": {"filename": "example.pdf"},
        }

        with patch(
            "main.get_do_cells_text", side_effect=KeyError("Storage container error")
        ) as mock_storage_containers:
            mock_storage_containers.clear()
            must_continue, message, next_service = self.deployment.process(json_input)

            assert must_continue is True
            assert next_service == PREPROCESS_END_SERVICE

    @patch("main.get_do_cells_text")
    @patch("main.get_languages")
    @patch("main.get_force_ocr")
    @patch("main.get_project_type")
    @patch("main.get_document")
    @patch("main.update_status")
    def test_get_lines_error(
        self,
        mock_update_status,
        mock_get_document,
        mock_get_project_type,
        mock_get_force_ocr,
        mock_get_languages,
        mock_get_do_cells_text,
    ):
        """Test process method for Exception during file download"""
        json_input = {
            "generic": {},
            "specific": {},
            "document": {"filename": "example.pdf"},
        }

        with patch(
            "main.get_do_lines_text", side_effect=KeyError("Storage container error")
        ) as mock_storage_containers:
            mock_storage_containers.clear()
            must_continue, message, next_service = self.deployment.process(json_input)

            assert must_continue is True
            assert next_service == PREPROCESS_END_SERVICE

    @patch("main.get_do_lines_text")
    @patch("main.get_do_cells_text")
    @patch("main.get_languages")
    @patch("main.get_force_ocr")
    @patch("main.get_project_type")
    @patch("main.get_document")
    @patch("main.update_status")
    def test_get_segments_error(
        self,
        mock_update_status,
        mock_get_document,
        mock_get_project_type,
        mock_get_force_ocr,
        mock_get_languages,
        mock_get_do_cells_text,
        mock_get_do_lines_text,
    ):
        """Test process method for Exception during file download"""
        json_input = {
            "generic": {},
            "specific": {},
            "document": {"filename": "example.pdf"},
        }

        with patch(
            "main.get_do_segments", side_effect=KeyError("Storage container error")
        ) as mock_storage_containers:
            mock_storage_containers.clear()
            must_continue, message, next_service = self.deployment.process(json_input)

            assert must_continue is True
            assert next_service == PREPROCESS_END_SERVICE

    @patch("main.get_do_segments")
    @patch("main.get_do_lines_text")
    @patch("main.get_do_cells_text")
    @patch("main.get_languages")
    @patch("main.get_force_ocr")
    @patch("main.get_project_type")
    @patch("main.get_document")
    @patch("main.update_status")
    def test_download_file_error(
        self,
        mock_update_status,
        mock_get_document,
        mock_get_project_type,
        mock_get_force_ocr,
        mock_get_languages,
        mock_get_do_cells_text,
        mock_get_do_lines_text,
        mock_get_do_segments,
    ):
        """Test process method for Exception during file download"""
        json_input = {
            "generic": {},
            "specific": {},
            "document": {"filename": "example.pdf"},
        }

        with patch(
            "main.download_file", side_effect=KeyError("Storage container error")
        ) as mock_storage_containers:
            must_continue, message, next_service = self.deployment.process(json_input)

            assert must_continue is True
            assert next_service == PREPROCESS_END_SERVICE

    @patch("main.get_num_pages")
    @patch("main.download_file")
    @patch("main.get_do_segments")
    @patch("main.get_do_lines_text")
    @patch("main.get_do_cells_text")
    @patch("main.get_languages")
    @patch("main.get_force_ocr")
    @patch("main.get_project_type")
    @patch("main.get_document")
    @patch("main.update_status")
    def test_get_num_error_openfile(
        self,
        mock_update_status,
        mock_get_document,
        mock_get_project_type,
        mock_get_force_ocr,
        mock_get_languages,
        mock_get_do_cells_text,
        mock_get_do_lines_text,
        mock_get_do_segments,
        mock_download_file,
        mock_get_num_pages,
    ):
        """Test process method for Exception during file download"""
        json_input = {
            "generic": {"preprocess_conf": {"page_limit": {}}},
            "specific": {},
            "document": {"filename": "example.pdf"},
        }
        mock_get_num_pages.side_effect = PDFSyntaxError
        with patch("builtins.open") as mock_file:
            must_continue, _, next_service = self.deployment.process(json_input)

            assert must_continue is True
            assert next_service == PREPROCESS_END_SERVICE

    @patch("main.get_num_pages")
    @patch("main.download_file")
    @patch("main.get_do_segments")
    @patch("main.get_do_lines_text")
    @patch("main.get_do_cells_text")
    @patch("main.get_languages")
    @patch("main.get_force_ocr")
    @patch("main.get_project_type")
    @patch("main.get_document")
    @patch("main.update_status")
    def test_get_num_error_encryptionfile(
        self,
        mock_update_status,
        mock_get_document,
        mock_get_project_type,
        mock_get_force_ocr,
        mock_get_languages,
        mock_get_do_cells_text,
        mock_get_do_lines_text,
        mock_get_do_segments,
        mock_download_file,
        mock_get_num_pages,
    ):
        """Test process method for Exception during file download"""
        json_input = {
            "generic": {"preprocess_conf": {"page_limit": {}}},
            "specific": {},
            "document": {"filename": "example.pdf"},
        }
        mock_get_num_pages.side_effect = PDFEncryptionError
        with patch("builtins.open") as mock_file:
            must_continue, _, next_service = self.deployment.process(json_input)

            assert must_continue is True
            assert next_service == PREPROCESS_END_SERVICE

    @patch("main.Process")
    @patch("main.get_num_pages")
    @patch("main.download_file")
    @patch("main.get_do_segments")
    @patch("main.get_do_lines_text")
    @patch("main.get_do_cells_text")
    @patch("main.get_languages")
    @patch("main.get_force_ocr")
    @patch("main.get_project_type")
    @patch("main.get_document")
    @patch("main.update_status")
    def test_terminate_exception(
        self,
        mock_update_status,
        mock_get_document,
        mock_get_project_type,
        mock_get_force_ocr,
        mock_get_languages,
        mock_get_do_cells_text,
        mock_get_do_lines_text,
        mock_get_do_segments,
        mock_download_file,
        mock_get_num_pages,
        mock_process,
    ):
        """Test process method for Exception during file download"""
        json_input = {
            "generic": {"preprocess_conf": {"page_limit": {}}},
            "specific": {},
            "document": {"filename": "example.pdf"},
        }
        mock_proc = MagicMock()
        mock_proc.start = MagicMock()
        mock_proc.join = MagicMock()
        mock_proc.is_alive = MagicMock()
        mock_proc.terminate = MagicMock()
        mock_proc.terminate.side_effect = Exception("Error")
        mock_process.return_value = mock_proc
        mock_proc.is_alive.return_value = True
        must_continue, _, next_service = self.deployment.process(json_input)

        assert must_continue is True
        assert next_service == PREPROCESS_END_SERVICE

    @patch("main.Process")
    @patch("main.get_num_pages", return_value=5)
    @patch("main.download_file")
    @patch("main.get_do_segments")
    @patch("main.get_do_lines_text", return_value=True)
    @patch("main.get_do_cells_text")
    @patch("main.get_languages")
    @patch("main.get_force_ocr", return_value=True)
    @patch("main.get_project_type", return_value="text")
    @patch("main.get_document")
    @patch("main.update_status")
    def test_files_extracted(
        self,
        mock_update_status,
        mock_get_document,
        mock_get_project_type,
        mock_get_force_ocr,
        mock_get_languages,
        mock_get_do_cells_text,
        mock_get_do_lines_text,
        mock_get_do_segments,
        mock_download_file,
        mock_get_num_pages,
        mock_process,
        monkeypatch,
    ):
        """Test process method for Exception during file download"""
        json_input = {
            "generic": {"preprocess_conf": {"page_limit": {}}},
            "specific": {"path_cells": {}},
            "document": {"filename": "example.pdf"},
        }
        mock_proc = MagicMock()
        mock_proc.start = MagicMock()
        mock_proc.join = MagicMock()
        mock_proc.is_alive = MagicMock()
        mock_proc.terminate = MagicMock()
        mock_process.return_value = mock_proc
        mock_proc.is_alive.return_value = True

        def mockreturn(path):
            return "example_path"

        monkeypatch.setattr(os.path, "join", mockreturn)

        must_continue, _, next_service = self.deployment.process(json_input)

        assert must_continue is True
        assert next_service == PREPROCESS_END_SERVICE

    @patch("main.Process")
    @patch("main.get_num_pages", return_value=101)
    @patch("main.download_file")
    @patch("main.get_do_segments")
    @patch("main.get_do_lines_text", return_value=True)
    @patch("main.get_do_cells_text")
    @patch("main.get_languages")
    @patch("main.get_force_ocr", return_value=True)
    @patch("main.get_project_type", return_value="text")
    @patch("main.get_document")
    @patch("main.update_status")
    def test_files_extracted_100(
        self,
        mock_update_status,
        mock_get_document,
        mock_get_project_type,
        mock_get_force_ocr,
        mock_get_languages,
        mock_get_do_cells_text,
        mock_get_do_lines_text,
        mock_get_do_segments,
        mock_download_file,
        mock_get_num_pages,
        mock_process,
        monkeypatch,
    ):
        """Test process method for Exception during file download"""
        json_input = {
            "generic": {"preprocess_conf": {"page_limit": {}}},
            "specific": {"path_cells": {}},
            "document": {"filename": "example.pdf"},
        }
        mock_proc = MagicMock()
        mock_proc.start = MagicMock()
        mock_proc.join = MagicMock()
        mock_proc.is_alive = MagicMock()
        mock_proc.terminate = MagicMock()
        mock_process.return_value = mock_proc
        mock_proc.is_alive.return_value = True

        def mockreturn(path):
            return "example_path"

        monkeypatch.setattr(os.path, "join", mockreturn)

        must_continue, _, next_service = self.deployment.process(json_input)

        assert must_continue is True
        assert next_service == PREPROCESS_END_SERVICE

    @patch("main.remove_local_files", side_effect=Exception("Error"))
    @patch("os.remove")
    @patch("main.Process")
    @patch("main.get_num_pages", return_value=101)
    @patch("main.download_file")
    @patch("main.get_do_segments")
    @patch("main.get_do_lines_text", return_value=True)
    @patch("main.get_do_cells_text")
    @patch("main.get_languages")
    @patch("main.get_force_ocr", return_value=True)
    @patch("main.get_project_type", return_value="text")
    @patch("main.get_document")
    @patch("main.update_status")
    def test_files_extracted_remove_exception(
        self,
        mock_update_status,
        mock_get_document,
        mock_get_project_type,
        mock_get_force_ocr,
        mock_get_languages,
        mock_get_do_cells_text,
        mock_get_do_lines_text,
        mock_get_do_segments,
        mock_download_file,
        mock_get_num_pages,
        mock_process,
        mock_remove,
        mock_remove_local_files,
        monkeypatch,
    ):
        """Test process method for Exception during file download"""
        json_input = {
            "generic": {"preprocess_conf": {"page_limit": {}}},
            "specific": {"path_cells": {}, 'document': {}},
            "document": {"filename": "example.pdf"},
        }
        mock_proc = MagicMock()
        mock_proc.start = MagicMock()
        mock_proc.join = MagicMock()
        mock_proc.is_alive = MagicMock()
        mock_proc.terminate = MagicMock()
        mock_process.return_value = mock_proc
        mock_proc.is_alive.return_value = True

        def mockreturn(path):
            return "example_path"

        monkeypatch.setattr(os.path, "join", mockreturn)

        must_continue, _, next_service = self.deployment.process(json_input)

        assert must_continue is True
        assert next_service == PREPROCESS_END_SERVICE

    @patch("main.remove_local_files")
    @patch("os.remove")
    @patch("main.Process")
    @patch("main.get_num_pages", return_value=101)
    @patch("main.download_file")
    @patch("main.get_do_segments")
    @patch("main.get_do_lines_text", return_value=True)
    @patch("main.get_do_cells_text")
    @patch("main.get_languages")
    @patch("main.get_force_ocr", return_value=False)
    @patch("main.get_project_type", return_value="text")
    @patch("main.get_document")
    @patch("main.update_status")
    @patch("main.extract_text")
    @patch("main.extract_images_conditional", return_value=(["",""]))
    def test_to_end(
        self,
        mock_extract_images_conditional,
        mock_extract_text,
        mock_update_status,
        mock_get_document,
        mock_get_project_type,
        mock_get_force_ocr,
        mock_get_languages,
        mock_get_do_cells_text,
        mock_get_do_lines_text,
        mock_get_do_segments,
        mock_download_file,
        mock_get_num_pages,
        mock_process,
        mock_remove,
        mock_remove_local_files,
        monkeypatch,
    ):
        """Test process method for Exception during file download"""
        json_input = {
            "generic": {"preprocess_conf": {"page_limit": {}}},
            "specific": {"path_cells": {}, 'document': {}},
            "document": {"filename": "example.pdf"},
        }
        mock_proc = MagicMock()
        mock_proc.start = MagicMock()
        mock_proc.join = MagicMock()
        mock_proc.is_alive = MagicMock()
        mock_proc.terminate = MagicMock()
        mock_process.return_value = mock_proc
        mock_proc.is_alive.return_value = True
    
        mock_return_dict = {
            'lang': "en", 
            'text': "Some extracted text", 
            'extraction': {}, 
            'boxes': [], 
            'cells': [], 
            'lines': []
        }

        mock_extract_text.return_value = mock_return_dict
        monkeypatch.setattr(os.path, "join", lambda x, y: "example_path")

        must_continue, _, next_service = self.deployment.process(json_input)

        assert must_continue is True
        assert next_service == "preprocess_ocr"
    

    @patch("main.remove_local_files")
    @patch("os.remove")
    @patch("main.Process")
    @patch("main.get_num_pages", return_value=101)
    @patch("main.download_file")
    @patch("main.get_do_segments")
    @patch("main.get_do_lines_text", return_value=True)
    @patch("main.get_do_cells_text")
    @patch("main.get_languages")
    @patch("main.get_force_ocr", return_value=False)
    @patch("main.get_project_type", return_value="otro")
    @patch("main.get_document")
    @patch("main.update_status")
    @patch("main.extract_text", return_value="Some extracted text")
    @patch("main.extract_images_conditional", return_value=(["",""]))
    def test_to_end_img_extracted(
        self,
        mock_extract_images_conditional,
        mock_extract_text,
        mock_update_status,
        mock_get_document,
        mock_get_project_type,
        mock_get_force_ocr,
        mock_get_languages,
        mock_get_do_cells_text,
        mock_get_do_lines_text,
        mock_get_do_segments,
        mock_download_file,
        mock_get_num_pages,
        mock_process,
        mock_remove,
        mock_remove_local_files,
        monkeypatch,
    ):
        """Test process method for Exception during file download"""
        json_input = {
            "generic": {"preprocess_conf": {"page_limit": {}}},
            "specific": {"path_cells": {}, 'document': {}},
            "document": {"filename": "example.pdf"},
        }
        mock_proc = MagicMock()
        mock_proc.start = MagicMock()
        mock_proc.join = MagicMock()
        mock_proc.is_alive = MagicMock()
        mock_proc.terminate = MagicMock()
        mock_process.return_value = mock_proc
        mock_proc.is_alive.return_value = True
    
        mock_return_dict = {
            'lang': "en", 
            'text': "Some extracted text", 
            'extraction': {"text": "Some extracted text"}, 
            'boxes': [], 
            'cells': [], 
            'lines': []
        }

        mock_extract_text.return_value = mock_return_dict
        monkeypatch.setattr(os.path, "join", lambda x, y: "example_path")

        must_continue, _, next_service = self.deployment.process(json_input)

        assert must_continue is True
        assert next_service == "preprocess_ocr"

    @patch("main.remove_local_files")
    @patch("os.remove")
    @patch("main.Process")
    @patch("main.get_num_pages", return_value=2)
    @patch("main.download_file")
    @patch("main.get_do_segments")
    @patch("main.get_do_lines_text", return_value=False)
    @patch("main.get_do_cells_text")
    @patch("main.get_languages")
    @patch("main.get_force_ocr", return_value=False)
    @patch("main.get_project_type", return_value="otro")
    @patch("main.get_document")
    @patch("main.update_status")
    @patch("main.extract_text", return_value="Some extracted text")
    @patch("main.extract_images_conditional", return_value=(["",""]))
    def test_numpags_force_ocr_do_lines_text(
        self,
        mock_extract_images_conditional,
        mock_extract_text,
        mock_update_status,
        mock_get_document,
        mock_get_project_type,
        mock_get_force_ocr,
        mock_get_languages,
        mock_get_do_cells_text,
        mock_get_do_lines_text,
        mock_get_do_segments,
        mock_download_file,
        mock_get_num_pages,
        mock_process,
        mock_remove,
        mock_remove_local_files,
        monkeypatch,
    ):
        """Test process method for Exception during file download"""
        json_input = {
            "generic": {"preprocess_conf": {"page_limit": {}}},
            "specific": {"path_cells": {}, 'document': {}},
            "document": {"filename": "example.pdf"},
        }
        mock_proc = MagicMock()
        mock_proc.start = MagicMock()
        mock_proc.join = MagicMock()
        mock_proc.is_alive = MagicMock()
        mock_proc.terminate = MagicMock()
        mock_process.return_value = mock_proc
        mock_proc.is_alive.return_value = True
    
        mock_return_dict = {
            'lang': "en", 
            'text': "Some extracted text", 
            'extraction': {"text": "Some extracted text"}, 
            'boxes': [], 
            'cells': [], 
            'lines': []
        }

        mock_extract_text.return_value = mock_return_dict
        monkeypatch.setattr(os.path, "join", lambda x, y: "example_path")

        must_continue, _, next_service = self.deployment.process(json_input)

        assert must_continue is True
        assert next_service == "preprocess_ocr"
