import unittest
from unittest.mock import patch, MagicMock
import os
import pytest
from requests.auth import HTTPBasicAuth

# Import the class to test
from langfuse_manager import LangFuseManager


class TestLangFuseManager(unittest.TestCase):
    """Test suite for LangFuseManager class."""

    def setUp(self):
        """Set up test fixtures."""
        # Clear environment variables before each test
        if "LANGFUSE" in os.environ:
            del os.environ["LANGFUSE"]
        if "LANGFUSE_SECRET_KEY" in os.environ:
            del os.environ["LANGFUSE_SECRET_KEY"]
        if "LANGFUSE_PUBLIC_KEY" in os.environ:
            del os.environ["LANGFUSE_PUBLIC_KEY"]
        if "LANGFUSE_HOST" in os.environ:
            del os.environ["LANGFUSE_HOST"]

    @patch('langfuse_manager.Langfuse')
    def test_init_with_env_vars(self, mock_langfuse):
        """Test initialization with environment variables."""
        # Set environment variables
        os.environ["LANGFUSE"] = "True"
        os.environ["LANGFUSE_SECRET_KEY"] = "test_secret_key"
        os.environ["LANGFUSE_PUBLIC_KEY"] = "test_public_key"
        os.environ["LANGFUSE_HOST"] = "test_host"

        # Create an instance
        manager = LangFuseManager()

        # Check if Langfuse was initialized with correct parameters
        mock_langfuse.assert_called_once_with(
            secret_key="test_secret_key",
            public_key="test_public_key",
            host="test_host"
        )
        self.assertIsNotNone(manager.langfuse)
        self.assertEqual(manager.langfuse_config, {
            "secret_key": "test_secret_key",
            "public_key": "test_public_key",
            "host": "test_host"
        })

    def test_init_without_env_vars(self):
        """Test initialization without environment variables."""
        manager = LangFuseManager()
        self.assertIsNone(manager.langfuse)

    @patch('langfuse_manager.Langfuse')
    def test_parse_with_langfuse_config(self, mock_langfuse):
        """Test parse method with langfuse config."""
        manager = LangFuseManager()
        compose_config = {
            "langfuse": {
                "secret_key": "compose_secret_key",
                "public_key": "compose_public_key",
                "host": "compose_host"
            }
        }

        result = manager.parse(compose_config)

        # Check if Langfuse was initialized with correct parameters
        mock_langfuse.assert_called_once_with(
            secret_key="compose_secret_key",
            public_key="compose_public_key",
            host="compose_host"
        )
        self.assertEqual(result, manager)
        self.assertIsNotNone(manager.langfuse)

    @patch('langfuse_manager.Langfuse')
    def test_parse_with_existing_langfuse(self, mock_langfuse):
        """Test parse method when langfuse is already initialized."""
        # Initialize with environment variables
        os.environ["LANGFUSE"] = "True"
        os.environ["LANGFUSE_SECRET_KEY"] = "test_secret_key"
        os.environ["LANGFUSE_PUBLIC_KEY"] = "test_public_key"
        os.environ["LANGFUSE_HOST"] = "test_host"

        manager = LangFuseManager()
        mock_langfuse.reset_mock()  # Reset the mock after initialization

        # Call parse with some config
        compose_config = {"langfuse": {"some": "config"}}
        result = manager.parse(compose_config)

        # Check that Langfuse wasn't initialized again
        mock_langfuse.assert_not_called()
        self.assertEqual(result, manager)

    @patch('langfuse_manager.Langfuse')
    def test_parse_without_langfuse_config(self, mock_langfuse):
        """Test parse method without langfuse config."""
        manager = LangFuseManager()
        compose_config = {"some": "config"}

        result = manager.parse(compose_config)

        # Check that Langfuse wasn't initialized
        mock_langfuse.assert_not_called()
        self.assertEqual(result, manager)
        self.assertIsNone(manager.langfuse)

    @patch('langfuse_manager.Langfuse')
    def test_create_trace(self, mock_langfuse):
        """Test create_trace method."""
        # Setup mock
        os.environ["LANGFUSE"] = "True"
        manager = LangFuseManager()
        mock_trace = MagicMock()
        manager.langfuse.trace.return_value = mock_trace

        # Call method
        manager.create_trace("test_session_id")

        # Check if trace was created with correct parameters
        manager.langfuse.trace.assert_called_once_with(session_id="test_session_id")
        self.assertEqual(manager.trace, mock_trace)

    def test_update_metadata_without_langfuse(self):
        """Test update_metadata method without langfuse initialized."""
        manager = LangFuseManager()
        # This should not raise an exception
        manager.update_metadata({"test": "metadata"})

    @patch('langfuse_manager.Langfuse')
    def test_update_metadata_with_langfuse(self, mock_langfuse):
        """Test update_metadata method with langfuse initialized."""
        os.environ["LANGFUSE"] = "True"
        manager = LangFuseManager()
        manager.trace = MagicMock()

        manager.update_metadata({"test": "metadata"})

        manager.trace.update.assert_called_once_with(metadata={"test": "metadata"})

    def test_update_input_without_langfuse(self):
        """Test update_input method without langfuse initialized."""
        manager = LangFuseManager()
        # This should not raise an exception
        manager.update_input({"test": "input"})

    @patch('langfuse_manager.Langfuse')
    def test_update_input_with_langfuse(self, mock_langfuse):
        """Test update_input method with langfuse initialized."""
        os.environ["LANGFUSE"] = "True"
        manager = LangFuseManager()
        manager.trace = MagicMock()

        manager.update_input({"test": "input"})

        manager.trace.update.assert_called_once_with(input={"test": "input"})

    def test_update_output_without_langfuse(self):
        """Test update_output method without langfuse initialized."""
        manager = LangFuseManager()
        # This should not raise an exception
        manager.update_output({"test": "output"})

    @patch('langfuse_manager.Langfuse')
    def test_update_output_with_langfuse(self, mock_langfuse):
        """Test update_output method with langfuse initialized."""
        os.environ["LANGFUSE"] = "True"
        manager = LangFuseManager()
        manager.trace = MagicMock()

        manager.update_output({"test": "output"})

        manager.trace.update.assert_called_once_with(output={"test": "output"})

    def test_add_span_without_langfuse(self):
        """Test add_span method without langfuse initialized."""
        manager = LangFuseManager()
        # This should not raise an exception and return None
        result = manager.add_span("test_name", {"test": "metadata"}, {"test": "input"})
        self.assertIsNone(result)

    @patch('langfuse_manager.Langfuse')
    def test_add_span_with_langfuse(self, mock_langfuse):
        """Test add_span method with langfuse initialized."""
        os.environ["LANGFUSE"] = "True"
        manager = LangFuseManager()
        manager.trace = MagicMock()
        mock_span = MagicMock()
        manager.trace.span.return_value = mock_span

        result = manager.add_span("test_name", {"test": "metadata"}, {"test": "input"})

        manager.trace.span.assert_called_once_with(
            name="test_name",
            metadata={"test": "metadata"},
            input={"test": "input"}
        )
        self.assertEqual(result, mock_span)

    def test_add_span_output_without_langfuse(self):
        """Test add_span_output method without langfuse initialized."""
        manager = LangFuseManager()
        manager.add_span_output(MagicMock(), {"test": "output"})

    @patch('langfuse_manager.Langfuse')
    def test_add_span_output_with_langfuse(self, mock_langfuse):
        """Test add_span_output method with langfuse initialized."""
        os.environ["LANGFUSE"] = "True"
        manager = LangFuseManager()
        mock_span = MagicMock()

        manager.add_span_output(mock_span, {"test": "output"})

        mock_span.end.assert_called_once_with(output={"test": "output"})

    def test_add_generation_without_langfuse(self):
        """Test add_generation method without langfuse initialized."""
        manager = LangFuseManager()
        result = manager.add_generation("test_name", {"test": "metadata"}, {"test": "input"}, "test_model",
                                        {"param": "value"})
        self.assertIsNone(result)

    @patch('langfuse_manager.Langfuse')
    def test_add_generation_with_langfuse(self, mock_langfuse):
        """Test add_generation method with langfuse initialized."""
        os.environ["LANGFUSE"] = "True"
        manager = LangFuseManager()
        manager.trace = MagicMock()
        mock_generation = MagicMock()
        manager.trace.generation.return_value = mock_generation

        result = manager.add_generation("test_name", {"test": "metadata"}, {"test": "input"}, "test_model",
                                        {"param": "value"})

        manager.trace.generation.assert_called_once_with(
            name="test_name",
            metadata={"test": "metadata"},
            input={"test": "input"},
            model="test_model",
            model_parameters={"param": "value"}
        )
        self.assertEqual(result, mock_generation)

    def test_add_generation_output_without_langfuse(self):
        """Test add_generation_output method without langfuse initialized."""
        manager = LangFuseManager()
        manager.add_generation_output(MagicMock(), {"test": "output"})

    @patch('langfuse_manager.Langfuse')
    def test_add_generation_output_with_langfuse(self, mock_langfuse):
        """Test add_generation_output method with langfuse initialized."""
        os.environ["LANGFUSE"] = "True"
        manager = LangFuseManager()
        mock_generation = MagicMock()

        manager.add_generation_output(mock_generation, {"test": "output"})

        mock_generation.end.assert_called_once_with(output={"test": "output"})

    def test_flush_without_langfuse(self):
        """Test flush method without langfuse initialized."""
        manager = LangFuseManager()
        manager.flush()

    @patch('langfuse_manager.Langfuse')
    def test_flush_with_langfuse(self, mock_langfuse):
        """Test flush method with langfuse initialized."""
        os.environ["LANGFUSE"] = "True"
        manager = LangFuseManager()

        manager.flush()

        if hasattr(manager.langfuse, 'flush'):
            manager.langfuse.flush.assert_not_called()

    @patch('langfuse_manager.Langfuse')
    def test_load_template(self, mock_langfuse):
        """Test load_template method."""
        os.environ["LANGFUSE"] = "True"
        manager = LangFuseManager()
        mock_prompt = MagicMock()
        manager.langfuse.get_prompt.return_value = mock_prompt

        result = manager.load_template("test_template", "custom_label")

        manager.langfuse.get_prompt.assert_called_once_with("test_template", label="custom_label")
        self.assertEqual(result, mock_prompt)

    @patch('langfuse_manager.Langfuse')
    def test_upload_template(self, mock_langfuse):
        """Test upload_template method."""
        os.environ["LANGFUSE"] = "True"
        manager = LangFuseManager()
        mock_result = MagicMock()
        manager.langfuse.create_prompt.return_value = mock_result

        result = manager.upload_template("test_template", "template content", "test_label")

        manager.langfuse.create_prompt.assert_called_once_with(
            name="test_template",
            prompt="template content",
            type="text",
            labels=["test_label", "latest"]
        )
        self.assertEqual(result, mock_result)

    @patch('langfuse_manager.requests.get')
    @patch('langfuse_manager.HTTPBasicAuth')
    @patch('langfuse_manager.Langfuse')
    def test_get_list_templates(self, mock_langfuse, mock_auth, mock_get):
        """Test get_list_templates method."""
        os.environ["LANGFUSE"] = "True"
        os.environ["LANGFUSE_SECRET_KEY"] = "test_secret_key"
        os.environ["LANGFUSE_PUBLIC_KEY"] = "test_public_key"
        os.environ["LANGFUSE_HOST"] = "test_host"

        manager = LangFuseManager()

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": [
                {"name": "template1"},
                {"name": "template2"}
            ]
        }
        mock_get.return_value = mock_response
        mock_auth.return_value = "auth_obj"

        result = manager.get_list_templates("test_label")

        mock_auth.assert_called_once_with("test_public_key", "test_secret_key")
        mock_get.assert_called_once_with(
            "test_host/api/public/v2/prompts",
            auth="auth_obj",
            params={"limit": 50, "label": "test_label"}
        )

        self.assertEqual(result, ["template1", "template2"])

    @patch('langfuse_manager.requests.get')
    @patch('langfuse_manager.Langfuse')
    def test_get_list_templates_non_200(self, mock_langfuse, mock_get):
        """Test get_list_templates method with non-200 response."""
        os.environ["LANGFUSE"] = "True"
        os.environ["LANGFUSE_SECRET_KEY"] = "test_secret_key"
        os.environ["LANGFUSE_PUBLIC_KEY"] = "test_public_key"
        os.environ["LANGFUSE_HOST"] = "test_host"

        manager = LangFuseManager()

        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        result = manager.get_list_templates("test_label")

        self.assertIsNone(result)


if __name__ == '__main__':
    unittest.main()