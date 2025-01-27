### This code is property of the GGAO ###

import os
os.environ['URL_LLM'] = "test_url"
os.environ['URL_RETRIEVE'] = "test_retrieve"
import pytest
from compose.actions.merge import MergeMethod, AddMerge, MetaMerge, MergeFactory
from compose.streamchunk import StreamChunk
from common.errors.genaierrors import PrintableGenaiError
from unittest.mock import patch


@pytest.fixture(scope="class")
def mock_streamchunk():
    """Create a mock StreamChunk for testing."""
    return [
        StreamChunk({"content": "chunk1", "meta": {"key": "value1"}}),
        StreamChunk({"content": "chunk2", "meta": {"key": "value2"}}),
    ]


@pytest.fixture
def mock_storage_containers():
    """Fixture to provide mock storage containers."""
    return {"workspace": "mock_workspace"}


@pytest.fixture
def mock_streamchunk_with_common_and_unique_metadata():
    """Create a mock StreamChunk with common and unique metadata for testing."""
    return [
        StreamChunk(
            {
                "content": "chunk1",
                "meta": {"shared_key": "common_value", "unique_key": "value1"},
            }
        ),
        StreamChunk(
            {
                "content": "chunk2",
                "meta": {"shared_key": "common_value", "unique_key": "value2"},
            }
        ),
        StreamChunk(
            {
                "content": "chunk3",
                "meta": {"shared_key": "common_value", "extra_key": "value3"},
            }
        ),
    ]


@pytest.fixture
def mock_streamchunk_with_same_grouping_key():
    """Create a mock StreamChunk where all chunks have the same grouping key."""
    return [
        StreamChunk({"content": "chunk1", "meta": {"key": "group1"}}),
        StreamChunk({"content": "chunk2", "meta": {"key": "group1"}}),
    ]


class TestMergeMethod(MergeMethod):
    """Test subclass for MergeMethod to cover abstract methods."""

    TYPE = "test"

    def process(self):
        """Mock process method for testing."""
        return []

    def _get_example(self):
        """Return a mock example using super and ensure it returns the expected format."""
        example = super()._get_example()
        return {
            "type": self.TYPE,
            "params": {},
        }


class TestMergeMethodExample:
    """Test suite for MergeMethod subclass."""

    def test_get_example(self):
        """Test the get_example method of MergeMethod."""
        merge_method = TestMergeMethod([])
        example = merge_method.get_example()
        expected_example = '{"type": "test", "params": {}}'
        assert example == expected_example


class TestAddMerge:
    """Test suite for AddMerge class."""

    def test_process(self, mock_streamchunk):
        """Test the process method of AddMerge."""
        add_merge = AddMerge(mock_streamchunk)
        result = add_merge.process()
        assert len(result) == 1
        assert result[0]["content"] == "chunk1\nchunk2"

    def test_get_example(self):
        """Test the get_example method of AddMerge."""
        add_merge = AddMerge([])
        example = add_merge.get_example()
        expected_example = '{"type": "add", "params": {"SEQ": "\\n"}}'
        assert example == expected_example


class TestMetaMerge:
    """Test suite for MetaMerge class."""

    def test_process_with_params(self, mock_streamchunk):
        """Test the process method of MetaMerge with parameters."""
        meta_merge = MetaMerge(mock_streamchunk)
        params = {
            "template": "Custom content: $content",
            "sep": ", ",
            "grouping_key": None,
        }
        result = meta_merge.process(params)

        assert len(result) == 1
        assert result[0].content == "Custom content: chunk1, Custom content: chunk2"

    def test_process_with_grouping_key(self, mock_streamchunk):
        """Test the process method of MetaMerge with a grouping key."""
        meta_merge = MetaMerge(mock_streamchunk)
        params = {
            "template": "Grouped content: $content",
            "sep": ", ",
            "grouping_key": "key",
        }
        result = meta_merge.process(params)

        assert len(result) > 0
        assert all(isinstance(chunk, StreamChunk) for chunk in result)

    def test_process_with_grouping_key_and_multiple_chunks(self, mock_streamchunk):
        """Test the process method of MetaMerge with a grouping key and multiple chunks."""
        mock_streamchunk[0].meta["key"] = "group1"
        mock_streamchunk[1].meta["key"] = "group2"

        meta_merge = MetaMerge(mock_streamchunk)
        params = {
            "template": "Grouped content: $content",
            "sep": ", ",
            "grouping_key": "key",
        }
        result = meta_merge.process(params)

        assert len(result) == 2
        assert result[0].content == "Grouped content: chunk1"
        assert result[1].content == "Grouped content: chunk2"

    def test_process_template_without_dollar(
        self, mock_streamchunk, mock_storage_containers
    ):
        """Test MetaMerge process when template doesn't contain '$'."""
        meta_merge = MetaMerge(mock_streamchunk)

        with patch("compose.actions.merge.load_file", return_value=b""):
            params = {"template": "non_existing_template"}
            with pytest.raises(PrintableGenaiError) as exc_info:
                meta_merge.process(params)

            assert exc_info.value.status_code == 400
            assert str(exc_info.value) == "Error 400: Template empty"

    def test_process_template_not_found(
        self, mock_streamchunk, mock_storage_containers
    ):
        """Test MetaMerge process when template file doesn't exist."""
        meta_merge = MetaMerge(mock_streamchunk)

        with patch("compose.actions.merge.load_file", side_effect=ValueError):
            params = {"template": "non_existing_template"}
            with pytest.raises(PrintableGenaiError) as exc_info:
                meta_merge.process(params)

            assert exc_info.value.status_code == 404
            assert (
                str(exc_info.value)
                == "Error 404: Cloud config file doesn't exist for name non_existing_template"
            )

    def test_process_with_grouping_value_and_key_in_template(self, mock_streamchunk):
        """Test MetaMerge process where grouping value is valid and key is in template."""

        mock_streamchunk[0].meta["key"] = "group1"
        mock_streamchunk[1].meta["key"] = "group2"

        meta_merge = MetaMerge(mock_streamchunk)

        params = {
            "template": "Grouped key: $key, content: $content",
            "sep": ", ",
            "grouping_key": "key",
        }

        result = meta_merge.process(params)

        assert len(result) == 2
        assert "key: group1" in result[0].content
        assert "content: chunk1" in result[0].content
        assert "key: group2" in result[1].content
        assert "content: chunk2" in result[1].content

    def test_process_with_common_and_unique_metadata(
        self, mock_streamchunk_with_common_and_unique_metadata
    ):
        """Test the process method of MetaMerge with common and unique metadata."""
        meta_merge = MetaMerge(mock_streamchunk_with_common_and_unique_metadata)
        params = {
            "template": "Content: $content",
            "sep": ", ",
            "grouping_key": "shared_key",
        }
        result = meta_merge.process(params)

        assert len(result) == 1
        assert result[0].content == "Content: chunk1, Content: chunk2, Content: chunk3"
        assert result[0].meta == {"shared_key": "common_value"}

    def test_get_example(self):
        """Test the _get_example method of MetaMerge."""
        meta_merge = MetaMerge([])
        example = meta_merge._get_example()
        expected_example = {
            "type": "meta",
            "params": {"template": "Content: $content"},
        }
        assert example == expected_example

    def test_process_with_default_params(self, mock_streamchunk):
        """Test the process method of MetaMerge with default parameters."""
        meta_merge = MetaMerge(mock_streamchunk)
        result = meta_merge.process(None)

        assert len(result) == 1
        assert result[0].content == "Content: chunk1-##########-Content: chunk2"


class TestMergeFactory:
    """Test suite for MergeFactory class."""

    def test_create_meta_merge(self, mock_streamchunk):
        """Test creating MetaMerge via MergeFactory."""
        factory = MergeFactory("meta")
        params = {
            "template": "Content: $content",
            "sep": ", ",
            "grouping_key": None,
        }
        result = factory.process(mock_streamchunk, params)

        assert len(result) == 1
        assert result[0].content == "Content: chunk1, Content: chunk2"

    def test_invalid_merge_type(self):
        """Test MergeFactory with an invalid merge type."""
        with pytest.raises(PrintableGenaiError):
            MergeFactory("invalid_type")
