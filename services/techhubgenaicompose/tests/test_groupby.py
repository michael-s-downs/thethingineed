import pytest
from compose.actions.groupby import GroupByDoc, GroupByDate, GroupByFactory
from common.errors.genaierrors import PrintableGenaiError


class MockStreamChunk:
    def __init__(self, document_id, score, snippet_number, date):
        self.document_id = document_id
        self.score = score
        self.snippet_number = snippet_number
        self.date = date

    def get_mean_score(self):
        return self.score

    def get(self, key):
        if key == "document_id":
            return self.document_id
        elif key == "snippet_number":
            return self.snippet_number
        elif key == "date":
            return self.date
        return None


@pytest.fixture
def mock_streamlist():
    return [
        MockStreamChunk("doc1", 0.9, 1, "2023-10-10"),
        MockStreamChunk("doc1", 0.7, 2, "2023-10-10"),
        MockStreamChunk("doc2", 0.85, 1, "2023-10-09"),
        MockStreamChunk("doc2", 0.75, 2, "2023-10-09"),
    ]


class TestGroupByDoc:
    def test_groupby_doc(self, mock_streamlist):
        """Test the GroupByDoc process method."""
        groupby = GroupByDoc(mock_streamlist)
        result = groupby.process({"desc": True, "method": "max"})

        assert result[0].get("document_id") == "doc1"
        assert result[1].get("document_id") == "doc1"
        assert result[2].get("document_id") == "doc2"

    def test_groupby_doc_invalid_method(self, mock_streamlist):
        """Test GroupByDoc raises an error for an invalid method."""
        groupby = GroupByDoc(mock_streamlist)
        with pytest.raises(
            PrintableGenaiError, match="Groupby sorting method not found"
        ):
            groupby.process({"desc": True, "method": "invalid"})


class TestGroupByDate:
    def test_groupby_date(self, mock_streamlist):
        """Test the GroupByDate process method."""
        groupby = GroupByDate(mock_streamlist)
        result = groupby.process({"desc": False})

        assert result[0].get("date") == "2023-10-09"
        assert result[2].get("date") == "2023-10-10"


class TestGroupByFactory:
    def test_groupby_factory_doc(self, mock_streamlist):
        """Test the GroupByFactory for docscore type."""
        factory = GroupByFactory("docscore")
        result = factory.process(mock_streamlist, {"desc": True, "method": "mean"})

        assert result[0].get("document_id") == "doc1"

    def test_groupby_factory_invalid_type(self):
        """Test GroupByFactory raises an error for an invalid groupby type."""
        with pytest.raises(
            PrintableGenaiError, match="Provided groupby does not match"
        ):
            GroupByFactory("invalid_type")
