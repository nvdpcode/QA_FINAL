import pytest
from record_counts import DataConsistencyChecker

@pytest.mark.usefixtures("caplog")
def test_compare_data_count(caplog):
    # Initialize the DataConsistencyChecker
    checker = DataConsistencyChecker()

    # Call the compare_documents method to test document comparison
    with caplog.at_level("INFO"):
        checker.run_consistency_check()

    # Assert log messages for data fetching and record matching
    assert "Fetched" in caplog.text, "Test failed: Data fetching logs not found."
    assert "Record counts match between Oracle and Solr." in caplog.text, "Test failed: Record counts do not match."
