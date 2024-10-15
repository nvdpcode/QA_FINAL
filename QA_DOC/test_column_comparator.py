import pytest
from column_comparator import ColumnComparator

@pytest.mark.usefixtures("caplog")
def test_compare_columns(caplog):
    comparator = ColumnComparator()
    
    # Perform the comparison
    with caplog.at_level("WARNING"):
        comparator.compare_columns()
    
    # Check if a mismatch was logged (which should cause the test to fail)
    assert "Columns mismatch" not in caplog.text, "Test failed: Column mismatch was detected"
    assert "Columns only in Oracle but not in Solr" not in caplog.text, "Test failed: Columns are missing from Solr"
    assert "Columns only in Solr but not in Oracle" not in caplog.text, "Test failed: Columns are missing from Oracle"
