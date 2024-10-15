import pytest
from status_check import LifeCycleChecker

@pytest.mark.usefixtures("caplog")
def test_run_solr_data_lifecycle_and_production_check(caplog):
    # Initialize the LifeCycleChecker
    checker = LifeCycleChecker()

    # Run the method under test
    with caplog.at_level("INFO"):
        checker.run_solr_data_lifecycle_and_production_check()

    # Assert that the discrepancy count is 0
    assert "Discrepancy Count: 0" in caplog.text, "Test failed: Discrepancy count is not 0."
