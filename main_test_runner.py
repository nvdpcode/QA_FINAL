import pytest

if __name__ == "__main__":
    test_files = [
        "test_column_comparator.py",
        "test_data_consistency_checker.py",
        "test_document_comparator.py",
    ]
    print(f"Running tests: {test_files}")
    pytest.main(test_files)

