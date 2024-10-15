# main.py
import logging
from record_counts import DataConsistencyChecker
from column_comparator import ColumnComparator

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def log_section_start(name):
    logging.info("=" * 100)
    logging.info(f"Starting execution of {name}")
    logging.info("=" * 100)


if __name__ == "__main__":
    # Running Data Consistency Check
    log_section_start("No of Records Checker for Doctype: BV")
    consistency_checker = DataConsistencyChecker()
    consistency_checker.run_consistency_check()
    
    
    log_section_start("Column Checker for Doctype: BV")
    column_checker = ColumnComparator()
    column_checker.compare_columns()
    