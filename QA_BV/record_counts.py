# data_consistency_checker.py
from QA_final.db_connections import OracleConnection, SolrConnection
import logging
from config import ORACLE_CONN_STR, SOLR_URL, PARENT_QUERY, CHILD_QUERY  # Import from config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataConsistencyChecker:
    def __init__(self):
        self.oracle_conn = OracleConnection(ORACLE_CONN_STR)
        self.solr_conn = SolrConnection(SOLR_URL)

    def run_consistency_check(self):
        """Run the full consistency check between Oracle and Solr."""
        # Fetch data from Oracle
        try:
            self.oracle_conn.connect()
            parent_rs = self.oracle_conn.format_cursor_data(self.oracle_conn.execute_query(PARENT_QUERY))
            logging.info(f"Fetched {len(parent_rs)} records from PARENT_QUERY")
            child_rs = self.oracle_conn.format_cursor_data(self.oracle_conn.execute_query(CHILD_QUERY))
            logging.info(f"Fetched {len(child_rs)} records from CHILD_QUERY")
            oracle_data = list(self.oracle_conn.process_documents(parent_rs, child_rs))  # Convert to list
            #logging.info(f"Final Oracle data count: {(oracle_data)}")
        finally:
            self.oracle_conn.close()

        # Fetch data from Solr
        solr_data = self.solr_conn.fetch_data()

        # Compare counts
        self.compare_data_count(oracle_data, solr_data)

    def compare_data_count(self, oracle_data, solr_data):
        """Compares the record counts from Oracle and Solr."""
        oracle_count = len(oracle_data)
        solr_count = len(solr_data)

        logging.info(f"Oracle record count: {oracle_count}")
        logging.info(f"Solr record count: {solr_count}")

        if oracle_count == solr_count:
            logging.info("Record counts match between Oracle and Solr.")
        else:
            logging.warning(f"Record count discrepancy: Oracle ({oracle_count}) vs Solr ({solr_count}).")

    def check_dsr(self, oracle_data, solr_data):
        pass
