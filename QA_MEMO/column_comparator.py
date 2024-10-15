# column_comparator.py

from QA_final.db_connections import OracleConnection, SolrConnection
import logging
from config import ORACLE_CONN_STR, SOLR_URL, ORACLE_QUERY, PARENT_QUERY, CHILD_QUERY  # Import from config


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ColumnComparator:
    def __init__(self):
        self.oracle_conn = OracleConnection(ORACLE_CONN_STR)  # Use connection string from config
        self.solr_conn = SolrConnection(SOLR_URL)  # Use Solr URL from config

    def compare_columns(self):
        """Fetch and compare column metadata between Oracle and Solr."""
        try:
            self.oracle_conn.connect()
            parent_rs = self.oracle_conn.format_cursor_data(self.oracle_conn.execute_query(PARENT_QUERY))
            logging.info(f"Fetched {len(parent_rs)} records from PARENT_QUERY")
            child_rs = self.oracle_conn.format_cursor_data(self.oracle_conn.execute_query(CHILD_QUERY))
            logging.info(f"Fetched {len(child_rs)} records from CHILD_QUERY")
            oracle_columns = list(self.oracle_conn.process_documents(parent_rs, child_rs))  # Convert to list
            #logging.info(f"Final Oracle data count: {(oracle_data)}")
        finally:
            self.oracle_conn.close()

        # Fetch Solr schema (fields)
        solr_fields = self.solr_conn.get_schema_fields()

        # Compare columns
        self.compare_column_metadata(oracle_columns, solr_fields)

    def compare_column_metadata(self, oracle_columns, solr_fields):
        """Compares the column metadata between Oracle and Solr."""
        logging.info("Starting Columns comparison...")
        
        oracle_column_names = {key.lower() for row in oracle_columns for key in row.keys()}
        solr_field_names = {field['name'].lower() for field in solr_fields}

        solr_ignore_fields = {'_text_', '_nest_path_','id', '_root_', '_version_', 'content','file_size'}
        solr_field_names -= solr_ignore_fields

        only_in_oracle = oracle_column_names - solr_field_names
        only_in_solr = solr_field_names - oracle_column_names

        if not only_in_oracle and not only_in_solr:
            logging.info("Columns match between Oracle and Solr.")
        else:
            if only_in_oracle:
                logging.warning(f"Columns mismatch : Columns only in Oracle but not in Solr: {only_in_oracle}")
            if only_in_solr:
                logging.warning(f"Columns mismatch : Columns present only in Solr but not in Oracle: {only_in_solr}")
