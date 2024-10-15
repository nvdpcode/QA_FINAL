from QA_final.db_connections import OracleConnection, SolrConnection
import logging
from config import ORACLE_CONN_STR, SOLR_URL, ORACLE_QUERY  # Importing from config
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DocumentComparator:
    def __init__(self):
        self.oracle_conn = OracleConnection(ORACLE_CONN_STR)
        self.solr_conn = SolrConnection(SOLR_URL)

    def compare_documents(self):
        """Fetch and compare document data between Oracle and Solr."""
        # Fetch data from Oracle
        try:
            self.oracle_conn.connect()
            oracle_data = self.oracle_conn.execute_query(ORACLE_QUERY)  # Using query from config
            logging.info(f"Fetched {len(oracle_data)} records from Oracle.")
        except Exception as e:
            logging.error(f"Error fetching data from Oracle: {e}")
            oracle_data = []
        finally:
            self.oracle_conn.close()

        # Fetch data from Solr
        try:
            solr_data = self.solr_conn.fetch_data()
            logging.info(f"Fetched {len(solr_data)} records from Solr.")
        except Exception as e:
            logging.error(f"Error fetching data from Solr: {e}")
            solr_data = []

        # Compare documents
        self.compare_document_data(oracle_data, solr_data)

    def compare_document_data(self, oracle_data, solr_data):
        """Compares the document data between Oracle and Solr based on unique ID."""
        oracle_docs = {self.document_to_key(doc): doc for doc in oracle_data if self.document_to_key(doc)}
        solr_docs = {self.document_to_key(doc): doc for doc in solr_data if self.document_to_key(doc)}

        logging.info(f"Oracle documents count: {len(oracle_docs)}")
        logging.info(f"Solr documents count: {len(solr_docs)}")

        # Find documents only in Oracle or only in Solr
        only_in_oracle = set(oracle_docs.keys()) - set(solr_docs.keys())
        only_in_solr = set(solr_docs.keys()) - set(oracle_docs.keys())

        if not only_in_oracle and not only_in_solr:
            logging.info("Documents match between Oracle and Solr.")
        else:
            if only_in_oracle:
                logging.warning(f"Documents only in Oracle: {len(only_in_oracle)}")
                logging.warning(f"Items only in Oracle: {self.extract_item_and_paths(only_in_oracle)[:10]}")
            if only_in_solr:
                logging.warning(f"Documents only in Solr: {len(only_in_solr)}")
                logging.warning(f"Items only in Solr: {self.extract_item_and_paths(only_in_solr)[:200]}")

        # Compare fields for documents that exist in both Oracle and Solr
        discrepancies_found = False
        for key in set(oracle_docs.keys()) & set(solr_docs.keys()):
            discrepancies_found |= self.compare_selected_fields(oracle_docs[key], solr_docs[key])

        if discrepancies_found:
            logging.warning("Unique key which is combination of item number and filepath is mismatched")

    def compare_selected_fields(self, oracle_doc, solr_doc):
        """Compare selected fields of a single document from Oracle and Solr."""
        fields_to_compare = [
            'ITEM_NUMBER', 'DESCRIPTION', 'CREATED_BY', 'DOC_TYPE',
            'FILENAME', 'IFS_FILEPATH', 'HFS_FILEPATH', 'REV_NUMBER','RELEASE_DATE'
        ]
        
        discrepancies = {}
        
        for field in fields_to_compare:
            oracle_value = self.normalize_value(oracle_doc.get(field, 'N/A'))
            solr_value = self.normalize_value(solr_doc.get(field.lower(), ['N/A'])[0] if isinstance(solr_doc.get(field.lower()), list) else solr_doc.get(field.lower(), 'N/A'))
            
            # Normalize the Solr values to strip unwanted spaces and characters
            solr_value = solr_value.strip().strip("'").replace('\\', '/')

            # Check if both values are considered equivalent
            if oracle_value == 'N/A' and solr_value == '#null#':
                continue  # Treat them as equal, skip logging

            if oracle_value != solr_value:
                discrepancies[field] = {'oracle': oracle_value, 'solr': solr_value}

        if discrepancies:
            logging.warning(f"Discrepancies for ITEM_NUMBER: {oracle_doc.get('ITEM_NUMBER', 'N/A')}")
            for field, values in discrepancies.items():
                logging.warning(f"Field '{field}' - Oracle: {values['oracle']} | Solr: {values['solr']}")
            return True  # Indicate that discrepancies were found
        
        return False  # No discrepancies found

    @staticmethod
    def normalize_value(value):
        """Normalize values for comparison by trimming spaces, handling 'null' values, and removing extra characters."""
        # Handle both #null# and None equivalently
        if value in (None, '#null#', 'N/A', 'None', 'unknown', ''):
            return 'N/A'
        
        # Strip leading/trailing spaces, remove invisible characters
        normalized_value = str(value).strip()
        
        # Replace multiple spaces or invisible characters with a single space
        normalized_value = re.sub(r'\s+', '', normalized_value)

        return normalized_value

    def document_to_key(self, doc):
        """Generate a unique key for each document based on ITEM_NUMBER and file path."""
        if 'ITEM_NUMBER' in doc:  # Oracle data
            item_number = str(doc['ITEM_NUMBER']).strip().lower()
            file_path = str(doc.get('IFS_FILEPATH', doc.get('HFS_FILEPATH', 'none'))).strip().replace('\\', '/').lower()
        elif 'item_number' in doc:  # Solr data
            # Clean up the item_number and file_path
            item_number = str(doc['item_number'][0]).strip().replace("'", "").strip().lower()
            file_path = str(doc.get('ifs_filepath', doc.get('hfs_filepath', ['none']))[0]).strip().replace("'", "").strip().replace('\\', '/').lower()
        else:
            return None

        if item_number and file_path:
            return (item_number, file_path)
        return None  # Return None if key cannot be formed

    def extract_item_and_paths(self, docs):
        """Helper method to extract item numbers and file paths from a set of documents."""
        return [(item[0], item[1]) for item in docs]
