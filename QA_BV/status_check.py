# status_check.py
from QA_final.db_connections import OracleConnection, SolrConnection
import logging
from config import SOLR_URL  # Import from config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class LifeCycleChecker:
    def __init__(self):
        self.solr_conn = SolrConnection(SOLR_URL)

    def is_valid_date(self, date_string):
        try:
            year, month, day = map(int, date_string.split('-'))
            return (
                1 <= month <= 12 and
                1 <= day <= 31 and
                1900 <= year <= 2100  # Adjust range as needed
            )
        except ValueError:
            return False

    def run_solr_data_lifecycle_and_production_check(self):
        """Checks for the valid lifecycle and release date"""

        # Fetch data from Solr
        solr_data = self.solr_conn.fetch_data()
        discrepancy_count = 0
        for data in solr_data:
            lifecycle = data.get('lifecycle')[0].split()[1]
            release_date = data.get('release_date')[0].split()[1]
            item_number = data.get('item_number')[0].split()[1]  # Assuming item_number is part of the data

            # Check for discrepancies
            if lifecycle != "Production" or not self.is_valid_date(release_date):
                logging.info(f"Discrepancy found in the item number: {item_number}. Release Date: {release_date}, Lifecycle: {lifecycle}")
                discrepancy_count += 1

        logging.info(f"Discrepancy Count: {discrepancy_count}")

if __name__ == "__main__":
    obj = LifeCycleChecker()
    obj.run_solr_data_lifecycle_and_production_check()
