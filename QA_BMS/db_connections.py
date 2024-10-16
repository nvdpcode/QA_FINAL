import cx_Oracle
import pysolr
import logging
import requests
import os
from datetime import datetime
from typing import List, Dict, Any, Iterator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class OracleConnection:
    def __init__(self, oracle_conn_str: str):
        self.oracle_conn_str = oracle_conn_str
        self.connection = None

    def connect(self):
        """Establish connection to Oracle DB."""
        try:
            self.connection = cx_Oracle.connect(self.oracle_conn_str)
        except cx_Oracle.DatabaseError as e:
            logging.error(f"Failed to connect to Oracle: {e}")
            raise

    def execute_query(self, query: str):
        """Execute a query and return the result."""
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            columns = [col[0] for col in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            cursor.close()
            return results
        except cx_Oracle.DatabaseError as e:
            logging.error(f"Oracle Database Error: {e}")
            return []

    def format_cursor_data(self, resultset):
        results = []
        for rec in resultset:
            for key, value in rec.items():
                if isinstance(value, datetime):
                    rec[key] = value.strftime('%Y-%m-%d %H:%M:%S')
            results.append(rec)
        return results

    def replace_null_values(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Replace null, empty, or 'n/a' values with '#null#' and handle case-insensitive keys."""
        tmp_dict = {}
        for key, value in data.items():
            key = key.lower()  # Normalize key to lowercase
            if value is None or value == "" or value == "null" or (isinstance(value, str) and value.lower() == "n/a"):
                value = "#null#"
            if key == 'rev_number':  # Case-insensitive match for 'rev_number'
                value = f"'{value}'"  # Wrap the rev_number in single quotes
            tmp_dict[key] = value
        return tmp_dict

    def process_documents(self, parents: List[Dict[str, Any]], children: List[Dict[str, Any]]) -> Iterator[Dict[str, Any]]:
        for parent in parents:
            parent = {k.lower(): v for k, v in parent.items()}  # Normalize parent keys to lowercase
            item_number = parent.get('item_number')

            if not item_number:
                logging.warning(f"Parent record missing item_number: {parent}")
                continue

            parent_children = [
                {k.lower(): v for k, v in child.items()}  # Normalize child keys to lowercase
                for child in children if child.get('ITEM_NUMBER', '').lower() == item_number.lower()
            ]

            if parent_children:
                for child in parent_children:
                    try:
                        combined_item = self.replace_null_values(parent)

                        child_item = {k: v for k, v in child.items() if k not in ['item_number', 'description', 'lifecycle', 'release_date']}
                        child_item = self.replace_null_values(child_item)

                        file_path = child.get('ifs_filepath') or child.get('hfs_filepath')
                       # if file_path:
                            #full_path = os.path.join(settings.FILEVAULT_PATH, file_path)
                            #if os.path.exists(full_path):
                                #if settings.INCLUDE_CONTENT:
                                    #child_item['content'] = self.extract_content(full_path)
                                #else:
                                    #child_item['content'] = "No-content"
                            #else:
                                #logging.error(f"File path does not exist / permission denied: {full_path}")
                                #child_item['content'] = "No-content"
                        #else:
                            #logging.error(f"Null value for IFS_filepath / HFS_filepath: {file_path}")
                            #child_item['content'] = "No-content"

                        combined_item.update(child_item)
                        yield combined_item
                    except Exception as e:
                        logging.error(f"Error processing child document for item_number {item_number}: {str(e)}")
            else:
                yield self.replace_null_values(parent)

    def close(self):
        """Close the Oracle DB connection."""
        if self.connection:
            try:
                self.connection.close()
            except cx_Oracle.DatabaseError as e:
                logging.error(f"Failed to close Oracle connection: {e}")


class SolrConnection:
    def __init__(self, solr_url: str):
        self.solr_client = pysolr.Solr(solr_url, always_commit=True, timeout=10)
        self.solr_url = solr_url

    def fetch_data(self, query: str = '*:*', rows: int = 2000) -> list:
        """Fetch data from Solr in batches."""
        solr_data = []
        start = 0
        while True:
            try:
                solr_results = self.solr_client.search(query, rows=rows, start=start)
                batch_size = len(solr_results)
                solr_data.extend(solr_results)
                if batch_size < rows:
                    break
                start += rows
            except Exception as e:
                logging.error(f"Solr Error: {e}")
                break
        return solr_data

    def get_schema_fields(self):
        """Fetch the schema fields from the Solr instance."""
        schema_url = f"{self.solr_url}/schema/fields"
        try:
            response = requests.get(schema_url)
            response.raise_for_status()
            schema_data = response.json()
            if 'fields' in schema_data:
                return schema_data['fields']
            else:
                logging.error("No fields found in Solr schema.")
                return []
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching Solr schema fields: {e}")
            return []
