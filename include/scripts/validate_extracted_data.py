import logging
from google.cloud import bigquery
from airflow.hooks.base import BaseHook
import os
from include.scripts.config import DATASET_ID, PROJECT_ID

def get_bigquery_client():
    """Fetch BigQuery client using Airflow connection settings."""
    try:
        connection = BaseHook.get_connection('gcp')
        key_path = "/usr/local/airflow/include/gcp/service_account.json"
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
        return bigquery.Client()
    except Exception as e:
        raise RuntimeError(f"Failed to establish a BigQuery client connection: {e}")

def validate_table_schema(table_name, expected_fields):
    """Validate that the table in BigQuery matches the expected schema and contains the necessary data."""
    client = get_bigquery_client()

    try:
        logging.info(f"Validating table: {table_name} in dataset: {DATASET_ID}...")
        table_ref = client.dataset(DATASET_ID, project=PROJECT_ID).table(table_name)
        table = client.get_table(table_ref)

        # Check schema
        actual_fields = {field.name for field in table.schema}
        missing_fields = expected_fields - actual_fields

        if missing_fields:
            raise Exception(f"Missing fields in {table_name} schema: {missing_fields}")

        logging.info(f"Schema validation for table {table_name} succeeded.")
        return True
    except Exception as e:
        logging.error(f"Validation failed for table {table_name}: {e}")
        raise

def validate_row_count(table_name, min_rows):
    """Validate that the table in BigQuery contains at least a minimum number of rows."""
    client = get_bigquery_client()

    try:
        logging.info(f"Validating row count for table: {table_name} in dataset: {DATASET_ID}...")
        table_ref = client.dataset(DATASET_ID, project=PROJECT_ID).table(table_name)
        table = client.get_table(table_ref)

        if table.num_rows < min_rows:
            raise Exception(f"Row count for {table_name} is below expected minimum: {table.num_rows} < {min_rows}")

        logging.info(f"Row count validation for table {table_name} succeeded.")
        return True
    except Exception as e:
        logging.error(f"Row count validation failed for table {table_name}: {e}")
        raise

def validate_null_values(table_name, critical_columns):
    """Check for null values in critical columns."""
    client = get_bigquery_client()

    try:
        logging.info(f"Checking null values for table: {table_name} in dataset: {DATASET_ID}...")
        query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}`"
        df = client.query(query).to_dataframe()

        null_check_failed = False
        for col in critical_columns:
            if df[col].isnull().any():
                null_indices = df[df[col].isnull()].index.tolist()
                logging.error(f"Null values found in column '{col}' at rows: {null_indices}")
                null_check_failed = True

        if null_check_failed:
            raise Exception("Null check failed for critical columns.")
        
        logging.info(f"Null value check for table {table_name} succeeded.")
        return True
    except Exception as e:
        logging.error(f"Null value check failed for table {table_name}: {e}")
        raise

def validate_duplicates(table_name):
    """Check for duplicate rows in the table."""
    client = get_bigquery_client()

    try:
        logging.info(f"Checking duplicates for table: {table_name} in dataset: {DATASET_ID}...")
        query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}`"
        df = client.query(query).to_dataframe()

        if df.duplicated().any():
            duplicated_rows = df[df.duplicated()]
            logging.warning(f"Duplicate rows found: {duplicated_rows.index.tolist()}")
            raise Exception("Duplicate rows found in the data.")
        
        logging.info(f"Duplicate check for table {table_name} succeeded.")
        return True
    except Exception as e:
        logging.error(f"Duplicate check failed for table {table_name}: {e}")
        raise
