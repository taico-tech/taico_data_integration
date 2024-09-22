import logging
import pandas as pd
from sqlalchemy import create_engine, text
from airflow.hooks.base import BaseHook

from include.scripts.config import TABLE_SCHEMAS, NON_NULLABLE_COLUMNS, UNIQUE_COLUMNS

def get_postgres_conn():
    """Fetch PostgreSQL connection details."""
    connection = BaseHook.get_connection('postgres')
    return f"postgresql+psycopg2://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}"

engine = create_engine(get_postgres_conn())

def validate_schema(table_name, data):
    """Validate schema of the table."""
    logging.info(f"Validating schema for table {table_name}")
    expected_schema = TABLE_SCHEMAS[table_name]
    actual_schema = set(data.columns)

    logging.info(f"Expected schema: {expected_schema}")
    logging.info(f"Actual schema: {actual_schema}")

    if expected_schema != actual_schema:
        error_message = f"Schema mismatch for {table_name}. Expected: {expected_schema}, Got: {actual_schema}"
        logging.error(error_message)
        logging.error(f"Data causing schema mismatch: {data.head()}")
        raise ValueError(error_message)
    logging.info(f"Schema validation passed for table {table_name}")

def validate_non_null(table_name, data):
    """Validate non-nullable columns of the table."""
    logging.info(f"Validating non-nullable columns for table {table_name}")
    non_nullable_columns = ['name', 'company_id']
    for column in non_nullable_columns:
        null_rows = data[data[column].isnull()]
        if not null_rows.empty:
            error_message = f"Null values found in non-nullable column {column} of table {table_name}"
            logging.error(error_message)
            logging.error(f"Rows with null values in {column}: {null_rows}")
            raise ValueError(error_message)
    logging.info(f"Non-nullable columns validation passed for table {table_name}")

def validate_unique(table_name, data):
    """Validate uniqueness of columns in the table."""
    logging.info(f"Validating unique columns for table {table_name}")
    unique_columns = ['name', 'company_id']
    
    # Check for uniqueness based on the combination of columns
    duplicate_rows = data[data.duplicated(subset=unique_columns, keep=False)]
    if not duplicate_rows.empty:
        error_message = f"Duplicate values found in unique columns {unique_columns} of table {table_name}"
        logging.error(error_message)
        logging.error(f"Rows with duplicate values in {unique_columns}: {duplicate_rows}")
        raise ValueError(error_message)
    
    logging.info(f"Unique columns validation passed for table {table_name}")

def validate_sync(table_name,filter_name, company_id, extracted_data):
    """
    Validate that the number of distinct values and their presence is synchronized with the extracted data.
    """
    logging.info(f"Validating data sync for table {table_name} and company_id {company_id}")
    try:
        extracted_values = set(extracted_data[filter_name])
        logging.info(f"Extracted values: {extracted_values}")

        with engine.connect() as connection:
            query = text(f"""
                SELECT name FROM {table_name} WHERE company_id = :company_id
            """)
            result = connection.execute(query, company_id=company_id).fetchall()
            existing_values = set([row['name'] for row in result])

            logging.info(f"Existing values in database: {existing_values}")

            if extracted_values != existing_values:
                error_message = f"Data sync issue for {table_name}. Extracted: {extracted_values}, Existing: {existing_values}"
                logging.error(error_message)
                missing_in_extracted = existing_values - extracted_values
                missing_in_existing = extracted_values - existing_values
                logging.error(f"Missing in extracted data: {missing_in_extracted}")
                logging.error(f"Missing in existing data: {missing_in_existing}")
                raise ValueError(error_message)

    except Exception as e:
        logging.error(f"Error during data sync validation: {e}")
        raise
    logging.info(f"Data sync validation passed for table {table_name} and company_id {company_id}")
