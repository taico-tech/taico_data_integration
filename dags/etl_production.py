import logging
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
import sys

sys.path.append('/opt/airflow/include/scripts')

from include.scripts.sync_data_production import (
    insert_if_not_exists, 
    process_filter, 
    fetch_company_info, 
    TABLE_MAPPINGS, 
    construct_queries, 
    get_postgres_conn,
    handle_media_combinations  # Import the new function
)
from include.scripts.data_quality_checks import (
    validate_schema, 
    validate_non_null, 
    validate_unique, 
    validate_sync
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

engine = create_engine(get_postgres_conn())

def fetch_company_and_queries(**kwargs):
    """Fetch company ID and table name and push to XCom."""
    alias = 'test_company'  # You can change this to a dynamic value if needed
    company_id, bq_table = fetch_company_info(alias)
    queries = construct_queries(bq_table)
    logging.info(f"Fetched company_id: {company_id}, bq_table: {bq_table}")
    kwargs['ti'].xcom_push(key='company_id', value=company_id)
    kwargs['ti'].xcom_push(key='queries', value=queries)
    kwargs['ti'].xcom_push(key='alias', value=alias)
    kwargs['ti'].xcom_push(key='bq_table', value=bq_table)

def decide_next_step(**kwargs):
    """Determine the next step based on the presence of data."""
    filter_name = kwargs['filter_name']
    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids=f'process_{filter_name}', key='status')
    logging.info(f"Deciding next step for {filter_name}. Status: {result}")
    if result == 'skip':
        return f'skip_insert_{filter_name}'
    else:
        return f'insert_{filter_name}'

def validate_data_quality(**kwargs):
    """Perform data quality checks."""
    filter_name = kwargs['filter_name']
    ti = kwargs['ti']
    company_id = ti.xcom_pull(task_ids='fetch_company_and_queries', key='company_id')
    table_name = TABLE_MAPPINGS[filter_name]

    # Pull the processed DataFrame from XCom
    extracted_df = ti.xcom_pull(task_ids=f'process_{filter_name}', key='data_frame')
    logging.info(f"Extracted DataFrame: {extracted_df}")

    logging.info(f"Validating data quality for {filter_name}. Extracted DataFrame: {extracted_df is not None}")

    if extracted_df is not None:
        try:
            # Fetch the inserted data from the target table
            with engine.connect() as connection:
                query = text(f"SELECT * FROM {table_name} WHERE company_id = :company_id")
                inserted_df = pd.read_sql(query, connection, params={'company_id': company_id})

            logging.info(f"Inserted DataFrame: {inserted_df}")

            # Validate non-nullable columns
            validate_non_null(table_name, inserted_df)

            # Validate unique columns
            validate_unique(table_name, inserted_df)

            # Validate synchronization with the extracted DataFrame
            validate_sync(table_name, filter_name, company_id, extracted_df)

            logging.info(f"Data quality checks passed for {filter_name}.")
            # No need to return anything; passing validation means the task will succeed.
        except Exception as e:
            # Log and raise an exception to fail the task
            logging.error(f"Data quality check failed for {filter_name}: {e}")
            raise  # Raising the exception will mark the task as failed
    else:
        error_msg = f"Extracted DataFrame for {filter_name} is None."
        logging.error(error_msg)
        raise ValueError(error_msg)  # Raising an exception will mark the task as failed

def process_filter_and_push(**kwargs):
    ti = kwargs['ti']
    filter_name = kwargs['filter_name']
    query = ti.xcom_pull(task_ids='fetch_company_and_queries', key='queries')[filter_name]
    company_id = ti.xcom_pull(task_ids='fetch_company_and_queries', key='company_id')
    result = process_filter(filter_name, query, company_id)
    logging.info(f"Processed filter {filter_name}. Result status: {result['status']}")
    ti.xcom_push(key='status', value=result['status'])
    ti.xcom_push(key='data_frame', value=result['data_frame'] if 'data_frame' in result else None)

def insert_with_dataframe(**kwargs):
    ti = kwargs['ti']
    filter_name = kwargs['filter_name']
    data_frame = ti.xcom_pull(task_ids=f'process_{filter_name}', key='data_frame')
    company_id = ti.xcom_pull(task_ids='fetch_company_and_queries', key='company_id')
    logging.info(f"Inserting data for {filter_name} with company_id: {company_id}")
    insert_if_not_exists(TABLE_MAPPINGS[filter_name], 'name', data_frame, filter_name, company_id)

with DAG(
    'ETL_Pipeline_production',
    default_args=default_args,
    description='An ETL pipeline from BigQuery to PostgreSQL with data quality checks',
    start_date=datetime(2023, 5, 12),
    catchup=False,
    schedule_interval='@hourly',
) as dag:

    start = DummyOperator(task_id='start')

    fetch_info = PythonOperator(
        task_id='fetch_company_and_queries',
        python_callable=fetch_company_and_queries,
        provide_context=True
    )

    # Create a task dictionary to hold all tasks for each filter
    process_tasks = {}
    decide_tasks = {}
    insert_tasks = {}
    skip_insert_tasks = {}
    quality_checks = {}

    for filter_name in TABLE_MAPPINGS.keys():
        process_tasks[filter_name] = PythonOperator(
            task_id=f'process_{filter_name}',
            python_callable=process_filter_and_push,
            op_kwargs={'filter_name': filter_name},
            provide_context=True
        )

        decide_tasks[filter_name] = BranchPythonOperator(
            task_id=f'decide_{filter_name}',
            python_callable=decide_next_step,
            op_kwargs={'filter_name': filter_name},
            provide_context=True
        )

        insert_tasks[filter_name] = PythonOperator(
            task_id=f'insert_{filter_name}',
            python_callable=insert_with_dataframe,
            op_kwargs={'filter_name': filter_name},
            provide_context=True
        )

        skip_insert_tasks[filter_name] = DummyOperator(task_id=f'skip_insert_{filter_name}')

        quality_checks[filter_name] = PythonOperator(
            task_id=f'validate_{filter_name}',
            python_callable=validate_data_quality,
            op_kwargs={'filter_name': filter_name},
            provide_context=True
        )

        # Define task dependencies
        start >> fetch_info >> process_tasks[filter_name] >> decide_tasks[filter_name]
        decide_tasks[filter_name] >> [skip_insert_tasks[filter_name], insert_tasks[filter_name]]
        insert_tasks[filter_name] >> quality_checks[filter_name]

    # Add the handle_media_combinations task
    handle_combinations = PythonOperator(
        task_id='handle_media_combinations',
        python_callable=handle_media_combinations,
        provide_context=True
    )

    # Set dependency for handle_combinations to run after all quality checks
    all_quality_checks = list(quality_checks.values())
    for quality_check in all_quality_checks:
        quality_check >> handle_combinations

    # Ensure that handle_combinations runs after fetch_info
    fetch_info >> handle_combinations
