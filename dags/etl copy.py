import logging
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
import sys

sys.path.append('/opt/airflow/include/scripts')

from include.scripts.sync_data import (
    create_media_relation_staging_table,
    create_staging_table,
    handle_media_combinations_staging,
    fetch_company_info,
    TABLE_MAPPINGS,
    construct_queries,
    get_postgres_conn,
    process_filter,
    insert_if_not_exists,
    move_table_staging_to_production,
    sync_data_staging
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
    alias = 'test_company'  # You can change this to a dynamic value if needed
    company_id, bq_table = fetch_company_info(alias)
    queries = construct_queries(bq_table)
    logging.info(f"Fetched company_id: {company_id}, bq_table: {bq_table}")
    kwargs['ti'].xcom_push(key='company_id', value=company_id)
    kwargs['ti'].xcom_push(key='queries', value=queries)
    kwargs['ti'].xcom_push(key='alias', value=alias)
    kwargs['ti'].xcom_push(key='bq_table', value=bq_table)

def decide_next_step(**kwargs):
    filter_name = kwargs['filter_name']
    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids=f'process_{filter_name}', key='status')
    logging.info(f"Deciding next step for {filter_name}. Status: {result}")
    if result == 'skip':
        return f'skip_insert_{filter_name}'
    else:
        return f'insert_{filter_name}'

def validate_data_quality(**kwargs):
    filter_name = kwargs['filter_name']
    ti = kwargs['ti']
    company_id = ti.xcom_pull(task_ids='fetch_company_and_queries', key='company_id')
    staging_table_name = f"staging_{TABLE_MAPPINGS[filter_name]}"

    extracted_df = ti.xcom_pull(task_ids=f'process_{filter_name}', key='data_frame')
    logging.info(f"Validating data quality for {filter_name}. Extracted DataFrame: {extracted_df is not None}")

    if extracted_df is not None:
        try:
            with engine.connect() as connection:
                query = text(f"SELECT * FROM {staging_table_name} WHERE company_id = :company_id")
                inserted_df = pd.read_sql(query, connection, params={'company_id': company_id})

            logging.info(f"Inserted DataFrame: {inserted_df}")

            validate_non_null(staging_table_name, inserted_df)
            validate_unique(staging_table_name, inserted_df)
            validate_sync(staging_table_name, filter_name, company_id, extracted_df)

            logging.info(f"Data quality checks passed for {filter_name}.")
        except Exception as e:
            logging.error(f"Data quality check failed for {filter_name}: {e}")
            raise
    else:
        error_msg = f"Extracted DataFrame for {filter_name} is None."
        logging.error(error_msg)
        raise ValueError(error_msg)

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
    sync_data_staging(f"staging_{TABLE_MAPPINGS[filter_name]}", 'name', data_frame, filter_name, company_id)

def create_staging_combination_table(**kwargs):
    ti = kwargs['ti']
    company_id = ti.xcom_pull(task_ids='fetch_company_and_queries', key='company_id')
    create_media_relation_staging_table(company_id)
    logging.info(f"Created staging table: media_relation_staging")


with DAG(
    'ETL_Pipeline',
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

    process_tasks = {}
    decide_tasks = {}
    insert_tasks = {}
    skip_insert_tasks = {}
    quality_checks = {}
    create_staging_tasks = {}
    move_to_prod_tasks = {}

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

        staging_table = f'staging_{TABLE_MAPPINGS[filter_name]}'
        create_staging_tasks[filter_name] = PythonOperator(
            task_id=f'create_staging_{filter_name}',
            python_callable=create_staging_table,
            op_kwargs={'production_table': TABLE_MAPPINGS[filter_name], 'staging_table': staging_table}
        )

        # move_to_prod_tasks[filter_name] = PythonOperator(
        #     task_id=f'move_to_production_{filter_name}',
        #     python_callable=move_table_staging_to_production,
        #     op_kwargs={'table_name': TABLE_MAPPINGS[filter_name], 'company_id': '{{ ti.xcom_pull(task_ids="fetch_company_and_queries", key="company_id") }}'},
        #     provide_context=True
        # )

        start >> fetch_info >> create_staging_tasks[filter_name] >> process_tasks[filter_name] >> decide_tasks[filter_name] 
        decide_tasks[filter_name] >> [skip_insert_tasks[filter_name], insert_tasks[filter_name]]
        insert_tasks[filter_name] >> quality_checks[filter_name]

    create_combination_staging = PythonOperator(
        task_id='create_combination_staging',
        python_callable=create_staging_combination_table,
        provide_context=True
    )

    handle_combinations_staging = PythonOperator(
        task_id='handle_media_combinations_staging',
        python_callable=handle_media_combinations_staging,
        op_kwargs={'staging_table': 'staging_media_relations', 'company_id': '{{ ti.xcom_pull(task_ids="fetch_company_and_queries", key="company_id") }}'},
        provide_context=True
    )

    # Ensure quality checks lead to staging combination handling
    for filter_name in TABLE_MAPPINGS.keys():
        quality_checks[filter_name] >> create_combination_staging >> handle_combinations_staging
 

