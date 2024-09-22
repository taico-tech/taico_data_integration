# import logging
# from airflow import DAG
# from airflow.operators.python import PythonOperator, BranchPythonOperator
# from airflow.operators.dummy import DummyOperator
# from datetime import datetime, timedelta
# import sys

# sys.path.append('/opt/airflow/include/scripts')

# from include.scripts.sync_data import insert_if_not_exists, process_filter, fetch_company_info, TABLE_MAPPINGS, construct_queries
# from include.scripts.data_quality_checks import validate_schema, validate_non_null, validate_unique, validate_sync

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# def fetch_company_and_queries(**kwargs):
#     """Fetch company ID and table name and push to XCom."""
#     alias = 'test_company'  # You can change this to a dynamic value if needed
#     company_id, bq_table = fetch_company_info(alias)
#     queries = construct_queries(bq_table)
#     kwargs['ti'].xcom_push(key='company_id', value=company_id)
#     kwargs['ti'].xcom_push(key='queries', value=queries)
#     kwargs['ti'].xcom_push(key='alias', value=alias)

# def decide_next_step(**kwargs):
#     """Determine the next step based on the presence of data."""
#     filter_name = kwargs['filter_name']
#     result = kwargs['ti'].xcom_pull(task_ids=f'process_{filter_name}')
#     if result and result.get('status') == 'skip':
#         return f'skip_insert_{filter_name}'
#     else:
#         return f'insert_{filter_name}'

# def validate_data_quality(**kwargs):
#     """Perform data quality checks."""
#     filter_name = kwargs['filter_name']
#     company_id = kwargs['ti'].xcom_pull(task_ids='fetch_company_and_queries', key='company_id')
#     table_name = TABLE_MAPPINGS[filter_name]
    
#     # Pull the processed DataFrame from XCom
#     df = kwargs['ti'].xcom_pull(task_ids=f'process_{filter_name}')
    
#     if df:
#         try:
#             validate_schema(table_name, df)
#             validate_non_null(table_name, df)
#             validate_unique(table_name, df)
#             validate_sync(table_name, company_id, df)
#             return 'data_quality_pass'
#         except Exception as e:
#             logging.error(f"Data quality check failed: {e}")
#             return 'data_quality_fail'
#     else:
#         return 'data_quality_fail'

# def data_quality_pass(**kwargs):
#     """Handle the case where data quality checks pass."""
#     logging.info(f"Data quality checks passed for {kwargs['filter_name']}.")

# def data_quality_fail(**kwargs):
#     """Handle the case where data quality checks fail."""
#     logging.error(f"Data quality checks failed for {kwargs['filter_name']}.")

# with DAG(
#     'ETL_Pipeline',
#     default_args=default_args,
#     description='An ETL pipeline from BigQuery to PostgreSQL with data quality checks',
#     start_date=datetime(2023, 5, 12),
#     catchup=False,
#     schedule_interval='@hourly',
# ) as dag:

#     start = DummyOperator(task_id='start')

#     fetch_info = PythonOperator(
#         task_id='fetch_company_and_queries',
#         python_callable=fetch_company_and_queries,
#         provide_context=True
#     )

#     for filter_name in TABLE_MAPPINGS.keys():
#         process_task = PythonOperator(
#             task_id=f'process_{filter_name}',
#             python_callable=process_filter,
#             op_args=[
#                 filter_name,
#                 '{{ task_instance.xcom_pull(task_ids="fetch_company_and_queries", key="queries")["' + filter_name + '"] }}',
#                 '{{ task_instance.xcom_pull(task_ids="fetch_company_and_queries", key="company_id") }}'
#             ],
#             provide_context=True
#         )

#         decide_task = BranchPythonOperator(
#             task_id=f'decide_{filter_name}',
#             python_callable=decide_next_step,
#             provide_context=True,
#             op_kwargs={'filter_name': filter_name}
#         )

#         insert_task = PythonOperator(
#             task_id=f'insert_{filter_name}',
#             python_callable=insert_if_not_exists,
#             op_args=[
#                 TABLE_MAPPINGS[filter_name],  # Table name directly
#                 'name',
#                 '{{ task_instance.xcom_pull(task_ids="process_' + filter_name + '") }}',
#                 filter_name,
#                 '{{ task_instance.xcom_pull(task_ids="fetch_company_and_queries", key="company_id") }}'
#             ],
#             provide_context=True
#         )

#         skip_insert_task = DummyOperator(task_id=f'skip_insert_{filter_name}')

#         quality_check = PythonOperator(
#             task_id=f'validate_{filter_name}',
#             python_callable=validate_data_quality,
#             op_kwargs={'filter_name': filter_name},
#             provide_context=True
#         )

#         quality_pass = PythonOperator(
#             task_id=f'data_quality_pass_{filter_name}',
#             python_callable=data_quality_pass,
#             op_kwargs={'filter_name': filter_name},
#             provide_context=True
#         )

#         quality_fail = PythonOperator(
#             task_id=f'data_quality_fail_{filter_name}',
#             python_callable=data_quality_fail,
#             op_kwargs={'filter_name': filter_name},
#             provide_context=True
#         )

#         # Define task dependencies
#         start >> fetch_info >> process_task >> decide_task
#         decide_task >> [insert_task, skip_insert_task]

#         insert_task >> quality_check
#         skip_insert_task >> quality_check

#         quality_check >> [quality_pass, quality_fail]
