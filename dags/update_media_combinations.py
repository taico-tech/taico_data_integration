from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDataOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

def print_result(**kwargs):
    ti = kwargs['ti']
    # Get the result from BigQuery task
    bigquery_result = ti.xcom_pull(task_ids='query_bigquery')
    print(f"BigQuery result: {bigquery_result}")

    # Get the result from Postgres task
    postgres_result = ti.xcom_pull(task_ids='query_postgres')
    print(f"Postgres result: {postgres_result}")

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    'bigquery_postgres_dag',
    default_args=default_args,
    description='A simple DAG to query BigQuery and Postgres',
    schedule_interval=None,
    catchup=False,
) as dag:

    query_bigquery = BigQueryGetDataOperator(
        task_id='query_bigquery',
        dataset_id='mms',
        table_id='main_mms',
        max_results=10,
        selected_fields='channel,publisher',  # specify the columns you want to select
        gcp_conn_id='gcp',  # Use your GCP connection ID
    )

    query_postgres = PostgresOperator(
        task_id='query_postgres',
        postgres_conn_id='postgres',  # Use your PostgreSQL connection ID
        sql='SELECT name, company_id FROM channels LIMIT 10;',
    )

    print_results = PythonOperator(
        task_id='print_results',
        python_callable=print_result,
        provide_context=True,
    )

    query_bigquery >> query_postgres >> print_results
