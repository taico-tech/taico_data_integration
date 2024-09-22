from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

base_path = '/usr/local/airflow/include'


with DAG(
    'bigquery_postgres_sync',
    default_args=default_args,
    description='A DAG to sync data from BigQuery to PostgreSQL',
    schedule_interval=None,
    catchup=False,
) as dag:

    sync_task = BashOperator(
        task_id='sync_data',
        bash_command=f'python "{base_path}/scripts/sync_data.py"'
    )

    sync_task