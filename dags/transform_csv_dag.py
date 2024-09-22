from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 20),
    'retries': 1,
}

dag = DAG(
    'transform_csv_dag',
    default_args=default_args,
    schedule_interval='@daily',
)
base_path = '/usr/local/airflow/include'

transform_task = BashOperator(
    task_id='transform_csv',
    bash_command=f'python "{base_path}/scripts/transform_csv.py" "{base_path}/dataset/online_retail.csv" "{base_path}/dataset/output.csv" InvoiceNo',
    dag=dag,
)

transform_task
