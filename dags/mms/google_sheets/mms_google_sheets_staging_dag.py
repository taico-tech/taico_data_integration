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
    'transform__mms_csv_dag',
    default_args=default_args,
    schedule_interval='@daily',
)

base_path = '/usr/local/airflow/include'

# Task to run mms_lb_staged.sql
dbt_run_mms_lb_staged = BashOperator(
    task_id='dbt_run_mms_lb_staged',
    bash_command='cd /usr/local/airflow/include/dbt && dbt run --models mms.google_sheets.staging.mms_lb_staged',
    dag=dag,
)

# Task to run normalized_costs.sql
dbt_run_normalized_costs = BashOperator(
    task_id='dbt_run_normalized_costs',
    bash_command='cd /usr/local/airflow/include/dbt && dbt run --models mms.google_sheets.staging.normalized_costs',
    dag=dag,
)

# Task to run staging_performance_cost_combined.sql
dbt_run_staging_performance_cost_combined = BashOperator(
    task_id='dbt_run_staging_performance_cost_combined',
    bash_command='cd /usr/local/airflow/include/dbt && dbt run --models mms.google_sheets.staging.staging_performance_cost_combined',
    dag=dag,
)

# Define the task dependencies
dbt_run_mms_lb_staged >> dbt_run_normalized_costs >> dbt_run_staging_performance_cost_combined
