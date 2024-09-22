from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'facebook_ads_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for Facebook Ads data using Airbyte and dbt',
    schedule_interval='@monthly',
)

start = DummyOperator(
    task_id='start',
    dag=dag,
)

extract_facebook_ads = BashOperator(
    task_id='extract_facebook_ads',
    bash_command='airbyte sync --connection-id=<YOUR_CONNECTION_ID>',
    dag=dag,
)

dbt_run_stg_facebook_ads_performance = BashOperator(
    task_id='dbt_run_stg_facebook_ads_performance',
    bash_command='dbt run --models stg_facebook_ads_performance --profiles-dir <PATH_TO_DBT_PROFILE>',
    dag=dag,
)

dbt_run_stg_facebook_ads_costs = BashOperator(
    task_id='dbt_run_stg_facebook_ads_costs',
    bash_command='dbt run --models stg_facebook_ads_costs --profiles-dir <PATH_TO_DBT_PROFILE>',
    dag=dag,
)

dbt_run_fact_facebook_ads = BashOperator(
    task_id='dbt_run_fact_facebook_ads',
    bash_command='dbt run --models fact_facebook_ads --profiles-dir <PATH_TO_DBT_PROFILE>',
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

start >> extract_facebook_ads >> dbt_run_stg_facebook_ads_performance >> dbt_run_stg_facebook_ads_costs >> dbt_run_fact_facebook_ads >> end
