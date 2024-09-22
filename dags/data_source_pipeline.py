from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import timedelta
import logging
import requests
import pandas as pd
from google.cloud import bigquery

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email': 'your-email@example.com',
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'facebook_etl_pipeline_with_staging',
    default_args=default_args,
    description='ETL pipeline for Facebook data with staging, validation, and alerts',
    schedule_interval=timedelta(days=1),
    catchup=False  # Disables backfilling
)

# Configure logging
logging.basicConfig(level=logging.INFO)

def extract_facebook_data(**kwargs):
    """
    Extract data from the Facebook API and load into BigQuery staging table.
    """
    try:
        logging.info("Starting data extraction from Facebook API.")
        
        # Example: Fetch data using Facebook API
        # api_url = "https://graph.facebook.com/v11.0/your_endpoint"
        # params = {'access_token': 'your_access_token', 'fields': 'id,name'}
        # response = requests.get(api_url, params=params)
        
        # Simulated response for illustration
        # response_json = response.json()  # Use response.json() in real code
        response_json = [{
            'ad_id': 'mock_ad_id_1',
            'campaign_id': 'mock_campaign_id_1',
            'created_date': '2024-08-08',
            'clicks': 10,
            'impressions': 100,
            'unique_clicks': 8,
            'spend': 50.0,
            'cpm': 5.0,
            'cpc': 0.5,
            'conversion_values': 0.0
        }]
        
        # Convert to DataFrame and write to BigQuery staging table
        df = pd.DataFrame(response_json)
        client = bigquery.Client()
        table_id = 'primordial-ship-332419.dummy_integration_staging.facebook_ads_staging'
        
        job = client.load_table_from_dataframe(df, table_id)
        job.result()  # Wait for the job to complete
        logging.info("Data loaded into BigQuery staging table successfully.")
    except Exception as e:
        logging.error(f"Failed to extract and stage data: {e}")
        raise

def validate_data_quality(**kwargs):
    """
    Validate data quality in the staging table.
    """
    try:
        logging.info("Starting data validation.")
        # Define data quality checks
        checks = [
            {
                'sql': 'SELECT COUNT(*) FROM `primordial-ship-332419.dummy_integration_staging.facebook_ads_staging` WHERE ad_id IS NULL',
                'expected_result': 0,
                'description': 'Check for null ad_id'
            },
            {
                'sql': 'SELECT COUNT(*) FROM `primordial-ship-332419.dummy_integration_staging.facebook_ads_staging` WHERE clicks < 0',
                'expected_result': 0,
                'description': 'Check for negative clicks'
            },
            {
                'sql': 'SELECT COUNT(*) FROM `primordial-ship-332419.dummy_integration_staging.facebook_ads_staging`',
                'expected_result': lambda count: count > 0,
                'description': 'Check that table is not empty'
            }
            # Add more checks as necessary
        ]
        
        client = bigquery.Client()
        
        # Execute checks
        for check in checks:
            logging.info(f"Running check: {check['description']}")
            query_job = client.query(check['sql'])
            result = query_job.result()
            count = [row[0] for row in result][0]
            
            if isinstance(check['expected_result'], int):
                assert count == check['expected_result'], f"Failed {check['description']}: {count}"
            elif callable(check['expected_result']):
                assert check['expected_result'](count), f"Failed {check['description']}: {count}"
            
            logging.info(f"Check passed: {check['description']}")
        
        logging.info("Data validation completed successfully.")
    except Exception as e:
        logging.error(f"Data validation failed: {e}")
        raise

def perform_dbt_run():
    """
    Transform data using dbt models.
    """
    try:
        logging.info("Running dbt transformations.")
        dbt_command = 'cd /usr/local/airflow/include/dbt && dbt run --models transform_facebook.facebook_ads_transformed'
        result = BashOperator(
            task_id='dbt_run',
            bash_command=dbt_command,
            dag=dag
        ).execute(context={})
        logging.info("dbt transformations completed successfully.")
    except Exception as e:
        logging.error(f"dbt transformations failed: {e}")
        raise

def load_to_production():
    """
    Load validated and transformed data into the production table in BigQuery.
    """
    try:
        logging.info("Loading data into BigQuery production table.")
        production_table = 'primordial-ship-332419.dummy_integration_staging.main_test_company'
        transformed_table = 'primordial-ship-332419.dummy_integrations.facebook_ads_transformed'
        
        bq_operator = BigQueryOperator(
            task_id='load_to_production',
            sql=f'''
                CREATE OR REPLACE TABLE `{production_table}` AS
                SELECT * FROM `{transformed_table}`
            ''',
            use_legacy_sql=False,
            dag=dag
        )
        bq_operator.execute(context={})
        logging.info("Data loaded into BigQuery production table successfully.")
    except Exception as e:
        logging.error(f"Failed to load data into production table: {e}")
        raise

def send_success_email(**kwargs):
    """
    Send an email notification on successful completion of the ETL pipeline.
    """
    email_operator = EmailOperator(
        task_id='send_email',
        to='your-email@example.com',
        subject='ETL Pipeline Success',
        html_content='The ETL pipeline for Facebook data has completed successfully.',
        dag=dag
    )
    email_operator.execute(context=kwargs)

# Define the tasks
extract_task = PythonOperator(
    task_id='extract_facebook_data',
    python_callable=extract_facebook_data,
    provide_context=True,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    provide_context=True,
    dag=dag,
)

dbt_run_task = PythonOperator(
    task_id='transform_data_with_dbt',
    python_callable=perform_dbt_run,
    dag=dag,
)

load_to_prod_task = PythonOperator(
    task_id='load_to_production',
    python_callable=load_to_production,
    dag=dag,
)

send_email_task = PythonOperator(
    task_id='send_success_email',
    python_callable=send_success_email,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
extract_task >> validate_task >> dbt_run_task >> load_to_prod_task >> send_email_task
