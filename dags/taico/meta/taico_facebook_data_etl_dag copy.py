import logging
from airflow.decorators import dag
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from datetime import datetime, timedelta
from include.scripts.validate_extracted_data import (
    validate_null_values,
    validate_table_schema,
    validate_row_count,
    validate_duplicates
)
from include.scripts.config import (
    EXPECTED_SCHEMA_ADS_INSIGHTS,
    EXPECTED_SCHEMA_CAMPAIGNS,
    EXPECTED_SCHEMA_TRANSFORMED_DATA,
    EXPECTED_SCHEMA_STAGING_TABLE,
    EXPECTED_SCHEMA_PRODUCTION_TABLE,
    DATASET_ID,
    PROJECT_ID,
    MIN_ROWS
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

@dag(
    dag_id='taico_facebook_data_etl_pipeline',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=['facebook', 'etl', 'data_pipeline'],
)
def taico_facebook_data_etl_pipeline():
    # Task to indicate the start of the pipeline
    start_pipeline = BashOperator(
        task_id='start_pipeline',
        bash_command='echo "Starting Facebook data ETL pipeline."'
    )

    # Extraction Phase
    with TaskGroup("extraction", tooltip="Extraction tasks for Facebook data") as extraction:
        connection_id = Variable.get("TAICO_META_STAGING_AIRBYTE_CONNECTION_ID")

        # Extract Facebook data using Airbyte
        extract_facebook_data = AirbyteTriggerSyncOperator(
            task_id='extract_facebook_data',
            airbyte_conn_id='airbyte',
            connection_id=connection_id,
            asynchronous=False,
            timeout=3600,
        )

    # Validation Phase for Extracted Data
    with TaskGroup("extracted_data_validation", tooltip="Validation tasks for extracted data") as extracted_data_validation:
        # Validate ads_insights table schema
        validate_ads_insights_schema = PythonOperator(
            task_id='validate_ads_insights_schema',
            python_callable=lambda: validate_table_schema('ads_insights', EXPECTED_SCHEMA_ADS_INSIGHTS),
        )

        # Validate campaigns table schema
        validate_campaigns_schema = PythonOperator(
            task_id='validate_campaigns_schema',
            python_callable=lambda: validate_table_schema('campaigns', EXPECTED_SCHEMA_CAMPAIGNS),
        )

    # Transformation Phase
    with TaskGroup("transformation", tooltip="Transformation tasks for Facebook data") as transformation:
        # Run dbt transformation to create transformed_facebook_data table
        dbt_create_transformed_facebook_data_table = BashOperator(
            task_id='dbt_create_transformed_facebook_data_table',
            bash_command='cd /usr/local/airflow/include/dbt && dbt run --models taico.meta.fb_meta_ads_transformed',
        )

    # Validation Phase for Transformed Data
    with TaskGroup("transformed_data_validation", tooltip="Validation tasks for transformed Facebook data") as transformed_data_validation:
        # Validate transformed_facebook_data table schema
        validate_transformed_data_schema = PythonOperator(
            task_id='validate_transformed_data_schema',
            python_callable=lambda: validate_table_schema('fb_meta_ads_transformed', EXPECTED_SCHEMA_TRANSFORMED_DATA),
        )

        # Validate transformed_facebook_data table row count
        validate_transformed_data_row_count = PythonOperator(
            task_id='validate_transformed_data_row_count',
            python_callable=lambda: validate_row_count('fb_meta_ads_transformed', MIN_ROWS),
        )

        # Validate transformed_facebook_data table for null values
        validate_transformed_data_null_values = PythonOperator(
            task_id='validate_transformed_data_null_values',
            python_callable=lambda: validate_null_values('fb_meta_ads_transformed', ['id', 'date', 'clicks', 'impressions']),
        )

        # Validate transformed_facebook_data table for duplicates
        validate_transformed_data_duplicates = PythonOperator(
            task_id='validate_transformed_data_duplicates',
            python_callable=lambda: validate_duplicates('fb_meta_ads_transformed'),
        )

    # Loading Phase (Staging and Production)
    with TaskGroup("loading", tooltip="Loading tasks to move data to staging and production") as loading:
        # Staging Environment
        with TaskGroup("staging_environment", tooltip="Staging environment tasks") as staging_environment:
            # Load data into main_taico_staging table
            create_insert_into_staging_table = BashOperator(
                task_id='create_insert_into_main_taico_staging',
                bash_command='cd /usr/local/airflow/include/dbt && dbt run --models taico.meta.main_taico_staging',
            )

            # Validate staging table schema
            validate_staging_table_schema = PythonOperator(
                task_id='validate_staging_table_schema',
                python_callable=lambda: validate_table_schema('main_taico_staging', EXPECTED_SCHEMA_STAGING_TABLE),
            )

            # Ensure staging table is validated
            create_insert_into_staging_table >> validate_staging_table_schema

        # Switch to Production
        with TaskGroup("switch_to_production", tooltip="Switch staging data to production") as switch_to_production:
            # Truncate production table
            truncate_production_table = BashOperator(
                task_id='truncate_production_table',
                bash_command=(
                    'cd /usr/local/airflow/include/dbt && '
                    'dbt run-operation truncate_table --args \'{"project_id": "' + PROJECT_ID + '", "dataset_id": "' + DATASET_ID + '", "table_name": "main_taico_production"}\''
                )
            )

            # Insert data from staging to production
            insert_data_to_production = BashOperator(
                task_id='insert_data_to_production',
                bash_command=(
                    'cd /usr/local/airflow/include/dbt && '
                    'dbt run-operation insert_data --args \'{"source_project_id": "' + PROJECT_ID + '", "source_dataset_id": "' + DATASET_ID + '", "source_table": "main_taico_staging", '
                    '"target_project_id": "' + PROJECT_ID + '", "target_dataset_id": "' + DATASET_ID + '", "target_table": "main_taico_production"}\''
                )
            )

            truncate_production_table >> insert_data_to_production

    # Validation Phase for Production Data
    with TaskGroup("production_data_validation", tooltip="Validation tasks for production data") as production_data_validation:
        # Validate production table schema
        validate_production_table_schema = PythonOperator(
            task_id='validate_production_table_schema',
            python_callable=lambda: validate_table_schema('main_taico_production', EXPECTED_SCHEMA_PRODUCTION_TABLE),
        )

        # Validate production table row count
        validate_production_table_row_count = PythonOperator(
            task_id='validate_production_table_row_count',
            python_callable=lambda: validate_row_count('main_taico_production', MIN_ROWS),
        )

        # Validate production table for null values
        validate_production_table_null_values = PythonOperator(
            task_id='validate_production_table_null_values',
            python_callable=lambda: validate_null_values('main_taico_production', ['id', 'date', 'clicks', 'impressions']),
        )

        # Validate production table for duplicates
        validate_production_table_duplicates = PythonOperator(
            task_id='validate_production_table_duplicates',
            python_callable=lambda: validate_duplicates('main_taico_production'),
        )

        # Ensure all validations pass before dropping staging table
        validate_production_table_schema >> validate_production_table_row_count
        validate_production_table_row_count >> validate_production_table_null_values
        validate_production_table_null_values >> validate_production_table_duplicates

    # Drop Staging Table
    drop_staging_table = BashOperator(
        task_id='drop_staging_table',
        bash_command=(
            'cd /usr/local/airflow/include/dbt && '
            'dbt run-operation drop_table --args \'{"project_id": "' + PROJECT_ID + '", "dataset_id": "' + DATASET_ID + '", "table_name": "main_taico_staging"}\''
        )
    )

    # Task to indicate the end of the pipeline
    end_pipeline = BashOperator(
        task_id='end_pipeline',
        bash_command='echo "Facebook data ETL pipeline completed successfully."'
    )

    # Define Task Dependencies
    start_pipeline >> extraction >> extracted_data_validation >> transformation >> transformed_data_validation
    transformed_data_validation >> loading
    staging_environment >> switch_to_production >> production_data_validation >> drop_staging_table
    drop_staging_table >> end_pipeline

# Instantiate the DAG
taico_facebook_data_etl_pipeline()
