from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
import pandas as pd
from google.cloud import bigquery
from sqlalchemy import create_engine
import psycopg2

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'bigquery_to_postgres_unique_filters',
    default_args=default_args,
    description='Extract distinct filters from BigQuery and load unique values into PostgreSQL',
    schedule_interval='@daily',  # Adjust as needed
)

def extract_distinct_values(**kwargs):
    client = bigquery.Client()
    tables = {
    'channel': 'SELECT DISTINCT channel FROM `mms.main_mms` WHERE channel IS NOT NULL',
    'publisher': 'SELECT DISTINCT publisher FROM `mms.main_mms` WHERE publisher IS NOT NULL',
    'campaign': 'SELECT DISTINCT campaign FROM `mms.main_mms` WHERE campaign IS NOT NULL',
    'media_type': 'SELECT DISTINCT media_type FROM `mms.main_mms` WHERE media_type IS NOT NULL',
    'media_cluster': 'SELECT DISTINCT media_cluster FROM `mms.main_mms` WHERE media_cluster IS NOT NULL',
    'property': 'SELECT DISTINCT property FROM `mms.main_mms` WHERE property IS NOT NULL',
    'audience': 'SELECT DISTINCT audience FROM `mms.main_mms` WHERE audience IS NOT NULL',
    'product': 'SELECT DISTINCT product FROM `mms.main_mms` WHERE product IS NOT NULL',
    'product_group': 'SELECT DISTINCT product_group FROM `mms.main_mms` WHERE product_group IS NOT NULL'
    }
    results = {}
    for filter_name, query in tables.items():
        df = client.query(query).to_dataframe()
        results[filter_name] = df
    return results

def fetch_existing_values(engine, table_name):
    with engine.connect() as conn:
        query = f"SELECT name FROM {table_name};"
        result = conn.execute(query)
        existing_values = {row[0] for row in result}
    return existing_values

def insert_filters_into_postgres(filters, **kwargs):
    engine = create_engine('postgresql://your_user:your_password@your_host:your_port/your_db')
    
    for filter_name, df in filters.items():
        table_name = f'taico.public.{filter_name}s'  # Assuming table names are like 'channels', 'publishers', etc.
        df = df.rename(columns={filter_name: 'name'})  # Rename column to 'name' if needed
        df.dropna(subset=['name'], inplace=True)  # Drop rows where 'name' is NaN
        
        existing_values = fetch_existing_values(engine, table_name)
        new_values = df[~df['name'].isin(existing_values)]

        if not new_values.empty:
            new_values.to_sql(table_name, engine, if_exists='append', index=False, method='multi')

def main(**kwargs):
    filters = extract_distinct_values(**kwargs)
    # insert_filters_into_postgres(filters, **kwargs)

start = DummyOperator(task_id='start', dag=dag)
main_task = PythonOperator(task_id='main', python_callable=main, provide_context=True, dag=dag)
end = DummyOperator(task_id='end', dag=dag)

start >> main_task >> end
