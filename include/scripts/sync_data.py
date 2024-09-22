import logging
import os
from google.cloud import bigquery
from sqlalchemy import create_engine, text
from airflow.hooks.base import BaseHook
import pandas as pd

from include.scripts.config import STAGING_TABLES, TABLE_MAPPINGS

# Import configuration settings

logging.basicConfig(level=logging.INFO)

def get_bigquery_client():
    """Fetch BigQuery client."""
    connection = BaseHook.get_connection('gcp')
    key_path = "/usr/local/airflow/include/gcp/service_account.json"
    if not key_path:
        raise ValueError("Google Cloud key path not set in Airflow connection 'gcp'.")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
    return bigquery.Client()

def get_postgres_conn():
    """Fetch PostgreSQL connection details."""
    connection = BaseHook.get_connection('postgres')
    return f"postgresql+psycopg2://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}"

def get_dynamic_values():
    """Fetch company ID and table name, and initialize engine."""
    engine = create_engine(get_postgres_conn())
    return engine


def create_staging_table(production_table, staging_table):
    """Create a staging table by dropping it if it exists, then copying the structure and data of the production table."""
    try:
        engine = get_dynamic_values()
        with engine.connect() as connection:
            # Step 1: Drop the staging table if it exists
            drop_table_query = text(f"""
                DROP TABLE IF EXISTS {staging_table}
            """)
            connection.execute(drop_table_query)
            logging.info(f"Staging table {staging_table} dropped if it existed.")

            # Step 2: Create the table structure
            create_structure_query = text(f"""
                CREATE TABLE {staging_table} (
                    id INT,           -- Assuming INT, adjust if needed
                    name VARCHAR,     -- Assuming VARCHAR, adjust length if needed
                    company_id INT    -- Assuming INT, adjust if needed
                )
            """)

            connection.execute(create_structure_query)
            logging.info(f"Staging table {staging_table} created with structure from {production_table}.")

            # Step 3: Copy data from production table to staging table
            copy_data_query = text(f"""
                INSERT INTO {staging_table} SELECT * FROM {production_table}
            """)
            connection.execute(copy_data_query)
            logging.info(f"Data copied from {production_table} to {staging_table}.")
    
    except Exception as e:
        logging.error(f"Error creating and populating staging table {staging_table}: {e}")
        raise






def create_media_relation_staging_table(company_id):
    """Create a media_relations_staging table as a copy of media_relations, filtered by company_id."""
    try:
        engine = get_dynamic_values()
        with engine.connect() as connection:
            # Step 1: Drop the staging table if it exists
            drop_table_query = text("""
                DROP TABLE IF EXISTS media_relations_staging
            """)
            connection.execute(drop_table_query)
            logging.info("Dropped existing media_relations_staging table if it existed.")

            # Step 2: Create the table structure using LIKE clause
            create_structure_query = text("""
                CREATE TABLE media_relations_staging (LIKE media_relations INCLUDING ALL)
            """)
            connection.execute(create_structure_query)
            logging.info("Created media_relations_staging table with the structure of media_relations.")

            # Step 3: Insert data from media_relation to media_relation_staging for the specified company_id
            copy_data_query = text("""
                INSERT INTO media_relations_staging (date, channel_id, publisher_id, campaign_id, media_type_id, media_cluster_id, property_id, audience_id, product_id, product_group_id, company_id)
                SELECT date, channel_id, publisher_id, campaign_id, media_type_id, media_cluster_id, property_id, audience_id, product_id, product_group_id, company_id
                FROM media_relations
            """)
            connection.execute(copy_data_query, {'company_id': company_id})
            logging.info(f"Copied data from media_relations to media_relations_staging for company_id: {company_id}.")

    except Exception as e:
        logging.error(f"Error creating media_relations_staging table: {e}")
        raise

def handle_media_combinations_staging(**kwargs):
    """Handle media combinations in the staging environment."""
    ti = kwargs['ti']
    
    # Fetch company ID and BigQuery table name from XCom
    company_id = ti.xcom_pull(task_ids='fetch_company_and_queries', key='company_id')
    bq_table = ti.xcom_pull(task_ids='fetch_company_and_queries', key='bq_table')

    # Create a SQLAlchemy engine
    engine = create_engine(get_postgres_conn())

    # Query distinct combinations from BigQuery
    query = f"""
        SELECT DISTINCT date, channel, publisher, campaign, media_type, media_cluster, property, audience, product, product_group
        FROM `{bq_table}`;
    """
    res_df = extract_data(query)  # Fetch the DataFrame from BigQuery

    # Fetch existing records from PostgreSQL staging tables
    with engine.connect() as connection:
        channels = pd.read_sql(text("SELECT id AS channel_id, name FROM staging_channels WHERE company_id = :company_id"), connection, params={'company_id': company_id})
        publishers = pd.read_sql(text("SELECT id AS publisher_id, name FROM staging_publishers WHERE company_id = :company_id"), connection, params={'company_id': company_id})
        audiences = pd.read_sql(text("SELECT id AS audience_id, name FROM staging_audiences WHERE company_id = :company_id"), connection, params={'company_id': company_id})
        products = pd.read_sql(text("SELECT id AS product_id, name FROM staging_products WHERE company_id = :company_id"), connection, params={'company_id': company_id})
        product_groups = pd.read_sql(text("SELECT id AS product_group_id, name FROM staging_product_groups WHERE company_id = :company_id"), connection, params={'company_id': company_id})
        campaigns = pd.read_sql(text("SELECT id AS campaign_id, name FROM staging_campaigns WHERE company_id = :company_id"), connection, params={'company_id': company_id})
        media_types = pd.read_sql(text("SELECT id AS media_type_id, name FROM staging_media_types WHERE company_id = :company_id"), connection, params={'company_id': company_id})
        media_clusters = pd.read_sql(text("SELECT id AS media_cluster_id, name FROM staging_media_clusters WHERE company_id = :company_id"), connection, params={'company_id': company_id})
        properties = pd.read_sql(text("SELECT id AS property_id, name FROM staging_properties WHERE company_id = :company_id"), connection, params={'company_id': company_id})

    # Merge data frames
    res_df = (res_df
              .merge(channels, left_on="channel", right_on="name", suffixes=('', '_channel'))
              .merge(publishers, left_on="publisher", right_on="name", suffixes=('', '_publisher'))
              .merge(campaigns, left_on="campaign", right_on="name", suffixes=('', '_campaign'))
              .merge(media_types, left_on="media_type", right_on="name", suffixes=('', '_media_type'))
              .merge(media_clusters, left_on="media_cluster", right_on="name", suffixes=('', '_media_cluster'))
              .merge(properties, left_on="property", right_on="name", suffixes=('', '_property'))
              .merge(audiences, left_on="audience", right_on="name", suffixes=('', '_audience'))
              .merge(products, left_on="product", right_on="name", suffixes=('', '_product'))
              .merge(product_groups, left_on="product_group", right_on="name", suffixes=('', '_product_group')))

    # Select columns and drop duplicates
    res_df = res_df[[
        "date", "channel_id", "publisher_id", "campaign_id", 
        "media_type_id", "media_cluster_id", "property_id", 
        "audience_id", "product_id", "product_group_id"
    ]].drop_duplicates()
    res_df["company_id"] = company_id

    # Insert new combinations into media_relation_staging
    with engine.connect() as connection:
        # Delete old rows from media_relation_staging
        delete_query = text("""
            DELETE FROM media_relations_staging
            WHERE company_id = :company_id
        """)
        connection.execute(delete_query, company_id=company_id)
        logging.info(f"Deleted old rows for company_id: {company_id} from media_relations_staging")

        # Insert new data into media_relation_staging
        insert_query = text("""
            INSERT INTO media_relations_staging (date, channel_id, publisher_id, campaign_id, media_type_id, media_cluster_id, property_id, audience_id, product_id, product_group_id, company_id)
            SELECT :date, :channel_id, :publisher_id, :campaign_id, :media_type_id, :media_cluster_id, :property_id, :audience_id, :product_id, :product_group_id, :company_id
            WHERE NOT EXISTS (
                SELECT 1 FROM media_relations_staging
                WHERE date = :date
                AND channel_id = :channel_id
                AND publisher_id = :publisher_id
                AND campaign_id = :campaign_id
                AND media_type_id = :media_type_id
                AND media_cluster_id = :media_cluster_id
                AND property_id = :property_id
                AND audience_id = :audience_id
                AND product_id = :product_id
                AND product_group_id = :product_group_id
                AND company_id = :company_id
            )
        """)
        for row in res_df.itertuples(index=False):
            connection.execute(insert_query, **row._asdict())
            logging.info(f"Inserted media relation: {row} into media_relations_staging")




def fetch_company_info(alias):
    """Fetch company ID and BigQuery table name from PostgreSQL based on alias."""
    query = f"SELECT id FROM companies WHERE alias = '{alias}'"
    engine = create_engine(get_postgres_conn())
    with engine.connect() as connection:
        result = connection.execute(query).fetchone()
        if not result:
            raise ValueError(f"No company info found for alias '{alias}'.")
        company_id = result[0]  # Extracting the company_id from the result tuple
        table_name = "mms.main_mms"
        return company_id, table_name
    


def construct_queries(bq_table):
    """Construct BigQuery queries based on table name."""
    return {
        'channel': f'SELECT DISTINCT channel FROM `{bq_table}` WHERE channel IS NOT NULL',
        'publisher': f'SELECT DISTINCT publisher FROM `{bq_table}` WHERE publisher IS NOT NULL',
        'campaign': f'SELECT DISTINCT campaign FROM `{bq_table}` WHERE campaign IS NOT NULL',
        'media_type': f'SELECT DISTINCT media_type FROM `{bq_table}` WHERE media_type IS NOT NULL',
        'media_cluster': f'SELECT DISTINCT media_cluster FROM `{bq_table}` WHERE media_cluster IS NOT NULL',
        'property': f'SELECT DISTINCT property FROM `{bq_table}` WHERE property IS NOT NULL',
        'audience': f'SELECT DISTINCT audience FROM `{bq_table}` WHERE audience IS NOT NULL',
        'product': f'SELECT DISTINCT product FROM `{bq_table}` WHERE product IS NOT NULL',
        'product_group': f'SELECT DISTINCT product_group FROM `{bq_table}` WHERE product_group IS NOT NULL'
    }

client = get_bigquery_client()
COMPANY_ID = None
BQ_TABLE = None

def sync_data_staging(model, name_column, data_frame, filter_column, company_id):
    """Sync data between staging and big query  : insert, update, and delete as needed."""
    if not isinstance(data_frame, pd.DataFrame):
        raise TypeError("Expected data_frame to be a pandas DataFrame")
    
    if filter_column not in data_frame.columns:
        raise ValueError(f"Column {filter_column} does not exist in data_frame")
    
    column_data = data_frame[filter_column]
    
    if not isinstance(column_data, pd.Series):
        raise TypeError("Expected column data to be a pandas Series")
    
    try:
        engine = get_dynamic_values()  # Ensure to fetch the latest company_id and table_name
        with engine.connect() as connection:
            
            # Insert new records
            insert_query = text(f"""
                INSERT INTO {model} ({name_column}, company_id)
                SELECT :value, :company_id
                WHERE NOT EXISTS (
                    SELECT 1 FROM {model} WHERE {name_column} = :value AND company_id = :company_id
                )
            """)
            
            for value in column_data:
                connection.execute(insert_query, value=value, company_id=company_id)
                logging.info(f"Inserted value: {value} into {model}")

            # Find records to delete
            existing_data_query = text(f"""
                SELECT {name_column} FROM {model}
                WHERE company_id = :company_id
            """)
            existing_data_df = pd.read_sql(existing_data_query, connection, params={'company_id': company_id})

            # Identify records to delete
            to_delete_df = existing_data_df[~existing_data_df[name_column].isin(column_data)]
            if not to_delete_df.empty:
                delete_query = text(f"""
                    DELETE FROM {model}
                    WHERE {name_column} IN :values AND company_id = :company_id
                """)
                connection.execute(delete_query, values=(tuple(to_delete_df[name_column]),), company_id=company_id)
                logging.info(f"Deleted {len(to_delete_df)} records from {model}")
            
    except Exception as e:
        logging.error(f"Error syncing data in PostgreSQL: {e}")
        raise







def move_table_staging_to_production(table_name, company_id):
    """Move data from staging to production for a given table and company_id."""
    try:
        engine = get_dynamic_values()
        with engine.connect() as connection:
            staging_table = f"staging_{table_name}"
            production_table = f"production_{table_name}"

            # Delete old production rows for the company_id
            delete_query = text(f"""
                DELETE FROM {production_table}
                WHERE company_id = :company_id
            """)
            connection.execute(delete_query, company_id=company_id)
            logging.info(f"Deleted old rows for company_id: {company_id} from {production_table}")

            # Insert new data from staging to production
            insert_query = text(f"""
                INSERT INTO {production_table} (id, name, company_id)
                SELECT id, name, company_id
                FROM {staging_table}
                WHERE company_id = :company_id
            """)
            connection.execute(insert_query, company_id=company_id)
            logging.info(f"Inserted new rows for company_id: {company_id} into {production_table}")

    except Exception as e:
        logging.error(f"Error moving data from {staging_table} to {production_table}: {e}")
        raise



def extract_data(query):
    """Extract data from BigQuery."""
    try:
        logging.info(f"Executing query: {query}")
        df = client.query(query).to_dataframe()
        
        # Debugging information
        logging.info(f"Extracted DataFrame: {df.head()}")
        logging.info(f"DataFrame type: {type(df)}")
        
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Expected df to be a pandas DataFrame")
        
        return df
    except Exception as e:
        logging.error(f"Error extracting data from BigQuery: {e}")
        raise

def insert_if_not_exists(model, name_column, data_frame, filter_column, company_id):
    """Insert data into PostgreSQL if not exists."""
    if not isinstance(data_frame, pd.DataFrame):
        raise TypeError("Expected data_frame to be a pandas DataFrame")
    
    if filter_column not in data_frame.columns:
        raise ValueError(f"Column {filter_column} does not exist in data_frame")
    
    column_data = data_frame[filter_column]
    
    if not isinstance(column_data, pd.Series):
        raise TypeError("Expected column data to be a pandas Series")
    
    try:
        engine = get_dynamic_values()  # Ensure to fetch the latest company_id and table_name
        with engine.connect() as connection:
            for value in column_data:
                insert_query = text(f"""
                    INSERT INTO {model} ({name_column}, company_id)
                    SELECT :value, :company_id
                    WHERE NOT EXISTS (
                        SELECT 1 FROM {model} WHERE {name_column} = :value AND company_id = :company_id
                    )
                """)
                connection.execute(insert_query, value=value, company_id=company_id)
                logging.info(f"Inserted value: {value} into {model}")
    except Exception as e:
        logging.error(f"Error inserting data into PostgreSQL: {e}")
        raise

def process_filter(filter_name, query, company_id):
    """Extract data based on filter_name and query."""
    logging.info(f"Processing filter: {filter_name} with query: {query}")
    
    df = extract_data(query)
    if df.empty:
        logging.info(f"No data found for filter: {filter_name}")
        return {'status': 'skip'}
    else:
        return {'status': 'insert', 'data_frame': df}


def handle_media_combinations(**kwargs):
    """Handle media combinations: fetch distinct combinations from BigQuery and insert missing ones into PostgreSQL."""
    ti = kwargs['ti']
    
    # Fetch company ID and BigQuery table name from XCom
    company_id = ti.xcom_pull(task_ids='fetch_company_and_queries', key='company_id')
    bq_table = ti.xcom_pull(task_ids='fetch_company_and_queries', key='bq_table')

    # Create a SQLAlchemy engine
    engine = create_engine(get_postgres_conn())
    
    # Query distinct combinations from BigQuery
    query = f"""
        SELECT DISTINCT date, channel, publisher, campaign, media_type, media_cluster, property, audience, product, product_group
        FROM `{bq_table}`;
    """
    res_df = extract_data(query)  # Assuming extract_data fetches the DataFrame

    # Fetch existing records from PostgreSQL
    with engine.connect() as connection:
        channels = pd.read_sql(text("SELECT id AS channel_id, name FROM channels WHERE company_id = :company_id"), connection, params={'company_id': company_id})
        publishers = pd.read_sql(text("SELECT id AS publisher_id, name FROM publishers WHERE company_id = :company_id"), connection, params={'company_id': company_id})
        audiences = pd.read_sql(text("SELECT id AS audience_id, name FROM audiences WHERE company_id = :company_id"), connection, params={'company_id': company_id})
        products = pd.read_sql(text("SELECT id AS product_id, name FROM products WHERE company_id = :company_id"), connection, params={'company_id': company_id})
        product_groups = pd.read_sql(text("SELECT id AS product_group_id, name FROM product_groups WHERE company_id = :company_id"), connection, params={'company_id': company_id})
        campaigns = pd.read_sql(text("SELECT id AS campaign_id, name FROM campaigns WHERE company_id = :company_id"), connection, params={'company_id': company_id})
        media_types = pd.read_sql(text("SELECT id AS media_type_id, name FROM media_types WHERE company_id = :company_id"), connection, params={'company_id': company_id})
        media_clusters = pd.read_sql(text("SELECT id AS media_cluster_id, name FROM media_clusters WHERE company_id = :company_id"), connection, params={'company_id': company_id})
        properties = pd.read_sql(text("SELECT id AS property_id, name FROM properties WHERE company_id = :company_id"), connection, params={'company_id': company_id})

    # Merge data frames
    res_df = (res_df
              .merge(channels, left_on="channel", right_on="name", suffixes=('', '_channel'))
              .merge(publishers, left_on="publisher", right_on="name", suffixes=('', '_publisher'))
              .merge(campaigns, left_on="campaign", right_on="name", suffixes=('', '_campaign'))
              .merge(media_types, left_on="media_type", right_on="name", suffixes=('', '_media_type'))
              .merge(media_clusters, left_on="media_cluster", right_on="name", suffixes=('', '_media_cluster'))
              .merge(properties, left_on="property", right_on="name", suffixes=('', '_property'))
              .merge(audiences, left_on="audience", right_on="name", suffixes=('', '_audience'))
              .merge(products, left_on="product", right_on="name", suffixes=('', '_product'))
              .merge(product_groups, left_on="product_group", right_on="name", suffixes=('', '_product_group')))

    # Select columns and drop duplicates
    res_df = res_df[[
        "date", "channel_id", "publisher_id", "campaign_id", 
        "media_type_id", "media_cluster_id", "property_id", 
        "audience_id", "product_id", "product_group_id"
    ]].drop_duplicates()
    res_df["company_id"] = company_id

    # Insert new combinations into PostgreSQL
    with engine.connect() as connection:
        query = text("""
            INSERT INTO media_relations (date, channel_id, publisher_id, campaign_id, media_type_id, media_cluster_id, property_id, audience_id, product_id, product_group_id, company_id)
            SELECT :date, :channel_id, :publisher_id, :campaign_id, :media_type_id, :media_cluster_id, :property_id, :audience_id, :product_id, :product_group_id, :company_id
            WHERE NOT EXISTS (
                SELECT 1 FROM media_relations
                WHERE date = :date
                AND channel_id = :channel_id
                AND publisher_id = :publisher_id
                AND campaign_id = :campaign_id
                AND media_type_id = :media_type_id
                AND media_cluster_id = :media_cluster_id
                AND property_id = :property_id
                AND audience_id = :audience_id
                AND product_id = :product_id
                AND product_group_id = :product_group_id
                AND company_id = :company_id
            )
        """)
        try:
            for row in res_df.itertuples(index=False):
                connection.execute(query, **row._asdict())
        except Exception as e:
            logging.error(f"Error executing query: {e}")

