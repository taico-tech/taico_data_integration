# Data Pipeline with Astro, DBT, and Airflow on GCP

## Project Overview
This project demonstrates the creation of an automated data pipeline using Apache Airflow, DBT for data modeling, and Python scripts for data validation. The pipeline extracts, validates, transforms, and loads data into Google BigQuery.

## Project Goals
The goal is to build a scalable data pipeline integrated with GCP. DBT is used for data transformations, while custom Python scripts are used for validation to ensure data quality before processing.

## Architecture Overview
This project follows these steps:
1. **Data Ingestion**: Data is pulled into Google BigQuery from different sources.
2. **Data Validation**: Python scripts verify the accuracy and structure of the data.
3. **Data Transformation**: DBT models transform the raw data into a star schema.
4. **Data Loading**: The processed data is loaded into BigQuery for storage and analysis.

## Tools and Technologies
- **BigQuery (GCP)**: A managed, serverless data warehouse used for querying large datasets.
- **DBT**: Used for modeling and SQL-based transformations.
- **Astro CLI**: Provides an environment for orchestrating Apache Airflow locally.
- **Docker**: Containerizes the development environment for consistency across different environments.
- **Python Scripts**: Used for data validation.
  
## Steps to Run the Project

### 1. Install Astro CLI
Follow [Astro CLI's installation guide](https://docs.astronomer.io/astro/cli/install-cli) to set up Astro locally. Ensure Docker is installed as Astro uses it to containerize Airflow.

### 2. Clone the Repository
```bash
git clone <repository_url>
cd taico_data_integration
```markdown
# Data Pipeline with Astro, DBT, and Airflow on GCP

```
### 3. Configure Environment Variables
Set up your GCP and Airbyte connection credentials in the `include/airbyte_variables.json` file. Include your service account key for GCP in the `gcp` folder under `include`.

### Example airbyte_variables.json:

```json
{
   "airbyte_server_host": "localhost",
   "airbyte_server_port": "8000",
   "airbyte_connection_id": "<your_connection_id>"
}
```
Place your GCP service account key in the `gcp/` folder under `include`. Then, update the Airflow environment to include the path to this service account key. Add the following to your `.env` file:

```bash
GOOGLE_APPLICATION_CREDENTIALS=/usr/local/airflow/include/gcp/service_account_key.json
```

## 4. Install Project Dependencies
To install all necessary dependencies, run:

```bash
pip install -r requirements.txt
```

This will install required packages, including:

```bash
apache-airflow[gcp]==2.3.0
dbt-bigquery==1.0.0
pandas==1.3.3
google-cloud-bigquery==2.24.0
```

## 5. Start Airflow Environment with Astro
To start the Airflow environment with Astro, use:

```bash
astro dev start
```

You can access the Airflow UI at [http://localhost:8080](http://localhost:8080). The Airflow UI allows you to monitor, configure, and trigger DAGs.

## Setting Up the DAGs
A DAG (Directed Acyclic Graph) in Airflow represents a sequence of tasks, such as data ingestion, transformation, and validation.

### 1. Define the DAG
To create a DAG, add a Python file under the `dags/` folder. The DAG defines tasks like uploading data to GCS, transforming it with DBT, or running validation scripts.

#### Example:
```python
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

@dag(schedule_interval="@daily", start_date=days_ago(1))
def my_data_pipeline():
    upload_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_csv_to_gcs',
        src='/usr/local/airflow/include/dataset/online_retail.csv',
        dst='raw/online_retail.csv',
        bucket='my_bucket',
        gcp_conn_id='my_gcp_conn'
    )

my_data_pipeline()
```

This DAG uploads a CSV file to Google Cloud Storage (GCS). Add more tasks for validation, transformation, etc.

### 2. Scheduling the DAG
Set the `schedule_interval` when creating your DAG. You can use cron expressions ('0 12 * * *') or presets like `@daily` or `@hourly`.

### 3. Triggering the DAG
You can manually trigger DAGs from the Airflow UI, or they will run based on the schedule you define.

### 4. Python Data Validation Task
Add Python scripts under the `scripts/` directory to validate data before processing. For example:

```python
@task
def validate_data():
    import pandas as pd
    data = pd.read_csv('/usr/local/airflow/include/dataset/online_retail.csv')
    if data.isnull().values.any():
        raise ValueError("Data contains null values")
    return "Validation passed"
```

In the DAG, you can call this validation task:

```python
with DAG('my_data_pipeline', schedule_interval="@daily") as dag:
    validation_task = validate_data()
    upload_csv_to_gcs >> validation_task
```

## Data Transformation Using DBT
DBT (Data Build Tool) is used to transform and structure the data. All DBT SQL models are stored under the `dbt/` folder.

### 1. Setting Up DBT
- `dbt_project.yml`: Defines the project configuration.
- `profiles.yml`: Specifies connection details to BigQuery.
- `models/transform/`: Contains SQL files for DBT transformations.

### 2. Running DBT Locally
Run DBT commands locally by first activating the DBT environment:

```bash
astro dev bash
source dbt_venv/bin/activate
dbt run
```

This command runs all DBT models to transform your data. After running, the data will be stored in BigQuery.

### 3. Integrating DBT with Airflow
You can run DBT models directly in Airflow using Cosmos for better DBT and DAG integration:

```python
from cosmos.task_group import DbtTaskGroup

transform = DbtTaskGroup(
    group_id="transform",
    project_dir="/usr/local/airflow/dbt",
    profiles_dir="/usr/local/airflow/dbt",
)
```

Add the DBT transformation step to the DAG to make it part of the overall workflow.

### DAG Chain Setup
Finally, define the sequence of tasks in your DAG to ensure proper execution order:

```python
upload_csv_to_gcs >> validate_data() >> transform
```

This ensures that data is uploaded to GCS, validated, transformed, and finally loaded into BigQuery.

## Running the Full Pipeline
Once the DAG is set up and all tasks are defined:

1. Start Airflow (`astro dev start`).
2. Access the UI at `localhost:8080`.
3. Trigger the DAG manually or let it run based on the defined schedule.
```

