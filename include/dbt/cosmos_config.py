# include/dbt/cosmos_config.py

from cosmos.config import ProfileConfig, ProjectConfig
from pathlib import Path

DBT_CONFIG = ProfileConfig(
    profile_name='google_cloud_bigquery',
    target_name='dev',
    profiles_yml_filepath=Path('/usr/local/airflow/include/dbt/profiles.yml')
)

# DBT_CONFIG_TAICO= ProfileConfig(
#     profile_name='google_cloud_bigquery',
#     target_name='dev',
#     profiles_yml_filepath=Path('/usr/local/airflow/include/dbt/profiles.yml')
# )

DBT_PROJECT_CONFIG = ProjectConfig(
    dbt_project_path='/usr/local/airflow/include/dbt/',
)