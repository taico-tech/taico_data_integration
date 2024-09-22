# from airflow.decorators import dag, task
# from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
# from datetime import datetime 

# AIRBYTE_ID_LOAD_CUSTOMER_TRANSACTION_RAW='ef0aa5bc-eb38-4ac2-9372-72449b58b1cc'
# @dag(
#     start_date=datetime(2024,1,1),
#     schedule='@daily', 
#     catchup=False,
#     tags=['airbyte','risk'],
# )

# def customer_metrics() :
#     load_customer_transactions_raw = AirbyteTriggerSyncOperator (
#         task_id='load_customer_transactions_raw' ,
#         airbyte_conn_id='airbyte',
#         connection_id=AIRBYTE_ID_LOAD_CUSTOMER_TRANSACTION_RAW
#               ) 

#     @task
#     def airbyte_jobs_done():
#         return True
# customer_metrics()