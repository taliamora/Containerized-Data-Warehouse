
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage

from datetime import datetime, timedelta
import os
from google.cloud import bigquery
from ETL.gcp_connect import extract_api_data
from ETL.Transform import transform_data
from ETL.load import load_data, execute_sql_script
from ETL.load_to_Cloud import transfer_data_to_bigquery


SOURCE_URL = "https://data.austintexas.gov/resource/9t4d-g238.json?$where=date_trunc_ymd(datetime)='{{ ds }}'"
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow') # Establish shared directory
CLOUD_STORAGE = storage.Client().bucket("dcsc-ariflow-storage")
BQ_CLIENT = bigquery.Client.from_service_account_json(os.getenv('GOOGLE_APPLICATION_CREDENTIALS'))

# Location for downloaded data (gotten from source):
JSON_TARGET_DIR = 'Raw Downloads/'
JSON_TARGET_FILE = 'outcomes_{{ ds }}.json' # Saved data file name
CSV_TARGET_DIR = 'Transformed/'+JSON_TARGET_FILE.replace('.json', '.csv') # {{ ds }} inserts rundate

current_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) 

with DAG(
    dag_id = "outcomes_dag",
    start_date = datetime(2023,11,1), # Required: Date from which you want to run the DAG (backfills for past)
    end_date = current_date, # Prevents attempts to pull TOMORROW'S data before it is available
    schedule_interval='@daily'
) as dag:
    # Gets Data from API -> into cloud storage:
    extract = PythonOperator(
        task_id="Extract",
        python_callable= extract_api_data,
        op_kwargs = {
            'api_url' : SOURCE_URL, # API endpoint
            'destination_blob_name' : JSON_TARGET_DIR+JSON_TARGET_FILE, # Data fetched from API
            'bucket' : CLOUD_STORAGE
        }
    )


    # Applies transformations to raw data
    transform = PythonOperator(
        task_id="Transform",
        python_callable= transform_data, # Function from Transform.py
        op_kwargs = { # Provide Arguments for python_callable function
            'blob_name' : JSON_TARGET_DIR+JSON_TARGET_FILE, # Data to be transformed
            'destination_blob_name' : CSV_TARGET_DIR,
            'bucket' : CLOUD_STORAGE
        }
    )

    # Load Data into SQL Database (Staging Area)
    # SQL Script is carried out in staging area
    load = PythonOperator(
        task_id="Load",
        python_callable= load_data, # Function from load.py
        retries=3,
        retry_delay=timedelta(minutes=1),
        op_kwargs = { 
            'blob_name' : CSV_TARGET_DIR, # Transformed data from cloud storage
            'bucket' : CLOUD_STORAGE

        }
    )

    # Insert data into Fact & Dim tables (staging -> schema)
    insert = PythonOperator(
        task_id='Insert',
        python_callable=execute_sql_script,
        op_kwargs ={
            'script_path' : AIRFLOW_HOME+'/dags/ETL/data_insertion.sql'
        }
        
    )

    # Save data to Cloud Warehouse
    transfer_dim1 = PythonOperator(
        task_id='dim1_to_cloud',
        python_callable=transfer_data_to_bigquery,
        op_kwargs ={
            'table_name' : "animal_dim",
            'client' : BQ_CLIENT

        }      
    )

    transfer_dim2 = PythonOperator(
        task_id='dim2_to_cloud',
        python_callable=transfer_data_to_bigquery,
        op_kwargs ={
            'table_name' : "date_dim",
            'client' : BQ_CLIENT

        }      
    )

    transfer_dim3 = PythonOperator(
        task_id='dim3_to_cloud',
        python_callable=transfer_data_to_bigquery,
        op_kwargs ={
            'table_name' : "outcome_type_dim",
            'client' : BQ_CLIENT

        }      
    )

    transfer_dim4 = PythonOperator(
        task_id='dim4_to_cloud',
        python_callable=transfer_data_to_bigquery,
        op_kwargs ={
            'table_name' : "reproductive_status_dim",
            'client' : BQ_CLIENT

        }      
    )

    transfer_fact = PythonOperator(
        task_id='fact_to_cloud',
        python_callable=transfer_data_to_bigquery,
        op_kwargs ={
            'table_name' : "outcomes_fct",
            'client' : BQ_CLIENT

        }      
    )
    
    extract >> transform >> load >> insert >> [transfer_dim1, transfer_dim2, transfer_dim3, transfer_dim4] >> transfer_fact

