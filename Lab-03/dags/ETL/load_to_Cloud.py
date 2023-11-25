import pandas as pd
from google.cloud import bigquery
import psycopg2
import os
from sqlalchemy import create_engine
 

def transfer_data_to_bigquery(table_name, client):
    # Connect to PostgreSQL
    db_url = os.getenv('DB_URL')
    engine = create_engine(db_url)

    sql_query = 'SELECT * FROM '+table_name
    df = pd.read_sql(sql_query, engine)

    # Connect to BigQuery
    dataset_id = 'shelter_warehouse_schema'
    table_id = table_name
    table_ref = client.dataset(dataset_id).table(table_id)

    # Create or overwrite the table if it doesn't exist
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

    # Upload DataFrame to BigQuery
    client.load_table_from_dataframe(df, table_ref, job_config=job_config)