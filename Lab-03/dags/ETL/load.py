import pandas as pd
import os
from sqlalchemy import create_engine, text
from ETL.gcp_connect import get_blob




def load_data(blob_name, bucket):
    db_url = os.getenv('DB_URL')
    engine = create_engine(db_url)

    data_to_staging(engine, blob_name, bucket)

    print("Data is in Staging Area...")




def data_to_staging(engine, blob_name, bucket):

    data = get_blob(blob_name, "CSV", bucket)
    data.to_sql("outcomes_staging",
                engine,
                if_exists="append", # Just add data to the existing table
                index=False, # Don't push the data's index into the table
    )


def execute_sql_script(script_path):
    db_url = os.getenv('DB_URL')
    engine = create_engine(db_url)

    # Read SQL Script
    with open(script_path, 'r') as file:
        sql_script = file.read()

    try:
        with engine.connect() as connection:
            transaction = connection.begin()
            try:
                connection.execute(text(sql_script))
                transaction.commit()  # Commit the transaction if successful
            except Exception as e:
                transaction.rollback()  # Rollback the transaction on error
                print(f"Error executing SQL script: {str(e)}")
    except Exception as e:
        print(f"Error connecting to the database: {str(e)}")

    



    