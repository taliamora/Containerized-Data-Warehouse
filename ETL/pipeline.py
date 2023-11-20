#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import os
from io import StringIO
import argparse
import requests
from sqlalchemy import create_engine, text
from Transform import transform_data

'''
# Get data from a .csv web link
def extract_data(source):
    return pd.read_csv(source)
'''
# Get data from an API endpoint URL
def extract_data(source):
    r = requests.get(source)
    JSON = r.text
    return pd.read_json(StringIO(JSON))


# Deposits data into the staging table (the "outcomes_staging" table)
#  This table will catch all of the data before splitting it into the fct/dim tables
def load_data(data, engine):
    data.to_sql("outcomes_staging",
                engine,
                if_exists="append", # Just add data to the existing table
                index=False # Don't push the data's index into the table
    )

def execute_sql_script(filename, engine):
    # Read SQL Script
    with open(filename, 'r') as file:
        sql_script = file.read()

    # Execute SQL Script
    with engine.connect() as connection:
        connection.execute(text(sql_script))




if __name__ == "__main__": 
    parser = argparse.ArgumentParser()
    parser.add_argument('source', help='source csv')
#    parser.add_argument('target', help='target csv')
    args = parser.parse_args()
    db_url = os.getenv('DB_URL')
    engine = create_engine(db_url)

    print("Starting...")
    df = extract_data(args.source)
    print("Data is Extracted.")
    new_df = transform_data(df)
    print("Data is Transformed.")
    load_data(new_df, engine)
    print("Data is in Staging Table...")
    execute_sql_script('/app/data_insertion.sql', engine)
    print("Data is Loaded into Fact & Dim Tables")