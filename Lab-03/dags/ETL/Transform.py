
import pandas as pd
import numpy as np
from ETL.gcp_connect import get_blob, upload_blob



def transform_data(blob_name, destination_blob_name, bucket):
    df = get_blob(blob_name, "JSON", bucket)
    print(df.columns)
    new_data = pre_process(df)
    new_data = rename_columns(new_data)
    
    csv_data = new_data.to_csv(index=False) # Transformed data to CSV file (in memory)
    upload_blob(csv_data, destination_blob_name, "text/csv", bucket)


   
def pre_process(df):
    
    ## Expand Dates & Generate Date ID
    # Event Date (Outcome)
    df["datetime"] = pd.to_datetime(df["datetime"])
    df['Day'] = df["datetime"].dt.day
    df['Month'] = df["datetime"].dt.month
    df['Year'] = df["datetime"].dt.year
    df['Date ID'] = df["datetime"].dt.date.astype(str)
    df['Date ID'] = df["Date ID"].str.replace('-', '')

    # Birth Date
    df[['DOB', 'time']] = df["date_of_birth"].str.split("T", expand=True)

    # Seperate sex_upon_outcome
    df.sex_upon_outcome.unique()
    df[['Reproductive Status', 'Sex']] = df.sex_upon_outcome.str.split(expand=True)
    df['Reproductive Status'].replace('Unknown', None, inplace=True)

    # Name (remove "*")
    df['name'] = df.name.str.replace('*', '')

    # Drop uneeded columns
    df.drop(columns = ['monthyear', 'sex_upon_outcome','age_upon_outcome', 'outcome_subtype', 'time', 'datetime', 'date_of_birth'], inplace=True, errors='ignore')

    return df


def rename_columns(df):
    new_data = df.rename(columns={
        "animal_id":'Animal ID',
        "name":'Name',
        "outcome_type":'Outcome',
        'animal_type':'Animal Type',
        'breed':'Breed',
        'color':'Color'
        })
    return new_data
    
