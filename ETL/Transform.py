
import pandas as pd
import numpy as np


def transform_data(data):
    new_data = data.copy()
    new_data = pre_process(new_data)
    new_data = rename_columns(new_data)
    return new_data



def pre_process(df):
    
    ## Expand Dates & Generate Date ID
    # Event Date (Outcome)
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
    df.drop(columns = ['monthyear', 'sex_upon_outcome','age_upon_outcome', 'outcome_subtype', 'time', 'datetime', 'date_of_birth'], inplace=True)

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
    
