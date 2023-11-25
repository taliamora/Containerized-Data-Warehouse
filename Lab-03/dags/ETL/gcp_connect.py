from google.cloud import storage
import os
import io
import pandas as pd
import json
import requests
import logging


def extract_api_data(api_url, destination_blob_name, bucket):
   response = requests.get(api_url)
   data = response.json()
   json_string = json.dumps(data)

   upload_blob(json_string, destination_blob_name, 'application/json', bucket)



def upload_blob(data_string, destination_blob_name, file_format, bucket):
  """Uploads a data file to the bucket."""
  try:
    blob = bucket.blob(destination_blob_name)
    
    blob.upload_from_string(data_string, content_type = file_format)
    print("Data Deposited into Cloud Storage")

  except Exception as e:
     logging.error(f"Error Occured while putting data into cloud storage..")  
     raise
  


def get_blob(blob_name, file_format, bucket):
    try:
       print("Getting data from Cloud Storage")
       blob = bucket.blob(blob_name)
       if file_format.lower() == "json":
          json_string = blob.download_as_string(client=None).decode('utf-8')
          data = json.loads(json_string)
          df = pd.DataFrame.from_dict(data)
       elif file_format.lower() == "csv":
          with blob.open("rt") as f:
             df = pd.read_csv(f)
       else:
          raise ValueError("Unsupported file format. Please use JSON or CSV")
       return df
    
    except Exception as e:
        logging.error(f"Error occurred while fetching data from cloud storage: {e}")
        raise      

  


