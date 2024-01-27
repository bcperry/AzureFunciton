from azure.storage.blob import BlobServiceClient
import azure.functions as func
import pandas as pd
import logging
import os
from io import BytesIO
import time

def process_example(myblob: func.InputStream):
   """
   performs all actions needed to convert raw, (bronze or level 1) 
   data to enriched (silver/level2) and curated (gold/level 3).
   :param myblob: The blob file uploaded to the data lake.
   :returns: Nothing.
   """
   # Converting the csv file to dataframe 
   df = pd.read_csv(BytesIO(myblob.read()))

   # Preprocessing the data
   df = preprocess(df)

   # Preparing the file name
   blob_name = myblob.name.split("/")[-1].split(".")[0]
   file_name = f"{blob_name}_{int(time.time())}_preprocessed.csv"

   # Convert the DataFrame to a CSV string
   csv_data = df.to_csv(index=False)

   # Saves the silver data to Azure blob storage
   save_data_to_blob(csv_data, myblob.metadata, os.environ.get("AzureWebJobsStorage"), "silver", file_name)


   # create a plot for the gold data, streaming images are an interesting excercise
   hist = df.plot.hist().figure
   image_stream = BytesIO()
   hist.savefig(image_stream)
   # reset stream's position to 0
   image_stream.seek(0)
   
   save_data_to_blob(image_stream.read(), myblob.metadata, os.environ.get("AzureWebJobsStorage"), "gold", f"{blob_name}_{int(time.time())}.png")


def preprocess(df: pd.DataFrame):
   """
   Preprocess the given dataframe by calculating
   the sum of all transactions per user.
   :param df: The dataframe to be preprocessed.
   :returns: The new aggregated dataframe.
   """
   return df.groupby("UserId")["Amount"].sum().reset_index()

def save_data_to_blob(data, metadata: dict, connection_string: str, container_name: str, blob_name: str):
   """
   Saves a dataframe to azure blob storage.
   :param df: The dataframe to be saved.
   :param metadata: the metadata dictionary from the original blob
   :param connection_string: The blob storage connection string.
   :param container_name: The container name.
   :param blob_name: The file name. 
   """

   # Create a BlobServiceClient object
   blob_service_client = BlobServiceClient.from_connection_string(connection_string)

   # Create a ContainerClient object
   container_client = blob_service_client.get_container_client(container_name)
   
   # update the metadata for the new data level
   if container_name == 'silver':
      data_level = '2'
   elif container_name == 'gold':
      data_level = '3'
   else:
      data_level = '1'
   metadata['data_level'] = data_level
   if 'ADSS' in metadata.keys():
      adss = metadata['ADSS']
   else:
      adss = 'unknown'
   # Create a blob client and upload the CSV data
   blob_client = container_client.get_blob_client(f'{adss}/{blob_name}')
   blob_client.upload_blob(data, overwrite=True, metadata=metadata)