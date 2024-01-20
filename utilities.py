from azure.storage.blob import BlobServiceClient
import pandas as pd


def preprocess(df: pd.DataFrame):
   """
   Preprocess the given dataframe by calculating
   the sum of all transactions per user.
   :param df: The dataframe to be preprocessed.
   :returns: The new aggregated dataframe.
   """
   return df.groupby("UserId")["Amount"].sum().reset_index()

def save_dataframe_to_blob(df: pd.DataFrame, connection_string: str, container_name: str, blob_name: str):
   """
   Saves a dataframe to azure blob storage.
   :param df: The dataframe to be saved.
   :param connection_string: The blob storage connection string.
   :param container_name: The container name.
   :param blob_name: The file name. 
   """
   # Convert the DataFrame to a CSV string
   csv_data = df.to_csv(index=False)

   # Create a BlobServiceClient object
   blob_service_client = BlobServiceClient.from_connection_string(connection_string)

   # Create a ContainerClient object
   container_client = blob_service_client.get_container_client(container_name)

   # Create a blob client and upload the CSV data
   blob_client = container_client.get_blob_client(blob_name)
   blob_client.upload_blob(csv_data, overwrite=True)