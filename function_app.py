# https://medium.com/mlearning-ai/etl-pipelines-with-python-azure-functions-6c3f7a7e35b1
import azure.functions as func
import logging
import os
from io import BytesIO
import pandas as pd
import time
from utilities import preprocess, save_dataframe_to_blob

app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="webdev",
                               connection="AzureWebJobsStorage") 
def blob_trigger(myblob: func.InputStream):
    # Converting the csv file to dataframe 
    df = pd.read_csv(BytesIO(myblob.read()))
    logging.info(f"Shape: {df.shape}")

    # Preprocessing the data
    df = preprocess(df)

    # Preparing the file name
    blob_name = myblob.name.split("/")[-1].split(".")[0]
    file_name = f"{blob_name}_{int(time.time())}_preprocessed.csv"

    # Saves the data to Azure blob storage
    save_dataframe_to_blob(df, os.environ.get("AzureWebJobsStorage"), "dataengineering", file_name)