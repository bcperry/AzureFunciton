# https://medium.com/mlearning-ai/etl-pipelines-with-python-azure-functions-6c3f7a7e35b1
# https://learn.microsoft.com/en-us/azure/azure-functions/functions-bindings-storage-blob-trigger?tabs=python-v2%2Cisolated-process%2Cnodejs-v4&pivots=programming-language-python#metadata
import azure.functions as func
import logging
from data_processing import process_example

app = func.FunctionApp()

# create the trigger on bronze ingest
@app.blob_trigger(arg_name="myblob", path="bronze",
                               connection="AzureWebJobsStorage") 
def bronze_blob_trigger(myblob: func.InputStream):

    # log information about this trigger
    logging.info(f'filename: {myblob.name}')
    if 'test_type' in myblob.metadata.keys():
        logging.info('Test_type is: ' + myblob.metadata['test_type'])

        # as an example, we will call the function to transform the data
        # there will be many of these functions required
        if myblob.metadata['test_type'] == 'example':
            process_example(myblob)

    # if the appropriate metadata is not attached to the data, we cannot performa any ETL
    else:
        logging.info('Test_type Metadata not appended')
    logging.info(myblob.metadata)


