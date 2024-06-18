from io import BytesIO
import pandas as pd
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.models import Variable
from logger import setup_logging

base_path = Variable.get("base_path")

my_logger = setup_logging()
my_logger.set_logger("main_logger")

def import_data(excel_path, pickle_path):
    """Function to import data from an Excel file and save it as a .pkl file."""
    my_logger.write('info', f"Starting to import data from {excel_path}")
    
    gcs_hook = GCSHook()

    try:
        # Check if pickle file already exists in GCS
        if gcs_hook.exists(bucket_name=base_path, object_name=pickle_path[len(f"gs://{base_path}/"):]):
            my_logger.write('info', f"Data has already been saved as a pickle file at {pickle_path}.")
            return pickle_path
    except Exception as e:
        my_logger.write('error', f"Failed to check if pickle file exists in GCS: {e}")
        raise
    
    try:
        # Download Excel file from GCS
        excel_data = gcs_hook.download(bucket_name=base_path, object_name=excel_path[len(f"gs://{base_path}/"):])
        df = pd.read_excel(excel_data)
        my_logger.write('info', "Data successfully read from the Excel file.")
    except Exception as e:
        my_logger.write('error', f"Failed to read data from Excel file: {e}")
        raise
    
    try:
       # Save DataFrame as .pkl file to GCS
        pickle_buffer = BytesIO()
        df.to_pickle(pickle_buffer)  # Write the DataFrame to the BytesIO buffer as a pickle
        pickle_buffer.seek(0)  # Reset the buffer's cursor to the beginning
        gcs_hook.upload(bucket_name=base_path, object_name=pickle_path[len(f"gs://{base_path}/"):], data=pickle_buffer.getvalue())
        my_logger.write('info', f"Data successfully saved as a pickle file to GCS at {pickle_path}.")
    except Exception as e:
        my_logger.write('error', f"Failed to save data as a pickle file to GCS: {e}")
        raise
    
    # Return the path to the .pkl file in GCS
    return pickle_path