from io import BytesIO
import os
from airflow.exceptions import AirflowException
import pandas as pd
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook

PROJECT_DIR = os.environ.get("PROJECT_DIR")

from src.logger import setup_logging
my_logger = setup_logging()
my_logger.set_logger("main_logger")

def load_file(path:str, type: str = "xlsx") -> pd.DataFrame:
    """Reads a file and returns dataframe"""

    data = None
    gcs_hook = GCSHook()
    
    if type.lower()=="xlsx":
        my_logger.logger.info(f"Loading data from {path}")
        excel_data = gcs_hook.download(bucket_name=Variable.get('base_path'), object_name=path[len(os.environ['PROJECT_DIR'])+1:])
        data = pd.read_excel(excel_data)
    elif type.lower()=="csv":
        my_logger.logger.info(f"Loading data from {path}")
        data = pd.read_csv(path)
    elif type.lower()=="pickle":
        my_logger.logger.info(f"Loading data from {path}")
        pickle_data = gcs_hook.download(bucket_name=Variable.get('base_path'), object_name=path[len(os.environ['PROJECT_DIR'])+1:])
        pickle_buffer = BytesIO(pickle_data)
        data = pd.read_pickle(pickle_buffer)
    else:
        msg = f"Invalid file type {type} passed to load_file()"
        my_logger.logger.error(msg)
        raise AirflowException(msg)
    return data

def save_file(df: pd.DataFrame, path: str, file_type: str = "pickle") -> None:
    """Saves a DataFrame to a file."""

    # Validate file_type against supported extensions
    valid_extensions = {
        "xlsx": ".xlsx",
        "csv": ".csv",
        "pickle": ".pkl",
    }

    gcs_hook = GCSHook()
    

    # Check if file_type is valid
    if file_type.lower() not in valid_extensions:
        msg = f"Invalid file type {file_type} passed to save_file()"
        my_logger.logger.error(msg)
        raise AirflowException(msg)

    # Check if file path ends with correct extension
    if not path.lower().endswith(valid_extensions[file_type.lower()]):
        msg = f"Invalid file extension for {file_type}: {os.path.splitext(path)[1]}"
        my_logger.logger.error(msg)
        raise AirflowException(msg)
    
    
    if file_type.lower() == "xlsx":
        my_logger.logger.info(f"Saving data to {path}")
        df.to_excel(path, index=False)
    elif file_type.lower() == "csv":
        my_logger.logger.info(f"Saving data to {path}")
        df.to_csv(path, index=False)
    elif file_type.lower() == "pickle":
        my_logger.logger.info(f"Saving data to {path}")
        pickle_buffer = BytesIO()
        df.to_pickle(pickle_buffer)  # Write the DataFrame to the BytesIO buffer as a pickle
        pickle_buffer.seek(0)  # Reset the buffer's cursor to the beginning
        gcs_hook.upload(bucket_name=Variable.get('base_path'), object_name=path[len(os.environ['PROJECT_DIR'])+1:], data=pickle_buffer.getvalue())
    else:
        msg = f"Invalid file type {file_type} passed to save_file()"
        my_logger.logger.error(msg)
        raise AirflowException(msg)