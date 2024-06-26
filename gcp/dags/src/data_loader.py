import os
from airflow.exceptions import AirflowException
import pandas as pd

PROJECT_DIR = os.environ.get("PROJECT_DIR")

from src.logger import setup_logging
my_logger = setup_logging()
my_logger.set_logger("main_logger")

def load_file(path:str, type: str = "xlsx") -> pd.DataFrame:
    """Reads a file and returns dataframe"""

    data = None
    
    if type.lower()=="xlsx":
        my_logger.logger.info(f"Loading data from {path}")
        data = pd.read_excel(path)
    elif type.lower()=="csv":
        my_logger.logger.info(f"Loading data from {path}")
        data = pd.read_csv(path)
    elif type.lower()=="pickle":
        my_logger.logger.info(f"Loading data from {path}")
        data = pd.read_pickle(path)
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
        df.to_pickle(path)
    else:
        msg = f"Invalid file type {file_type} passed to save_file()"
        my_logger.logger.error(msg)
        raise AirflowException(msg)