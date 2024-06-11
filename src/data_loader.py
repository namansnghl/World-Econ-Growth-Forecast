import os
import sys
import pandas as pd

PROJECT_DIR = os.environ.get("PROJECT_DIR")

from utilities.logger import setup_logging
my_logger = setup_logging()
my_logger.set_logger("main_logger")

def import_data(excel_path, pickle_path):
    """Function to import data from an Excel file and save it as a .pkl file."""
    my_logger.write('info', f"Starting to import data from {excel_path}")
    
    # Check if pickle file already exists
    if os.path.exists(pickle_path):
        my_logger.write('info', f"Data has already been saved as a pickle file at {pickle_path}.")
        return pickle_path
    
    try:
        # Read data from Excel file
        df = pd.read_excel(excel_path)
        my_logger.write('info', "Data successfully read from the Excel file.")
    except Exception as e:
        my_logger.write('error', f"Failed to read data from Excel file: {e}")
        raise
    
    try:
        # Save DataFrame as .pkl file
        df.to_pickle(pickle_path)
        my_logger.write('info', f"Data successfully saved as a pickle file at {pickle_path}.")
    except Exception as e:
        my_logger.write('error', f"Failed to save data as a pickle file: {e}")
        raise
    
    # Return the path to the .pkl file
    return pickle_path