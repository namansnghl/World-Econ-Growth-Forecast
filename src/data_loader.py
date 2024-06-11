import os
import sys
import pandas as pd

# Determine the absolute path of the project directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

sys.path.append(PROJECT_DIR)
from utilities.logger import setup_logging

DEFAULT_EXCEL_PATH = os.path.join(PROJECT_DIR, 'data', 'raw_data', 'IMF_WEO_Data.xlsx')
DEFAULT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data', 'processed_data', 'raw_data.pkl')

# Initialize logger
my_logger = setup_logging()
my_logger.set_logger("main_logger")

def import_data(excel_path = DEFAULT_EXCEL_PATH, pickle_path = DEFAULT_PICKLE_PATH):
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