"""
Data Pipeline
"""
import os
from data_loader import import_data
from data_cleaner import process_data
from filter_data import filter_data
from transform import transform_data

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Defining the default paths for the Excel and pickle files
DEFAULT_EXCEL_PATH = os.path.join(PROJECT_DIR, 'data', 'raw_data', 'IMF_WEO_Data.xlsx')
DEFAULT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data', 'processed_data', 'raw_data.pkl')
DEFAULT_COUNTRIES_TO_DROP_PATH = os.path.join(PROJECT_DIR, 'data', 'raw_data', 'countries_to_drop.csv')

if __name__ == "__main__":
    """
    Main script to execute the data pipeline
    """
    LOAD_DATA = import_data(DEFAULT_EXCEL_PATH, DEFAULT_PICKLE_PATH)
    CLEAN_DATA = process_data(LOAD_DATA)
    TRANSFORM_DATA = transform_data(CLEAN_DATA)
    FILTER_DATA = filter_data(pickle_path=TRANSFORM_DATA, countries_to_drop_path=DEFAULT_COUNTRIES_TO_DROP_PATH)
