import os
import pytest
from data_loader import import_data

# Determine the absolute path of the project directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_EXCEL_PATH = os.path.join(PROJECT_DIR, 'data', 'raw_data', 'IMF_WEO_Data.xlsx')
DEFAULT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data', 'processed_data', 'raw_data.pkl')

def test_import_data():
    """
    Test that import_data correctly imports data from Excel and saves it as a pickle file.
    """
    result = import_data(excel_path=DEFAULT_EXCEL_PATH, pickle_path=DEFAULT_PICKLE_PATH)
    assert result == DEFAULT_PICKLE_PATH, f"Expected {DEFAULT_PICKLE_PATH}, but got {result}."
    # Check if pickle file is created
    assert os.path.exists(DEFAULT_PICKLE_PATH), "Pickle file was not created."
