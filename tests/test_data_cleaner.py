import os
import pandas as pd
import numpy as np
import pytest
from data_cleaner import process_data

# Determine the absolute path of the project directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data', 'processed_data', 'raw_data.pkl')

def test_process_data_no_input_file():
    """
    Test that process_data raises an error when the input pickle file doesn't exist.
    """
    with pytest.raises(FileNotFoundError):
        process_data(pickle_path="non_existing_file.pkl")

def test_process_data():
    """
    Test that process_data correctly processes the data and saves it as a pickle file.
    """
    # Create a sample DataFrame with some data
    data = {
        'WEO Country Code': [1, 2, 3],
        'Country': ['A', 'B', 'C'],
        'WEO Subject Code': ['GDP', 'INFL', 'UNEMPL'],
        'Country/Series-specific Notes': ['Note1', 'Note2', 'Note3'],
        'Estimates Start After': ['2018', '2018', '2018'],
        'Subject Descriptor': ['GDP', 'Inflation', 'Unemployment'],
        'Units': ['Percentage', 'Index', 'Percent'],
        'Scale': ['Percent', 'Index', 'Percent'],
        '2019': [1.5, 2.5, 3.5],
        '2020': [2.0, 3.0, 4.0]
    }
    df = pd.DataFrame(data)
    
    # Save the sample DataFrame as a pickle file
    df.to_pickle(DEFAULT_PICKLE_PATH)
    
    # Process the data
    output_pickle_path = process_data(pickle_path=DEFAULT_PICKLE_PATH)
    
    # Check if the output pickle file exists
    assert os.path.exists(output_pickle_path), "Processed pickle file was not created."
    
    # Load the processed DataFrame from the output pickle file
    processed_df = pd.read_pickle(output_pickle_path)
    
    # Check if the processed DataFrame has the correct columns
    expected_columns = ['WEO Country Code', 'Country', 'Subject', '2019', '2020']
    assert processed_df.columns.tolist() == expected_columns, "Processed DataFrame columns do not match expectations."
    
    # Check if the processed DataFrame has the correct number of rows
    expected_num_rows = 3  # Same as the number of rows in the sample DataFrame
    assert len(processed_df) == expected_num_rows, "Processed DataFrame has incorrect number of rows."

    # Check if the "Subject" column values are correctly combined
    expected_subjects = [
        'GDP - Percentage (Percent)',
        'Inflation - Index (Index)',
        'Unemployment - Percent (Percent)'
    ]
    assert processed_df['Subject'].tolist() == expected_subjects, "Processed DataFrame 'Subject' column values do not match expectations."
    
    # Check if the null values are correctly converted
    assert processed_df.isnull().sum().sum() == 0, "There are still null values in the DataFrame."
    
    # Remove the output pickle file after the test
    os.remove(output_pickle_path)