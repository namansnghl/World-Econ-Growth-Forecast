import os, sys
import pandas as pd
import pytest

# Determine the absolute path of the project directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_DIR)
from src.transform import melt_dataframe, transform_data

# Determine the absolute path of the project directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data', 'processed_data', 'raw_data.pkl')

def test_transform_data_no_input_file():
    """
    Test that transform_data raises an error when the input pickle file doesn't exist.
    """
    with pytest.raises(FileNotFoundError):
        transform_data(pickle_path="non_existing_file.pkl")

def test_transform_data():
    """
    Test that transform_data correctly transforms the data and saves it as a pickle and Excel file.
    """
    # Create a sample DataFrame with some data
    data = {
        'WEO Country Code': [1, 2, 3],
        'Country': ['A', 'B', 'C'],
        'Subject': ['GDP', 'Inflation', 'Unemployment'],
        '2019': [1.5, 2.5, 3.5],
        '2020': [2.0, 3.0, 4.0]
    }
    df = pd.DataFrame(data)
    
    # Save the sample DataFrame as a pickle file
    df.to_pickle(DEFAULT_PICKLE_PATH)
    
    # Transform the data
    output_excel_path = transform_data(pickle_path=DEFAULT_PICKLE_PATH)
    
    # Check if the output Excel file exists
    assert os.path.exists(output_excel_path), "Transformed Excel file was not created."
    
    # Check if the output pickle file exists
    transformed_pickle_path = os.path.join(PROJECT_DIR, 'data', 'processed_data', 'transformed_data.pkl')
    assert os.path.exists(transformed_pickle_path), "Transformed pickle file was not created."
    
    # Load the transformed DataFrame from the output pickle file
    transformed_df = pd.read_pickle(transformed_pickle_path)
    
    # Check if the transformed DataFrame has the correct columns and values
    expected_columns = ['WEO Country Code', 'Country', 'Year', 'GDP', 'Inflation', 'Unemployment']
    assert transformed_df.columns.tolist() == expected_columns, "Transformed DataFrame columns do not match expectations."
    
    # Remove the output files after the test
    os.remove(output_excel_path)

def test_melt_dataframe():
    """
    Test the melt_dataframe function.
    """
    data = {
        'WEO Country Code': [1, 2],
        'Country': ['A', 'B'],
        'Subject': ['GDP', 'Inflation'],
        '2019': [1.5, 2.5],
        '2020': [2.0, 3.0]
    }
    df = pd.DataFrame(data)
    melted_df = melt_dataframe(df)
    
    # Check the structure of the melted DataFrame
    expected_columns = ['WEO Country Code', 'Country', 'Subject', 'Year', 'Value']
    assert melted_df.columns.tolist() == expected_columns, "Melted DataFrame columns do not match expectations."
    assert len(melted_df) == 4, "Melted DataFrame does not have the expected number of rows."
