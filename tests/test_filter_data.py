import os, sys
import pandas as pd
import pytest

# Determine the absolute path of the project directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_DIR)
from src.filter_data import filter_data

# Define the project directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data', 'processed_data', 'raw_data.pkl')
DEFAULT_COUNTRIES_TO_DROP_PATH = os.path.join(PROJECT_DIR, 'data', 'countries_to_drop.csv')

def test_filter_data_no_input_file():
    """
    Test that filter_data raises an error when the input pickle file doesn't exist.
    """
    with pytest.raises(FileNotFoundError):
        filter_data(pickle_path="non_existing_file.pkl", countries_to_drop_path=DEFAULT_COUNTRIES_TO_DROP_PATH)

def test_filter_data():
    """
    Test that filter_data correctly filters the data and saves it as a pickle and Excel file.
    """
    # Create a sample DataFrame with some data
    data = {
        'Country': ['A', 'B', 'C', 'D'],
        'Year': [1990, 2000, 2010, 2020],
        'GDP': [1.5, 2.5, 3.5, 4.5],
        'Percent of GDP': [0.1, 0.2, 0.3, 0.4],
        'Output gap in percent of potential GDP - Percent of potential GDP (Units)': [0.1, 0.2, 0.3, 0.4],
        'Employment - Persons (Millions)': [10, 20, 30, 40]
    }
    df = pd.DataFrame(data)
    
    # Save the sample DataFrame as a pickle file
    df.to_pickle(DEFAULT_PICKLE_PATH)
    
    # Create a sample DataFrame for countries to drop
    countries_to_drop = pd.DataFrame({'Country': ['B', 'D']})
    countries_to_drop.to_csv(DEFAULT_COUNTRIES_TO_DROP_PATH, index=False)
    
    # Filter the data
    output_pickle_path = filter_data(pickle_path=DEFAULT_PICKLE_PATH, countries_to_drop_path=DEFAULT_COUNTRIES_TO_DROP_PATH)
    
    # Check if the output pickle file exists
    assert os.path.exists(output_pickle_path), "Filtered pickle file was not created."

    # Check if the output Excel file exists
    output_excel_path = output_pickle_path.replace('.pkl', '.xlsx')
    assert os.path.exists(output_excel_path), "Filtered Excel file was not created."
    
    # Load the filtered DataFrame from the output pickle file
    filtered_df = pd.read_pickle(output_pickle_path)
    
    # Check if the filtered DataFrame has the correct columns and values
    expected_columns = ['Country', 'Year', 'GDP']
    assert filtered_df.columns.tolist() == expected_columns, "Filtered DataFrame columns do not match expectations."

    # Check that the DataFrame contains only countries and years within the specified range
    assert all(filtered_df['Year'] >= 1994) and all(filtered_df['Year'] <= 2029), "Year filtering failed."
    assert 'B' not in filtered_df['Country'].values, "Country 'B' was not dropped."
    assert 'D' not in filtered_df['Country'].values, "Country 'D' was not dropped."

    # Remove the output files after the test
    os.remove(output_pickle_path)
    os.remove(output_excel_path)

def test_filter_data_load_failures():
    """
    Test the handling of file loading failures.
    """
    # Check missing pickle file
    with pytest.raises(FileNotFoundError):
        filter_data(pickle_path="missing_pickle.pkl", countries_to_drop_path=DEFAULT_COUNTRIES_TO_DROP_PATH)

    # Create a sample DataFrame for the pickle file
    data = {
        'Country': ['A', 'B', 'C'],
        'Year': [1990, 2000, 2010],
        'GDP': [1.5, 2.5, 3.5]
    }
    df = pd.DataFrame(data)
    df.to_pickle(DEFAULT_PICKLE_PATH)

    # Check missing countries to drop file
    with pytest.raises(FileNotFoundError):
        filter_data(pickle_path=DEFAULT_PICKLE_PATH, countries_to_drop_path="missing_countries.csv")
