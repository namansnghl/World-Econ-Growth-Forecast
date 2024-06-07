import os
import pandas as pd
from utilities.logger import setup_logging

# Define project directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Initialize logger
file_path = os.path.join(PROJECT_DIR, 'utilities', 'log_config.json')
my_logger = setup_logging(file_path)
my_logger.set_logger("main_logger")

def filter_data(pickle_path, countries_to_drop_path):
    """Function to filter data based on various criteria and save the final dataset"""
    my_logger.write('info', f"Starting to filter data from {pickle_path}")

    # Load the transformed data from pickle file
    try:
        df = pd.read_pickle(pickle_path)
        my_logger.write('info', "Data successfully loaded from the transformed pickle file.")
    except FileNotFoundError:
        my_logger.write('error', "The transformed pickle file does not exist.")
        raise FileNotFoundError("The transformed pickle file does not exist.")
    except Exception as e:
        my_logger.write('error', f"Failed to load data from the transformed pickle file: {e}")
        raise

    # Load the list of countries to drop from a CSV file
    try:
        countries_to_drop_df = pd.read_csv(countries_to_drop_path)
        countries_to_drop = countries_to_drop_df['Country'].tolist()
        my_logger.write('info', "Countries to drop successfully loaded.")
    except FileNotFoundError:
        my_logger.write('error', "The countries to drop CSV file does not exist.")
        raise FileNotFoundError("The countries to drop CSV file does not exist.")
    except Exception as e:
        my_logger.write('error', f"Failed to load the countries to drop CSV file: {e}")
        raise

    # Ensure 'Year' column is numeric
    df['Year'] = pd.to_numeric(df['Year'], errors='coerce')

    # Drop the specified countries
    df = df[~df['Country'].isin(countries_to_drop)]

    # Filter the DataFrame for years between 1994 and 2029 (inclusive)
    filtered_df = df[(df['Year'] >= 1994) & (df['Year'] <= 2029)]

    # Drop columns containing "percent of GDP" in their names
    columns_to_drop = [col for col in filtered_df.columns if "percent of gdp" in col.lower()]
    filtered_df = filtered_df.drop(columns=columns_to_drop)

    # Drop additional specified columns
    additional_columns_to_drop = [
        'Output gap in percent of potential GDP - Percent of potential GDP (Units)',
        'Employment - Persons (Millions)',
        'General government net debt - National currency (Billions)',
        'General government structural balance - National currency (Billions)',
        'General government structural balance - Percent of potential GDP (Units)'
    ]
    filtered_df = filtered_df.drop(columns=additional_columns_to_drop, errors='ignore')

    # Save the filtered DataFrame to a new Pickle and Excel file
    filtered_pickle_path = os.path.join(PROJECT_DIR, 'data', 'processed_data', 'filtered_data.pkl')
    try:
        filtered_df.to_pickle(filtered_pickle_path)
        my_logger.write('info', f"Filtered data successfully saved to {filtered_pickle_path}")

        filtered_excel_path = os.path.join(PROJECT_DIR, 'data', 'processed_data', 'filtered_data.xlsx')
        filtered_df.to_excel(filtered_excel_path, index=False)
        my_logger.write('info', f"Filtered data successfully saved as an Excel file at {filtered_excel_path}.")
    except Exception as e:
        my_logger.write('error', f"Failed to save the filtered data: {e}")
        raise

    return filtered_pickle_path
