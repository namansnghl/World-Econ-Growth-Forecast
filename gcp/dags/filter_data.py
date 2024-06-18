import os
from io import BytesIO
import pandas as pd
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from logger import setup_logging
from airflow.models import Variable

base_path = Variable.get("base_path")


my_logger = setup_logging()
my_logger.set_logger("main_logger")

def filter_data(pickle_path, countries_to_drop_path):
    """Function to filter data based on various criteria and save the final dataset."""
    my_logger.write('info', f"Starting to filter data from {pickle_path}")

    gcs_hook = GCSHook()

    # Load the transformed data from pickle file
    try:
        pickle_data = gcs_hook.download(bucket_name=base_path, object_name=pickle_path[len(f"gs://{base_path}/"):])
        pickle_buffer = BytesIO(pickle_data)
        df = pd.read_pickle(pickle_buffer)
        my_logger.write('info', "Data successfully loaded from the transformed pickle file.")
    except FileNotFoundError:
        my_logger.write('error', "The transformed pickle file does not exist.")
        raise FileNotFoundError("The transformed pickle file does not exist.")
    except Exception as e:
        my_logger.write('error', f"Failed to load data from the transformed pickle file: {e}")
        raise

    # Load the list of countries to drop
    countries_data = gcs_hook.download(bucket_name=base_path, object_name=countries_to_drop_path[len(f"gs://{base_path}/"):])
    countries_to_drop = load_countries_to_drop(BytesIO(countries_data))

    # Drop the specified countries
    df = df[~df['Country'].isin(countries_to_drop)]

    # Filter the DataFrame for the specified years
    df = filter_years(df)

    # Define columns to drop
    columns_to_drop = [
        'Output gap in percent of potential GDP - Percent of potential GDP (Units)',
        'Employment - Persons (Millions)',
        'General government net debt - National currency (Billions)',
        'General government structural balance - National currency (Billions)',
        'General government structural balance - Percent of potential GDP (Units)'
    ]

    # Drop columns containing "percent of GDP" in their names
    columns_to_drop.extend([col for col in df.columns if "percent of gdp" in col.lower()])

    # Drop the specified columns
    df = drop_columns(df, columns_to_drop)

    # Save the filtered DataFrame to a new Pickle and Excel file
    filtered_pickle_path = f"gs://{base_path}/data/processed_data/filtered_data.pkl"
    filtered_excel_path = f"gs://{base_path}/data/processed_data/filtered_data.xlsx"

    try:
        pickle_buffer = BytesIO()
        df.to_pickle(pickle_buffer)  # Write the DataFrame to the BytesIO buffer as a pickle        
        pickle_buffer.seek(0)
        gcs_hook.upload(bucket_name=base_path, object_name=filtered_pickle_path[len(f"gs://{base_path}/"):], data=pickle_buffer.getvalue())
        my_logger.write('info', f"Filtered data successfully saved to {filtered_pickle_path}")

        # Save as Excel file
        excel_buffer = BytesIO()
        df.to_excel(excel_buffer, index=False)
        excel_buffer.seek(0)
        gcs_hook.upload(bucket_name=base_path, object_name=filtered_excel_path[len(f"gs://{base_path}/"):], data=excel_buffer.getvalue())
        my_logger.write('info', f"Filtered data successfully saved as an Excel file at {filtered_excel_path}.")
    except Exception as e:
        my_logger.write('error', f"Failed to save the filtered data: {e}")
        raise

    return filtered_pickle_path

def load_countries_to_drop(countries_data):
    """Load the list of countries to drop from a CSV file."""
    try:
        countries_to_drop_df = pd.read_csv(countries_data)
        countries_to_drop = countries_to_drop_df['Country'].tolist()
        my_logger.write('info', "Countries to drop successfully loaded.")
        return countries_to_drop
    except FileNotFoundError:
        my_logger.write('error', "The countries to drop CSV file does not exist.")
        raise FileNotFoundError("The countries to drop CSV file does not exist.")
    except Exception as e:
        my_logger.write('error', f"Failed to load the countries to drop CSV file: {e}")
        raise

def filter_years(df, start_year=1994, end_year=2029):
    """Filter the DataFrame for years between start_year and end_year (inclusive)."""
    df['Year'] = pd.to_numeric(df['Year'], errors='coerce')
    return df[(df['Year'] >= start_year) & (df['Year'] <= end_year)]

def drop_columns(df, columns_to_drop):
    """Drop specified columns from the DataFrame."""
    return df.drop(columns=columns_to_drop, errors='ignore')