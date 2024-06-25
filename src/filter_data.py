import os, src
import pandas as pd
from utilities.logger import setup_logging

PROJECT_DIR = os.environ.get("PROJECT_DIR")
my_logger = setup_logging()
my_logger.set_logger("main_logger")

def filter_data(df, countries_to_drop):
    """Function to filter data based on various criteria and save the final dataset."""
    my_logger.logger.info("Beginning to filter data")

    # Drop the specified countries
    df = df[~df['Country'].isin(countries_to_drop)]
    my_logger.logger.info("Dropped specific countries")

    # Filter the DataFrame for the specified years
    df = filter_years(df)
    my_logger.logger.info("Filtered out bad year data")

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
    my_logger.logger.info(f"Dropped columns {'\n'.join(columns_to_drop)}")

    return df

def load_countries_to_drop(countries_to_drop_path):
    """Load the list of countries to drop from a CSV file."""
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
    else:
        return countries_to_drop

def filter_years(df, start_year=1994, end_year=2029):
    """Filter the DataFrame for years between start_year and end_year (inclusive)."""
    df['Year'] = pd.to_numeric(df['Year'], errors='coerce')
    return df[(df['Year'] >= start_year) & (df['Year'] <= end_year)]

def drop_columns(df, columns_to_drop):
    """Drop specified columns from the DataFrame."""
    return df.drop(columns=columns_to_drop, errors='ignore')
