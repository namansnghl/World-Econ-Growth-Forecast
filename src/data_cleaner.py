import os
import pandas as pd
import numpy as np
from utilities.logger import setup_logging

# Determine the absolute path of the project directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Initialize logger
file_path = os.path.join(PROJECT_DIR, 'utilities', 'log_config.json')
my_logger = setup_logging(file_path)
my_logger.set_logger("main_logger")

def process_data(pickle_path):
    """Function to import, process, and save data as pickle file"""
    my_logger.write('info', f"Starting data processing for {pickle_path}")
    
    # Load data from pickle file
    try:
        df = pd.read_pickle(pickle_path)
    except FileNotFoundError:
        my_logger.write('error', "The input pickle file does not exist.")
        raise FileNotFoundError("The input pickle file does not exist.")
    
    # Perform data processing steps
    df = drop_columns(df)
    df = combine_columns(df)
    df = convert_null_values(df)
    
    # Save processed data as pickle file
    output_pickle_path = os.path.join(PROJECT_DIR, 'data/processed_data', 'processed_data.pkl')
    df.to_pickle(output_pickle_path)
    
    my_logger.write('info', f"Data processing completed. Processed data saved at {output_pickle_path}")
  
    return output_pickle_path

# Additional functions for data processing
def drop_columns(df):
    """Function to drop irrelevant columns"""
    df.drop(columns=['WEO Subject Code','Country/Series-specific Notes','Estimates Start After'], inplace=True)
    return df

def combine_columns(df):
    """Function to combine relevant columns into a new 'Subject' column"""
    df['Subject'] = df.apply(lambda row: f"{row['Subject Descriptor']} - {row['Units']} ({row['Scale']})", axis=1)
    df.drop(columns=['Subject Descriptor','Units','Scale'], inplace=True)
    return df

def convert_null_values(df):
    """Function to convert specific values to NaN and then convert numeric columns to numeric"""
    columns = ['WEO Country Code', 'Country', 'Subject'] + [col for col in df.columns if col not in ['WEO Country Code', 'Country', 'Subject']]
    df = df[columns]
    df.replace(["--", "na", ""], np.nan, inplace=True)
    df.iloc[:, 3:] = df.iloc[:, 3:].apply(pd.to_numeric)
    return df
