import os
import numpy as np
from io import BytesIO
import pandas as pd
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.models import Variable
from logger import setup_logging

base_path = Variable.get("base_path")

my_logger = setup_logging()
my_logger.set_logger("main_logger")

def process_data(pickle_path):
    """Function to import, process, and save data as pickle file"""
    my_logger.write('info', f"Starting data processing for {pickle_path}")
    
    gcs_hook = GCSHook()
    # Load data from pickle file
    try:
        pickle_data = gcs_hook.download(bucket_name=base_path, object_name=pickle_path[len(f"gs://{base_path}/"):])
        pickle_buffer = BytesIO(pickle_data)
        df = pd.read_pickle(pickle_buffer)
    except FileNotFoundError:
        my_logger.write('error', "The input pickle file does not exist.")
        raise FileNotFoundError("The input pickle file does not exist.")
    
    # Perform data processing steps
    df = drop_columns(df)
    df = combine_columns(df)
    df = convert_null_values(df)

    # Define the output path in GCS
    processed_data_path = f"gs://{base_path}/data/processed_data/processed_data.pkl"
    try:
        # Save processed data as pickle file
        pickle_buffer = BytesIO()
        df.to_pickle(pickle_buffer)  # Write the DataFrame to the BytesIO buffer as a pickle        
        pickle_buffer.seek(0)
        gcs_hook.upload(bucket_name=base_path, object_name=processed_data_path[len(f"gs://{base_path}/"):], data=pickle_buffer.getvalue())
        my_logger.write('info', f"Data processing completed. Processed data saved at {processed_data_path}")
    except Exception as e:
        my_logger.write('error', f"Failed to save processed data: {e}")
        raise
  
    return processed_data_path

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