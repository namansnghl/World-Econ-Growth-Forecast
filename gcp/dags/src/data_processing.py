import os
import src
import numpy as np
import pandas as pd

PROJECT_DIR = os.environ.get("PROJECT_DIR")
from utilities.logger import setup_logging
my_logger = setup_logging()
my_logger.set_logger("main_logger")
drop_cols = ['WEO Subject Code','Country/Series-specific Notes','Estimates Start After']

def pre_processing(raw_data):
    global drop_cols

    my_logger.logger.info("Performing pre processing on raw file")
    # Perform data processing steps
    raw_data = drop_columns(raw_data, drop_cols)
    my_logger.logger.info(f"Dropped below columns {'\n'.join(drop_cols)}")
    raw_data = combine_columns(raw_data)
    my_logger.logger.info("Combine columns completed")
    
    # cleaning null values
    raw_data = convert_null_values(raw_data)
    my_logger.logger.info("Non Nan to Nan conversion completed")
    raw_data.dropna(how='all', inplace=True)
    my_logger.logger.info("Dropped blank rows")

    return raw_data

def drop_columns(df, cols):
    """Function to drop irrelevant columns"""
    df.drop(columns=cols, inplace=True)
    return df

def combine_columns(df):
    """Function to combine relevant columns into a new 'Subject' column"""
    df['Subject'] = df.apply(lambda row: f"{row['Subject Descriptor']} - {row['Units']} ({row['Scale']})", axis=1)
    df.drop(columns=['Subject Descriptor','Units','Scale'], inplace=True)
    return df

def convert_null_values(df):
    """Function to convert specific values to NaN and then convert numeric columns to numeric"""
    df.replace(["--", "na", ""], np.nan, inplace=True)
    #df.iloc[:, 3:] = df.iloc[:, 3:].apply(pd.to_numeric)
    return df
