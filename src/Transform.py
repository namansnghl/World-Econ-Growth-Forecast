import pandas as pd
import numpy as np

def import_data(file_path):
    """Function to import data from a file"""
    df = pd.read_excel(file_path)
    return df

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
    df.replace(["--", "na", ""], np.nan, inplace=True)
    numeric_columns = df.select_dtypes(include=[np.number]).columns
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric)
    return df

def melt_dataframe(df):
    """Function to melt the DataFrame"""
    df_melted = pd.melt(df, id_vars=['WEO Country Code', 'Country', 'Subject'], var_name='Year', value_name='Value')
    return df_melted

def pivot_dataframe(df):
    """Function to pivot the melted DataFrame"""
    df_pivot = df.pivot_table(index=['WEO Country Code', 'Country', 'Year'], columns='Subject', values='Value').reset_index()
    return df_pivot

def save_to_file(df, file_path):
    """Function to save the DataFrame to a file"""
    df.to_excel(file_path, index=False)

def process_file(input_file_path, output_file_path):
    """Function to process the file and save the transformed data"""
    df = import_data(input_file_path)
    df = drop_columns(df)
    df = combine_columns(df)
    df = convert_null_values(df)
    df_melted = melt_dataframe(df)
    df_pivot = pivot_dataframe(df_melted)
    save_to_file(df_pivot, output_file_path)

# Usage example
input_file_path = 'data/raw_data/IMF_WEO_Data.xlsx'
output_file_path = 'data/processed_data/Transformed.xlsx'
process_file(input_file_path, output_file_path)
