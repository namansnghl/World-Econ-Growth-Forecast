import os
from io import BytesIO
import pandas as pd
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from logger import setup_logging
from airflow.models import Variable

base_path = Variable.get("base_path")
my_logger = setup_logging()
my_logger.set_logger("main_logger")

def transform_data(pickle_path):
    """Function to transform data and save it"""

    my_logger.write('info', f"Starting to transform data from {pickle_path}")
    gcs_hook = GCSHook()

    # Load data from pickle file
    try:
        pickle_data = gcs_hook.download(bucket_name=base_path, object_name=pickle_path[len(f"gs://{base_path}/"):])
        pickle_buffer = BytesIO(pickle_data)
        df = pd.read_pickle(pickle_buffer)
        my_logger.write('info', "Data successfully loaded from the pickle file.")
    except FileNotFoundError:
        my_logger.write('error', "The input pickle file does not exist.")
        raise FileNotFoundError("The input pickle file does not exist.")
    except Exception as e:
        my_logger.write('error', f"Failed to load data from pickle file: {e}")
        raise
    
     # Perform data transforming steps
    df = melt_dataframe(df)
    df = pivot_dataframe(df)

    # Save transformed data
    transformed_pickle_path = f"gs://{base_path}/data/processed_data/transformed_data.pkl"
    transformed_excel_path = f"gs://{base_path}/data/processed_data/transformed.xlsx"

    try:
        pickle_buffer = BytesIO()
        df.to_pickle(pickle_buffer)  # Write the DataFrame to the BytesIO buffer as a pickle        
        pickle_buffer.seek(0)
        gcs_hook.upload(bucket_name=base_path, object_name=transformed_pickle_path[len(f"gs://{base_path}/"):], data=pickle_buffer.getvalue())
        my_logger.write('info', f"Transformed data successfully saved as a pickle file at {transformed_pickle_path}.")

        # Save as Excel file
        excel_buffer = BytesIO()
        df.to_excel(excel_buffer, index=False)
        excel_buffer.seek(0)
        gcs_hook.upload(bucket_name=base_path, object_name=transformed_excel_path[len(f"gs://{base_path}/"):], data=excel_buffer.getvalue())
        my_logger.write('info', f"Transformed data successfully saved as an Excel file at {transformed_excel_path}.")
    except Exception as e:
        my_logger.write('error', f"Failed to save transformed data: {e}")
        raise

    return transformed_pickle_path

def melt_dataframe(df):
        """Function to melt the DataFrame"""
        df_melted = pd.melt(df, id_vars=['WEO Country Code', 'Country', 'Subject'], var_name='Year', value_name='Value')
        return df_melted
   
def pivot_dataframe(df):
        """Function to pivot the melted DataFrame"""
        df_pivot = df.pivot_table(index=['WEO Country Code', 'Country', 'Year'], columns='Subject', values='Value').reset_index()
        return df_pivot