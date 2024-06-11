import os
import pandas as pd
from utilities.logger import setup_logging

PROJECT_DIR = os.environ.get("PROJECT_DIR")
my_logger = setup_logging()
my_logger.set_logger("main_logger")

def transform_data(pickle_path):
    """Function to transform data and save it"""

    my_logger.write('info', f"Starting to transform data from {pickle_path}")

    # Load data from pickle file
    try:
        df = pd.read_pickle(pickle_path)
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
    transformed_pickle_path = os.path.join(PROJECT_DIR, 'data', 'processed_data', 'transformed_data.pkl')
    try:
        df.to_pickle(transformed_pickle_path)
        my_logger.write('info', f"Transformed data successfully saved as a pickle file at {transformed_pickle_path}.")

        # Save as Excel file
        transformed_excel_path = os.path.join(PROJECT_DIR, 'data', 'processed_data', 'transformed.xlsx')
        df.to_excel(transformed_excel_path, index=False)
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