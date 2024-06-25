import os, src
import pandas as pd
from utilities.logger import setup_logging

PROJECT_DIR = os.environ.get("PROJECT_DIR")
my_logger = setup_logging()
my_logger.set_logger("main_logger")

def transform_data(df):
    """Function to transform data and save it"""

    my_logger.logger.info("begin Data Transforming for Raw")

     # Perform data transforming steps
    df = melt_dataframe(df)
    my_logger.logger.debug("Melt Complete")
    df = pivot_dataframe(df)
    my_logger.logger.debug("Pivot Complete")

    return df

def melt_dataframe(df):
        """Function to melt the DataFrame"""
        df_melted = pd.melt(df, id_vars=['WEO Country Code', 'Country', 'Subject'], var_name='Year', value_name='Value')
        return df_melted
   
def pivot_dataframe(df):
        """Function to pivot the melted DataFrame"""
        df_pivot = df.pivot_table(index=['WEO Country Code', 'Country', 'Year'], columns='Subject', values='Value').reset_index()
        return df_pivot