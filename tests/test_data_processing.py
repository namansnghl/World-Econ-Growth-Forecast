import os, sys
import pandas as pd
from numpy import nan

# Determine the absolute path of the project directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_DIR)
import src # DO NOT REMOVE
from src.data_processing import *

def test_pre_processing():
    data = {
        'WEO Country Code': [1, 2, 3],
        'Country': ['', '--', 'na'],
        'WEO Subject Code': ['ABC', 'GDP', 'AB'],
        'Country/Series-specific Notes': ['Note1', 'Note2', 'Note3'],
        'Estimates Start After': ['2017', '2018', '2018'],
        'Subject Descriptor': ['GDP', 'Inflation', 'Unemployment'],
        'Units': ['Percentage', 'Index', 'Percent'],
        'Scale': ['Percent', 'Index', 'Percent'],
        '2019': [1.5, 2.5, 3.5],
        '2020': [2.0, 3.0, 4.0]
    }
    df = pd.DataFrame(data)
    processed_data = pre_processing(df)

    assert processed_data.isna().any(axis=1).sum() == 3, "Invalid valid are not converted to NAN correctly"
    assert processed_data.columns.to_list() == ['WEO Country Code', 'Country', '2019', '2020', 'Subject'], "Wrong columns returned"
    assert processed_data.shape[0] == 3, "Invalid Row count"
    assert processed_data.shape[1] == 5, "Invalid Column count"

def test_drop_cols():
    """
    Tests if drop_column() is dropping only columns and not rows
    """
    test_data = pd.DataFrame({
        'Name': ['Alice', nan, 'Charlie', 'David', 'Eve'],
        'Age': [25, 30, 35, 40, nan],
        'City': ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Boston']
        })
    new_data = drop_columns(test_data, ['Age', 'City'])

    assert new_data.shape[0] == 5, "Drop_Columns() dropped some rows unexpectidly"
    assert new_data.columns.to_list() == ['Name'], "Drop_Columns() dropped less/extra columns"