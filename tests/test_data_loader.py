import os
import src
from src.data_loader import *
from numpy import nan

# Determine the absolute path of the project directory
DEFAULT_RAW_PATH = os.environ["RAW_FILE"]
DEFAULT_TRANSF_PATH = os.environ["PROCESSED_FILE"]

test_data = pd.DataFrame({
        'Name': ['Alice', nan, 'Charlie', 'David', 'Eve'],
        'Age': [25, 30, 35, 40, nan],
        'City': ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Boston']
        })

def test_load_file_xl():
    df = load_file(DEFAULT_RAW_PATH, 'xlsx')
    assert df is not None, f"Failed to load file from {DEFAULT_RAW_PATH} as pickle"
    assert isinstance(df, pd.DataFrame), "Expected a pandas DataFrame"


def test_load_file_pkl():
    df = load_file(DEFAULT_TRANSF_PATH, 'pickle')
    assert df is not None, f"Failed to load file from {DEFAULT_TRANSF_PATH} as pickle"
    assert isinstance(df, pd.DataFrame), "Expected a pandas DataFrame"

def test_save_file_pkl():
    file_path = './test.pkl'
    global test_data
    save_file(test_data, file_path)
    assert os.path.exists(file_path) == True, "Unable to save pickle file"