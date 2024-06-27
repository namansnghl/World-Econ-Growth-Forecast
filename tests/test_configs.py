import os
import src
from src.data_loader import *

import os

DEFAULT_RAW_PATH = os.environ["RAW_FILE"]
DEFAULT_TRANSF_PATH = os.environ["PROCESSED_FILE"]
DEFAULT_COUNTRIES_TO_DROP_PATH = os.environ["COUNTRY_DROP"]
DEFAULT_CLEAN_PATH = os.environ["CLEAN_FILE"]

def test_raw_path():
    assert os.path.exists(DEFAULT_RAW_PATH), f"Raw file '{DEFAULT_RAW_PATH}' does not exist."

def test_transformed_path():
    assert isinstance(DEFAULT_TRANSF_PATH, str), f"Expected 'DEFAULT_TRANSF_PATH' to be a string, got {type(DEFAULT_TRANSF_PATH)}"

    # Extract the directory part of the file path
    parent_dir = os.path.dirname(DEFAULT_TRANSF_PATH)
    assert os.path.isdir(parent_dir), f"Parent directory '{parent_dir}' of '{DEFAULT_TRANSF_PATH}' does not exist or is not a valid directory."
    
def test_countries_to_drop_path():
    assert os.path.exists(DEFAULT_COUNTRIES_TO_DROP_PATH), f"Countries to drop file '{DEFAULT_COUNTRIES_TO_DROP_PATH}' does not exist."

def test_clean_path():
    assert isinstance(DEFAULT_CLEAN_PATH, str), f"Expected 'DEFAULT_CLEAN_PATH' to be a string, got {type(DEFAULT_CLEAN_PATH)}"
    
    # Extract the directory part of the file path
    parent_dir = os.path.dirname(DEFAULT_CLEAN_PATH)
    assert os.path.isdir(parent_dir), f"Parent directory '{parent_dir}' of '{DEFAULT_CLEAN_PATH}' does not exist or is not a valid directory."


    