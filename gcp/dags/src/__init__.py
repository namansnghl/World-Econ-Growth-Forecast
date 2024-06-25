import os, sys, configparser
from airflow.models import Variable

CONFIG_FILE = 'config.ini'
CONFIG_DATA_FOLDER_VAR = 'data_folder_name'
CONFIG_RAW_FILE_VAR = 'raw_file_name'
CONFIG_PROC_FILE_VAR = 'processed_file_name'
CONFIG_CLEAN_FILE_VAR = 'clean_file_name'
CONFIG_CNTR_DROP_VAR = 'cntr_to_drop_file'

def set_env_vars():
    curr_file = os.path.abspath(__file__)
    PROJECT_DIR = f"gs://{Variable.get("base_path")}"
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(curr_file), CONFIG_FILE))

    DATA_FOLDER = config['DEFAULT'][CONFIG_DATA_FOLDER_VAR]
    RAW_FILE = config['DEFAULT'][CONFIG_RAW_FILE_VAR]
    PROCESSED_FILE = config['DEFAULT'][CONFIG_PROC_FILE_VAR]
    COUNTRY_DROP = config['DEFAULT'][CONFIG_CNTR_DROP_VAR]
    CLEAN_FILE = config['DEFAULT'][CONFIG_CLEAN_FILE_VAR]

    # Add the parent directory to sys.path
    sys.path.append(PROJECT_DIR)
    os.environ["PROJECT_DIR"] = PROJECT_DIR
    os.environ["RAW_FILE"] = os.path.join(PROJECT_DIR, DATA_FOLDER, RAW_FILE)
    os.environ["PROCESSED_FILE"] = os.path.join(PROJECT_DIR, DATA_FOLDER, PROCESSED_FILE)
    os.environ["COUNTRY_DROP"] = os.path.join(PROJECT_DIR, DATA_FOLDER, COUNTRY_DROP)
    os.environ["CLEAN_FILE"] = os.path.join(PROJECT_DIR, DATA_FOLDER, CLEAN_FILE)

print("Setting Env Vars")
set_env_vars()