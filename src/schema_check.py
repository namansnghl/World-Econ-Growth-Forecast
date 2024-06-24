# Clean blank rows before schema generation
# Remove float from some columns which are compulsary

import src
import pandas as pd
from utilities.logger import setup_logging
from pydantic import ValidationError
from data_definition import *

my_logger = setup_logging()
my_logger.set_logger("main_logger")


def verify_raw_data(file_path):

    data = pd.read_excel(file_path, header=0)
    data.columns = map(str, data.columns)

    try:
        my_logger.logger.info("Validating the Raw Schema")
        WeoRawContainer(records=[WeoRawRecord(**item) for item in data.to_dict(orient='records')])
    except ValidationError as e:
        my_logger.logger.critical("Validation Failed")
        my_logger.logger.error(str(e))