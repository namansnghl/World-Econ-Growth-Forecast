# Clean blank rows before schema generation
# Remove float from some columns which are compulsary

import src
import pandas as pd
from utilities.logger import setup_logging
from pydantic import ValidationError
from src.schema_definition import *
from airflow.exceptions import AirflowException
from src.data_processing import convert_null_values

my_logger = setup_logging()
my_logger.set_logger("main_logger")


def verify_raw_data(file_path):
    """
    Uses Pydantic to verify Raw file schema
    """

    my_logger.logger.debug("Starting verify_raw_data()")
    my_logger.logger.debug("Reading File")
    data = pd.read_excel(file_path, header=0)
    data = convert_null_values(data)
    my_logger.logger.debug("Cleaning Nulls")
    data.columns = map(str, data.columns)

    try:
        my_logger.logger.info("Validating the Raw Schema")
        WeoRawContainer(records=[WeoRawRecord(**item) for item in data.to_dict(orient='records')])
    except ValidationError as e:
        my_logger.logger.critical("Validation Failed")
        my_logger.logger.error(str(e))
        raise AirflowException("Validation Failed: " + str(e))