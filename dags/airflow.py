"""
The Airflow DAGs for data pipeline
"""
import os, sys
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.configuration import conf

# Add the src and utilities directories to sys.path
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the parent directory to sys.path
sys.path.append(PROJECT_DIR)

import src # Do not remove
from src.data_processing import pre_processing
from src.data_loader import load_file, save_file
from src.transform import transform_data
from src.filter_data import *
from src.schema_check import verify_raw_data

# Define default paths
DEFAULT_RAW_PATH = os.environ["RAW_FILE"]
DEFAULT_TRANSF_PATH = os.environ["PROCESSED_FILE"]
DEFAULT_COUNTRIES_TO_DROP_PATH = os.environ["COUNTRY_DROP"]
DEFAULT_CLEAN_PATH = os.environ["CLEAN_FILE"]

# Set Airflow configuration to enable XCom pickling
conf.set('core', 'enable_xcom_pickling', 'True')

# Define default arguments for the DAG
data_load_args = {
    'owner': 'neu_weo_team',
    'start_date': datetime(2024, 5, 31, 18),
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

# Define the DAG
with DAG(
    dag_id='Data_Load_and_Transform',
    default_args=data_load_args,
    description='DAG for Data load from raw file and process it',
    schedule_interval=None,
    catchup=False,
) as dag1:
    
    # Task to check schema
    raw_validate = PythonOperator(
        task_id='Raw_Schema_Verification',
        python_callable=verify_raw_data,
        op_kwargs={'file_path': DEFAULT_RAW_PATH},
    )

    # Task to load data
    load_raw = PythonOperator(
        task_id='Load_Raw_File',
        python_callable=load_file,
        op_kwargs={'path': DEFAULT_RAW_PATH},
        provide_context = True,
    )

    # Task to pre processes data
    pre_process_data = PythonOperator(
        task_id='Pre_Processing',
        python_callable=pre_processing,
        op_args=[load_raw.output],
        provide_context = True,
    )

    # Task to transform data
    transform = PythonOperator(
        task_id='Transform_Data',
        python_callable=transform_data,
        op_args=[pre_process_data.output],
        provide_context = True,
    )

    # Save data
    save_data = PythonOperator(
        task_id='Save_Data_to_Pickle',
        python_callable=save_file,
        op_kwargs={'df': transform.output, 'path': DEFAULT_TRANSF_PATH},
        provide_context = True,
    )

    trigger_cleaning = TriggerDagRunOperator(
        trigger_dag_id = 'Data_Cleaning',
        task_id='Trigger_Data_Cleaning_DAG',
        trigger_rule='all_done',)
    
    # Define task dependencies
    raw_validate >> load_raw >> pre_process_data >> transform >> save_data >> trigger_cleaning



with DAG(
    dag_id='Data_Cleaning',
    default_args=data_load_args,
    description='DAGs for Data Cleaning',
    schedule_interval=None,
    catchup=False,
) as dag2:
    
    # Task to load data
    load_pro_file = PythonOperator(
        task_id='Load_Processed_File',
        python_callable=load_file,
        op_kwargs={'path': DEFAULT_TRANSF_PATH, 'type': 'pickle'},
        provide_context = True,
    )

    # Task to load drop countries
    load_drop_countries = PythonOperator(
        task_id='Load_Countries_to_Drop',
        python_callable=load_countries_to_drop,
        op_args=[DEFAULT_COUNTRIES_TO_DROP_PATH],
        provide_context = True,
    )

    # Task to clean data
    clean_data = PythonOperator(
        task_id='Data_Cleaning',
        python_callable=filter_data,
        op_args=[load_pro_file.output, load_drop_countries.output],
        provide_context = True,
    )

    # Save data
    save_data = PythonOperator(
        task_id='Save_Data_to_Pickle',
        python_callable=save_file,
        op_kwargs={'df': clean_data.output, 'path': DEFAULT_CLEAN_PATH},
        provide_context = True,
    )
    
    load_pro_file >> clean_data
    load_drop_countries >> clean_data
    clean_data >> save_data
    
# Optional: CLI access to the DAG
if __name__ == "__main__":
    dag1.cli()
    dag2.cli()
