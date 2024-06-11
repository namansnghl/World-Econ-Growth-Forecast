"""
The Airflow DAGs for data pipeline
"""
import os
import sys
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.configuration import conf

# Add the src and utilities directories to sys.path
SRC_DIR = '/opt/airflow/src'
UTILITIES_DIR = '/opt/airflow/utilities'
if SRC_DIR not in sys.path:
    sys.path.append(SRC_DIR)
if UTILITIES_DIR not in sys.path:
    sys.path.append(UTILITIES_DIR)

# Import necessary modules from your project
from data_cleaner import process_data
from data_loader import import_data
from transform import transform_data
from filter_data import filter_data

# Define default paths
PROJECT_DIR = '/opt/airflow'  # Use the mounted volume path
DEFAULT_EXCEL_PATH = os.path.join(PROJECT_DIR, 'data', 'raw_data', 'IMF_WEO_Data.xlsx')
DEFAULT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data', 'processed_data', 'raw_data.pkl')
DEFAULT_COUNTRIES_TO_DROP_PATH = os.path.join(PROJECT_DIR, 'data', 'raw_data', 'countries_to_drop.csv')

# Set Airflow configuration to enable XCom pickling
conf.set('core', 'enable_xcom_pickling', 'True')

# Define default arguments for the DAG
data_load_args = {
    'owner': 'manvithby',
    'start_date': datetime(2024, 5, 31, 18),
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
with DAG(
    dag_id='Data_Load_and_Transform',
    default_args=data_load_args,
    description='DAGs for Data Pipeline',
    schedule_interval=None,  # Set to None for manual triggering
    catchup=False  # Don't backfill past dates
) as dag:
    
    # Task to load data
    extract_data = PythonOperator(
        task_id='Extract_Data',
        python_callable=import_data,
        op_kwargs={'excel_path': DEFAULT_EXCEL_PATH, 'pickle_path': DEFAULT_PICKLE_PATH}
    )

    # Task to clean data
    clean_data = PythonOperator(
        task_id='Basic_Cleaning',
        python_callable=process_data,
        op_kwargs={'pickle_path': '{{ ti.xcom_pull(task_ids="Extract_Data") }}'}
    )

    # Task to transform data
    transform_dataset = PythonOperator(
        task_id='Transform',
        python_callable=transform_data,
        op_kwargs={'pickle_path': '{{ ti.xcom_pull(task_ids="Basic_Cleaning") }}'}
    )
    
    # Task to transform data
    filter_dataset = PythonOperator(
        task_id='filter_dataset',
        python_callable=filter_data,
        op_kwargs={'pickle_path': '{{ ti.xcom_pull(task_ids="transform_dataset") }}', 'countries_to_drop_path':DEFAULT_COUNTRIES_TO_DROP_PATH},
        provide_context=True
    )
    # Define task dependencies
    extract_data >> clean_data >> transform_dataset >> filter_dataset

    # Optional: CLI access to the DAG
    if __name__ == "__main__":
        dag.cli()
