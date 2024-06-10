"""
The Airflow Dags for data pipeline
"""
import os, sys
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow import configuration

# Determine the absolute path of the project directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the parent directory to sys.path
sys.path.append(PROJECT_DIR)
os.environ["PROJECT_DIR"] = PROJECT_DIR
from src.data_cleaner import *
from src.data_loader import import_data
from src.transform import transform_data

DEFAULT_EXCEL_PATH = os.path.join('data', 'raw_data', 'IMF_WEO_Data.xlsx')
DEFAULT_PICKLE_PATH = os.path.join('data', 'processed_data', 'raw_data.pkl')

# Set Airflow configuration to enable XCom pickling
configuration.set('core', 'enable_xcom_pickling', 'True')
 
# Define default arguments for the DAG
data_load_args = {
    'owner': 'manvithby',
    'start_date': datetime(2024, 5, 31, 18),
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
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
    load_data = PythonOperator(
        task_id='Load_Data',
        python_callable=import_data,
        op_kwargs={'excel_path': DEFAULT_EXCEL_PATH, 'pickle_path': DEFAULT_PICKLE_PATH}
    )

    # Task to clean data
    clean_data = PythonOperator(
        task_id='Basic_Cleaning',
        python_callable=process_data,
        op_kwargs={'pickle_path': '{{ ti.xcom_pull(task_ids="Load_Data") }}'}
    )

    # Task to transform data
    transform_dataset = PythonOperator(
        task_id='Transform',
        python_callable=transform_data,
        op_kwargs={'pickle_path': '{{ ti.xcom_pull(task_ids="Basic_Cleaning") }}'}
    )
    
    # Define task dependencies
    load_data >> clean_data >> transform_dataset

    # Optional: CLI access to the DAG
    if __name__ == "__main__":
        dag.cli()

# Define default arguments for the DAG
clean_data_args = {
    'owner': 'namansnghl',
    'start_date': datetime(2024, 5, 31, 18),
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

# Define the DAG
with DAG(
    dag_id='Data_Cleaning',
    default_args=clean_data_args,
    description='DAG for Data cleaning',
    schedule_interval=None,  # Set to None for manual triggering
    catchup=False  # Don't backfill past dates

) as dag:
    
    # Task to load data
    
    
    # Define task dependencies
    

    # Optional: CLI access to the DAG
    if __name__ == "__main__":
        dag.cli()