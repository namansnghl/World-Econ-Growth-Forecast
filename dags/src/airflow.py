"""
The Airflow Dags for data pipeline
"""
import os
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow import configuration
from src.data_cleaner import process_data
from src.data_loader import import_data
from src.transform import transform_data

# Determine the absolute path of the project directory
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DEFAULT_EXCEL_PATH = os.path.join(PROJECT_DIR, 'data', 'raw_data', 'IMF_WEO_Data.xlsx')
DEFAULT_PICKLE_PATH = os.path.join(PROJECT_DIR, 'data', 'processed_data', 'raw_data.pkl')

# Set Airflow configuration to enable XCom pickling
configuration.set('core', 'enable_xcom_pickling', 'True')
 
# Define default arguments for the DAG
default_args = {
    'owner': 'manvithby',
    'start_date': datetime(2024, 5, 31, 18),
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
with DAG(
    dag_id='data_pipeline',
    default_args=default_args,
    description='DAGs for Data Pipeline',
    schedule_interval=None,  # Set to None for manual triggering
    catchup=False  # Don't backfill past dates

) as dag:
    
    # Task to load data
    load_data = PythonOperator(
        task_id='load_data',
        python_callable=import_data,
        op_kwargs={'excel_path': DEFAULT_EXCEL_PATH, 'pickle_path': DEFAULT_PICKLE_PATH}
    )

    # Task to clean data
    clean_data = PythonOperator(
        task_id='clean_data',
        python_callable=process_data,
        op_kwargs={'pickle_path': '{{ ti.xcom_pull(task_ids="load_data") }}'}
    )

    # Task to transform data
    transform_dataset = PythonOperator(
        task_id='transform_dataset',
        python_callable=transform_data,
        op_kwargs={'pickle_path': '{{ ti.xcom_pull(task_ids="clean_data") }}'}
    )
    
    # Define task dependencies
    load_data >> clean_data >> transform_dataset

    # Optional: CLI access to the DAG
    if __name__ == "__main__":
        dag.cli()
