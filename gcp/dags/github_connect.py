"""
The Airflow DAGs for connecting Airflow with GitHub to pull DAG execution scripts
"""
import os
import sys
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.configuration import conf

# Add the src and utilities directories to sys.path
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the parent directory to sys.path
sys.path.append(PROJECT_DIR)
os.environ["PROJECT_DIR"] = PROJECT_DIR

GITHUB_RAW_URL = 'https://raw.githubusercontent.com/namansnghl/World-Econ-Growth-Forecast/development/gcp/dags'
BUCKET_NAME = 'us-east1-airflow-composer-05582c7e-bucket'
GCS_DAGS_PATH = f'gs://{BUCKET_NAME}/dags/'

# Set Airflow configuration to enable XCom pickling
conf.set('core', 'enable_xcom_pickling', 'True')

# Define default arguments for the DAG
data_load_args = {
    'owner': 'manvithby',
    'start_date': datetime(2024, 5, 31, 18),
    'retries': 5,
    'retry_delay': timedelta(seconds=30)
}

# Define the DAG
with DAG(
    dag_id='Connect_Github',
    default_args=data_load_args,
    description='Pull DAG execution scripts',
    schedule_interval=None,  # Set to None for manual triggering
    catchup=False  # Don't backfill past dates
) as dag:
    
       # Helper function to create BashOperator tasks
    def create_pull_task(task_id, script_name):
        return BashOperator(
            task_id=task_id,
            retries=3, 
            retry_delay=timedelta(seconds=30),
            bash_command=(
                f'curl -o /tmp/{script_name} {GITHUB_RAW_URL}/{script_name} && '
                f'gsutil cp /tmp/{script_name} {GCS_DAGS_PATH}{script_name}'
            ),
            dag=dag,
    )
    
     # Helper function to create GCSObjectExistenceSensor tasks
    def create_sensor_task(task_id, object_name):
        return GCSObjectExistenceSensor(
            task_id=task_id,
            bucket=BUCKET_NAME,
            object=f'dags/{object_name}',
            timeout=300,
            poke_interval=60,
            dag=dag,
    )
    
    # Create tasks to pull scripts from GitHub and upload to GCS
    pull_data_loader = create_pull_task('pull_data_loader_script', 'data_loader.py')
    pull_filter_data = create_pull_task('pull_filter_data_script', 'filter_data.py')
    pull_data_cleaner = create_pull_task('pull_data_cleaner_script', 'data_cleaner.py')
    pull_logger = create_pull_task('pull_logger_script', 'logger.py')
    pull_transform = create_pull_task('pull_transform_script', 'transform.py')
    pull_log_config = create_pull_task('pull_log_config_script', 'log_config.json')

    # Create sensor tasks to check if scripts exist in GCS
    check_data_loader = create_sensor_task('check_data_loader_script', 'data_loader.py')
    check_filter_data = create_sensor_task('check_filter_data_script', 'filter_data.py')
    check_data_cleaner = create_sensor_task('check_data_cleaner_script', 'data_cleaner.py')
    check_logger = create_sensor_task('check_logger_script', 'logger.py')
    check_transform = create_sensor_task('check_transform_script', 'transform.py')
    check_log_config = create_sensor_task('check_log_config_script', 'log_config.json')

    # At the end of the this DAG trigger data pipeline dag using TriggerDagRunOperator
    trigger_second_dag = TriggerDagRunOperator(
        task_id='trigger_second_dag',
        trigger_dag_id='Data_Load_and_Transform', 
        dag=dag,
    )

    # Setting the dependencies between tasks
    pull_data_loader >> check_data_loader >> trigger_second_dag
    pull_filter_data >> check_filter_data >> trigger_second_dag 
    pull_data_cleaner >> check_data_cleaner >> trigger_second_dag
    pull_logger >> check_logger >> trigger_second_dag
    pull_transform >> check_transform >> trigger_second_dag
    pull_log_config >> check_log_config >> trigger_second_dag
    
