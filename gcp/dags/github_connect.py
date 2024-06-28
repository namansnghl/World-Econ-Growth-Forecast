"""
The Airflow DAGs for connecting Airflow with GitHub to pull DAG execution scripts
"""
import os
import sys, src
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.configuration import conf
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from airflow.models import Variable

from_email = Variable.get("FROM_EMAIL")
to_email = 'weoteam@googlegroups.com'
smtp_port = Variable.get("SMTP_PORT")
smtp_host = Variable.get("SMTP_HOST")
smtp_password = Variable.get("PASSWORD")

conf.set('core', 'enable_xcom_pickling', 'True')

GITHUB_RAW_URL = 'https://raw.githubusercontent.com/namansnghl/World-Econ-Growth-Forecast/development/gcp/dags/src'
BUCKET_NAME = 'us-east1-composer-airflow-7e8e089d-bucket'
GCS_DAGS_PATH = f'gs://{BUCKET_NAME}/dags/src'
airflow_script_name = "https://raw.githubusercontent.com/namansnghl/World-Econ-Growth-Forecast/development/gcp/dags/airflow.py"
GCS_airflow_script_path = f'gs://{BUCKET_NAME}/dags/airflow.py'

# Set Airflow configuration to enable XCom pickling
conf.set('core', 'enable_xcom_pickling', 'True')



def send_email_func(subject, message):
    global from_email, to_email, smtp_port, smtp_host, smtp_password
    # Setup the MIME
    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = subject

    # Attach message
    msg.attach(MIMEText(message, 'html'))

    # Connect to the SMTP server
    server = smtplib.SMTP(smtp_host, smtp_port)
    server.starttls()
    server.login(from_email, smtp_password)

    # Send email
    server.sendmail(from_email, to_email, msg.as_string())

    # Disconnect from the server
    server.quit()

def notify_success(**kwargs):
    send_email_func('Success Notification from Airflow', "Latest Scripts have been pulled to GCP Composer")



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
    schedule_interval='0 0 * * *',  # Set to None for manual triggering
    catchup=False  # Don't backfill past dates
) as dag:
    
       # Helper function to create BashOperator tasks
    def create_pull_task(task_id, script_name):
        return BashOperator(
            task_id=task_id,
            retries=3, 
            retry_delay=timedelta(seconds=30),
            bash_command=(
                f'curl -sSL {GITHUB_RAW_URL}/{script_name} | gsutil cp - {GCS_DAGS_PATH}/{script_name}'
            ),
            dag=dag,
    )
    
     # Helper function to create GCSObjectExistenceSensor tasks
    def create_sensor_task(task_id, object_name):
        return GCSObjectExistenceSensor(
            task_id=task_id,    
            bucket=BUCKET_NAME,
            object=f'dags/src/{object_name}',
            timeout=300,
            poke_interval=60,
            dag=dag,
    )
    
    # Airflow.py pull
    pull_airflow_dags = BashOperator(
                task_id="pull_airflow_dags",
                retries=3, 
                retry_delay=timedelta(seconds=30),
                bash_command=(
                    f'curl -sSL {airflow_script_name} | gsutil cp - {GCS_airflow_script_path}'
                ),
    )

    check_airflow_dags = GCSObjectExistenceSensor(
                task_id="check_airflow_dags",
                bucket=BUCKET_NAME,
                object="dags/airflow.py",
                timeout=300,
                poke_interval=60,
    )


    # Create tasks to pull scripts from GitHub and upload to GCS
    pull_data_loader = create_pull_task('pull_data_loader_script', 'data_loader.py')
    pull_configini = create_pull_task('pull_pull_configini', 'config.ini')
    pull_src_init = create_pull_task('pull_src_init', '__init__.py')
    pull_filter_data = create_pull_task('pull_filter_data_script', 'filter_data.py')
    pull_data_processing = create_pull_task('pull_data_processing_script', 'data_processing.py')
    pull_logger = create_pull_task('pull_logger_script', 'logger.py')
    pull_transform = create_pull_task('pull_transform_script', 'transform.py')
    pull_log_config = create_pull_task('pull_log_config_script', 'log_config.json')
    pull_schema_check = create_pull_task('pull_schema_check_script', 'schema_check.py')
    pull_schema_def = create_pull_task('pull_schema_def_script', 'schema_definition.py')

    # Create sensor tasks to check if scripts exist in GCS
    check_data_loader = create_sensor_task('check_data_loader_script', 'data_loader.py')
    check_configini = create_sensor_task('check_cinfigini', 'config.ini')
    check_src_init = create_sensor_task('check_src_init', '__init__.py')
    check_filter_data = create_sensor_task('check_filter_data_script', 'filter_data.py')
    check_data_processing = create_sensor_task('check_data_processing_script', 'data_processing.py')
    check_logger = create_sensor_task('check_logger_script', 'logger.py')
    check_transform = create_sensor_task('check_transform_script', 'transform.py')
    check_log_config = create_sensor_task('check_log_config_script', 'log_config.json')
    check_schema_check = create_sensor_task('check_schema', 'schema_check.py')
    check_schema_def = create_sensor_task('schema_def', 'schema_definition.py')

    send_email = PythonOperator(
        task_id='send_email',
        python_callable=notify_success,
        trigger_rule='all_success',
        provide_context=True,
        )

    # Setting the dependencies between tasks
    pull_airflow_dags >> check_airflow_dags >> send_email
    pull_data_loader >> check_data_loader >> send_email
    pull_configini >> check_configini >> send_email
    pull_src_init >> check_src_init >> send_email
    pull_filter_data >> check_filter_data >> send_email 
    pull_data_processing >> check_data_processing >> send_email
    pull_logger >> check_logger >> send_email
    pull_transform >> check_transform >> send_email
    pull_log_config >> check_log_config >> send_email
    pull_schema_check >> check_schema_check >> send_email
    pull_schema_def >> check_schema_def >> send_email
