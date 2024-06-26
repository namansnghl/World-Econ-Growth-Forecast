"""
The Airflow DAGs for data pipeline
"""
import os, sys
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.configuration import conf
import src # Do not remove
from src.data_processing import pre_processing
from src.data_loader import load_file, save_file
from src.transform import transform_data
from src.filter_data import *
from src.schema_check import verify_raw_data
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from airflow.models import Variable

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
    'retries': 3,
    'retry_delay': timedelta(minutes=0.1)
}

from_email = Variable.get("FROM_EMAIL")
to_email = 'weoteam@googlegroups.com'
smtp_port = Variable.get("SMTP_PORT")
smtp_host = Variable.get("SMTP_HOST")
smtp_password = Variable.get("PASSWORD")
email_text1 = '<p>The task Load and Transform succeeded and triggered Data Cleaning.</p>'
email_text2 = "<p>The task Data Cleaning is completed.</p>"
email_fail1 = '<p>The DAG to Load and Transform Raw data has failed</p>'
email_fail2 = '<p>The DAG to clean data has failed</p>'

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

def notify_success(message, **kwargs):
    send_email_func('Success Notification from Airflow', message)

def notify_failure(context, message, **kwargs):
    send_email_func('Failure Notification from Airflow', message)




# Define the DAG
with DAG(
    dag_id='Data_Load_and_Transform',
    default_args=data_load_args,
    description='DAG for Data load from raw file and process it',
    schedule_interval=None,
    catchup=False,
    on_failure_callback=lambda context: notify_failure(context, email_fail1),
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
        trigger_rule='all_success',
        )
    
    send_email = PythonOperator(
        task_id='send_email',
        python_callable=notify_success,
        op_args=[email_text1],
        trigger_rule='all_success',
        provide_context=True,
        )
    
    # Define task dependencies
    raw_validate >> load_raw >> pre_process_data >> transform >> save_data >> trigger_cleaning >> send_email



with DAG(
    dag_id='Data_Cleaning',
    default_args=data_load_args,
    description='DAGs for Data Cleaning',
    schedule_interval=None,
    catchup=False,
    on_failure_callback=lambda context: notify_failure(context, email_fail2),
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

    send_email = PythonOperator(
        task_id='send_email',
        python_callable=notify_success,
        op_kwargs={'message': email_text2},
        trigger_rule='all_success',
        provide_context=True,
        )
    
    load_pro_file >> clean_data
    load_drop_countries >> clean_data
    clean_data >> save_data >> send_email
    
# Optional: CLI access to the DAG
if __name__ == "__main__":
    dag1.cli()
    dag2.cli()
