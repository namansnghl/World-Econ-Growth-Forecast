"""
The Airflow DAGs for data pipeline
"""
import os, sys
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.configuration import conf
from airflow.operators.bash import BashOperator

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
DEFAULT_PROJECT_DIR = os.environ["PROJECT_DIR"]

# Set Airflow configuration to enable XCom pickling
conf.set('core', 'enable_xcom_pickling', 'True')

# Define default arguments for the DAG
data_load_args = {
    'owner': 'neu_weo_team',
    'start_date': datetime(2024, 5, 31, 18),
    'retries': 3,
    'retry_delay': timedelta(minutes=0.1)
}

email_id = 'weoteam@googlegroups.com'
email_text1 = '<p>The task Load and Transform succeeded and triggered Data Cleaning.</p>'
email_text2 = "<p>The task Data Cleaning is completed.</p>"
email_fail1 = '<p>The DAG to Load and Transform Raw data has failed</p>'
email_fail2 = '<p>The DAG to clean data has failed</p>'
email_text3 = "<p>The task Model Training & serve is completed.</p>"
email_fail3 = '<p>The DAG to Train Model & serve has failed</p>'

def notify_success(message, **kwargs):
    global email_id
    success_email = EmailOperator(
        task_id='success_email',
        to=email_id,
        subject='Success Notification from Airflow',
        html_content=message,
        dag=kwargs['dag']
    )
    success_email.execute(context=kwargs)

def notify_failure(context, message, **kwargs):
    global email_id
    failure_email = EmailOperator(
        task_id='failure_email',
        to=email_id,
        subject='Failure Notification from Airflow',
        html_content=message,
        dag=context['dag']
    )
    failure_email.execute(context=context)



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
    
with DAG(
    dag_id='Model_Training_Serving',
    default_args=data_load_args,
    description='DAGs for Serving Model',
    schedule_interval=None,
    catchup=False,
    on_failure_callback=lambda context: notify_failure(context, email_fail3),
) as dag3:
    train_model = BashOperator(
            task_id="Train_Model",
            retries=3, 
            retry_delay=timedelta(seconds=30),
            bash_command=(f"python {DEFAULT_PROJECT_DIR}/gcp/train/train.py")
    )

    flask = BashOperator(
            task_id="Flask_API_Update",
            retries=3, 
            retry_delay=timedelta(seconds=30),
            bash_command=(f"python {DEFAULT_PROJECT_DIR}/gcp/serve/predict.py")
    )
    streamlit = BashOperator(
            task_id="Start_Streamlit",
            retries=3, 
            retry_delay=timedelta(seconds=30),
            bash_command=(f"python {DEFAULT_PROJECT_DIR}/gcp/serve/streamlit.py")
    )

    build = BashOperator(
            task_id="Build_Vertex_AI",
            retries=3, 
            retry_delay=timedelta(seconds=30),
            bash_command=(f"python {DEFAULT_PROJECT_DIR}/gcp/build.py")
    )

    send_email = PythonOperator(
        task_id='send_email',
        python_callable=notify_success,
        op_kwargs={'message': email_text3},
        trigger_rule='all_success',
        provide_context=True,
        )
    
    train_model >> flask >> streamlit >> build >> send_email
    
# Optional: CLI access to the DAG
if __name__ == "__main__":
    dag1.cli()
    dag2.cli()
