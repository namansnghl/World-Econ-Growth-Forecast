# Use the Apache Airflow base image
FROM apache/airflow:2.9.1

# Set the working directory
WORKDIR /opt/airflow

# Copy the project files into the container
COPY ./dags /opt/airflow/dags
COPY ./utilities /opt/airflow/utilities
COPY ./src /opt/airflow/src

# Copy the requirements file and install necessary packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Expose the DAGs folder to Airflow
ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags

# Ensure the src folder is in the Python path
# ENV PYTHONPATH="${PYTHONPATH}:/project_root/"
