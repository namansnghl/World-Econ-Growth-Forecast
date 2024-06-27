FROM apache/airflow:2.9.1

# Install necessary packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the project files into the container
COPY . /opt/airflow/project_root

# Set the working directory
WORKDIR /opt/airflow/project_root
