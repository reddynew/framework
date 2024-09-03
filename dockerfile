# Use the official Airflow image from Apache
FROM apache/airflow:2.7.0-python3.11

# Set environment variables for Airflow
ENV AIRFLOW_HOME=/opt/airflow

# Install any additional Python packages you need
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your Airflow DAGs and configuration files into the container
COPY dags/ ${AIRFLOW_HOME}/dags/
COPY config/ ${AIRFLOW_HOME}/config/

# Set the working directory to the Airflow home directory
WORKDIR ${AIRFLOW_HOME}

# Initialize Airflow database and start the Airflow webserver and scheduler
CMD ["bash", "-c", "airflow db init && airflow webserver --daemon && airflow scheduler"]
