# Use an official Airflow image as a base
FROM apache/airflow:2.7.2-python3.8

# Install additional dependencies
RUN pip install boto3 pyyaml

# Copy your DAGs and configuration files
COPY dags /opt/airflow/dags
COPY config /opt/airflow/config

# Set the entrypoint for Airflow
ENTRYPOINT ["airflow", "webserver"]
