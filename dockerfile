# Use the official Apache Airflow image
FROM apache/airflow:2.9.2

# Set the working directory
WORKDIR /opt/airflow

# Copy your DAGs, plugins, and other configurations
COPY dags/ /opt/airflow/dags/
COPY plugins/ /opt/airflow/plugins/
