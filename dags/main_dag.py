from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='glue_job_dag',
    default_args=default_args,
    description='A DAG to trigger AWS Glue job',
    schedule_interval='@hourly',  # Adjust the schedule as needed
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Define the Glue Job Operator
    glue_job = GlueJobOperator(
        task_id='run_glue_job',
        job_name='glue',  # Replace with your Glue job name
        aws_conn_id='default',  # Adjust if you're using a different connection ID
        script_args={
            '--bucket_name': 'templatedemobucket',
            '--config_file': 'config.yml',  # Replace with your config file or parameters
        },
        region_name='us-east-1',  # Replace with your AWS region
    )

    # Task sequence
    glue_job
