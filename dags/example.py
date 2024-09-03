from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='dummy_dag',
    default_args=default_args,
    description='A simple dummy DAG for testing',
    schedule_interval='@daily',  # Run the DAG once a day
    catchup=False,  # Disable backfilling
) as dag:

    # Define the tasks
    start_task = EmptyOperator(
        task_id='start_task'
    )

    end_task = EmptyOperator(
        task_id='end_task'
    )

    # Set up the task dependencies
    start_task >> end_task
