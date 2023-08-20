from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import subprocess


import os


default_args = {
    'owner': 'thev',
    'retries': 2,
    'retry_delay': timedelta(seconds=30)

}

def provide_question():
    followUp="How are you?"
    return followUp

def greet(name, ti):
    followUp = ti.xcom_pull(task_ids='provide_question_task')
    print(f"hello {name}! {followUp}")

def start_logging():
    command = 'logman start "New Data Collector Set"'
    print("starting logging")
    
    current = os.getcwd()
    print("current:",current)
    target_dir=(current+"/data")
    print("target:",target_dir)

    # Specify the file name you want to delete
    file_to_delete = "delete.txt"

    # Get the current script's directory
    #current_dir = os.path.dirname(__file__)

    # Navigate two levels up to reach the parent directory

    # Construct the full path to the file
    file_path = os.path.join(target_dir, file_to_delete)
    os.remove(file_path)
    #subprocess.run(command, shell=False)

with DAG(
    default_args=default_args,
    dag_id='my_first_python_dag',
    description='my first dag using python operator',
    start_date=datetime(2023, 8, 16),
    schedule_interval='@daily'
) as dag:
    task1= PythonOperator(
        task_id='greet',
        python_callable=greet,
        op_kwargs={'name': 'thev'}
    )

    task2 = PythonOperator(
        task_id = 'provide_question_task',
        python_callable = provide_question
    )

    task3 = PythonOperator(
        task_id = 'start_logging',
        python_callable = start_logging
    )

    task2 >> task1 >> task3