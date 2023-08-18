from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import subprocess




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
    command = 'logman start "Data Collector Set"'
    subprocess.run(command, shell=True)

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

    task2 >> task1