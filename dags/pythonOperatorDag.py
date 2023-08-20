from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import subprocess
from airflow.contrib.sensors.file_sensor import FileSensor
import os
import pandas as pd
import numpy as np

default_args = {
    'owner': 'thev',
    'retries': 2,
    'retry_delay': timedelta(seconds=30)

}

def send_transformed_data():
    followUp="send transformed data to a database"
    return followUp

def delete_data(name, ti):
    followUp = ti.xcom_pull(task_ids='send_transformed_data_task')
    print(f"hello {name}! {followUp}")

def start_transforming():
    #command = 'logman start "New Data Collector Set"'
    #print("starting logging")
    
    # current = os.getcwd()
    # print("current:",current)
    # target_dir=(current+"/data")
    # print("target:",target_dir)
    # target_dir = os.getcwd()+"/data/DESKTOP-67P0ISA_DataCollector01.csv"
    # data_frame = pd.read_csv(target_dir)
    # print(data_frame)
    # print(data_frame[0])

    target_dir = os.getcwd()+"/data/DESKTOP-67P0ISA_DataCollector01.csv"
    data_frame = pd.read_csv(target_dir)

    new_column_name = {'(PDH-CSV 4.0) (Eastern Daylight Time)(240)': ' Date Time'}
    data_frame.rename(columns=new_column_name, inplace=True)

    memory_ordered_filtered =  [data_frame.columns[0]] + [col for col in data_frame.columns if "Memory" in col]
    memory_data = data_frame[memory_ordered_filtered]

    processor_ordered_filtered =  [data_frame.columns[0]] + [col for col in data_frame.columns if "rocessor" in col]
    processor_data = data_frame[processor_ordered_filtered]

    datas=[memory_data, processor_data]

    for x in range(0,len(datas)):
        white_space_removed = datas[x].applymap(lambda x: x.strip() if isinstance(x, str) else x)
        empty_values_filled = white_space_removed.replace("", "0")
        empty_values_filled.columns = empty_values_filled.columns.str.lstrip("\\DESKTOP-67P0ISA\\")
        empty_values_filled.columns = empty_values_filled.columns.str.lstrip("Memory")
        empty_values_filled.columns = empty_values_filled.columns.str.lstrip("rocessor")
        output_filename = os.getcwd()+f'/data/output_{x}.csv'
        empty_values_filled.to_csv(output_filename, index=False, encoding='utf-8')

    # Specify the file name you want to delete
    #file_to_delete = "delete.txt"

    # Get the current script's directory
    #current_dir = os.path.dirname(__file__)

    # Navigate two levels up to reach the parent directory

    # Construct the full path to the file
    #file_path = os.path.join(target_dir, file_to_delete)
    #os.remove(file_path)
    #subprocess.run(command, shell=False)

with DAG(
    default_args=default_args,
    dag_id='my_first_python_dag',
    description='my first dag using python operator',
    start_date=datetime(2023, 8, 16),
    schedule_interval=timedelta(minutes=1)
) as dag:
    task1= PythonOperator(
        task_id='delete_data_task',
        python_callable=delete_data,
        op_kwargs={'name': 'thev'}
    )

    task2 = PythonOperator(
        task_id = 'send_transformed_data_task',
        python_callable = send_transformed_data
    )

    task3 = PythonOperator(
        task_id = 'start_transforming',
        python_callable = start_transforming
    ) 

    target_dir = os.getcwd()+"/data/DESKTOP-67P0ISA_DataCollector01.csv"

    wait_for_file_task = FileSensor(
        task_id='wait_for_file_task',
        filepath=target_dir,
        poke_interval=5,  # Set the poke interval in seconds
        timeout=16,
        fs_conn_id='file_system',
        mode='poke'
    )


    wait_for_file_task >> task3