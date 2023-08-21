from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor
import os
import pandas as pd
import numpy as np
import boto3
import pytz

default_args = {
    'owner': 'thev',
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}

def send_transformed_data(ti):
    print('starting send')
    datas = ti.xcom_pull(task_ids='start_transforming')
    print(datas)

    s3_client = boto3.client('s3')
    bucket_name = 'perf-mon-data'
    desired_timezone = pytz.timezone('America/Toronto')
    current_datetime = datetime.now(desired_timezone)
    for x in range(0, len(datas)):

        if(x==0):
            output_filename = 'memory_data.csv'
            output_file_path = os.path.join(os.getcwd(), 'data', 'output_0.csv')
        else:
            output_filename = 'processor_data.csv'
            output_file_path = os.path.join(os.getcwd(), 'data', 'output_1.csv')
        
        # Specify the S3 object key (path)
        s3_object_key = f'data/{current_datetime}/{output_filename}'  
        
        # Upload the file to S3
        s3_client.upload_file(output_file_path, bucket_name, s3_object_key)

    print("CSV files uploaded to S3 successfully.")


def delete_data():
    print('cleanup task starting')
    file_paths = [
        os.getcwd()+"/data/DESKTOP-67P0ISA_DataCollector01.csv",
        os.getcwd()+"/data/output_0.csv",
        os.getcwd()+"/data/output_1.csv"
    ]
    for file_path in file_paths:
        try:
            os.remove(file_path)
            print(f"Deleted {file_path}")
        except FileNotFoundError:
            print(f"{file_path} not found, skipping...")
        except Exception as e:
            print(f"Error deleting {file_path}: {e}")


def start_transforming():
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

    return datas

with DAG(
    default_args=default_args,
    dag_id='perf-monitor-python-dag',
    description='Python DAG for automated performance monitor',
    start_date=datetime(2023, 8, 20, 11, 0, 0),
    schedule_interval=timedelta(days=1)
) as dag:
    cleanup_data_task= PythonOperator(
        task_id='delete_data_task',
        python_callable=delete_data,
        op_kwargs={'name': 'thev'}
    )

    upload_data_s3 = PythonOperator(
        task_id = 'send_transformed_data_task',
        python_callable = send_transformed_data
    )

    clean_transform_task = PythonOperator(
        task_id = 'start_transforming',
        python_callable = start_transforming
    ) 

    target_dir = os.getcwd()+"/data/DESKTOP-67P0ISA_DataCollector01.csv"

    wait_for_file_task = FileSensor(
        task_id='wait_for_file_task',
        filepath=target_dir,
        poke_interval=3,
        timeout=7,
        fs_conn_id='file_system',
        mode='poke'
    )


    wait_for_file_task >> clean_transform_task >> upload_data_s3 >> cleanup_data_task