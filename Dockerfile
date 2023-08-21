FROM apache/airflow:2.6.3-python3.8
RUN pip install pandas
RUN pip install numpy
RUN pip install boto3
RUN pip install pytz
