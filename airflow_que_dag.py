from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.python_operator import PythonOperator
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 14),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'process_csv_to_parquet',
    default_args=default_args,
    description='Process CSV files to Parquet format and add extra columns',
    schedule_interval='@daily',
)

# Task 1: Read CSV files
def read_csv_files():
    # Code to read CSV files from a folder
    csv_folder = 'dags/GOOGL_data.csv'
    csv_files = [os.path.join(csv_folder, f) for f in os.listdir(csv_folder) if f.endswith('.csv')]
    return csv_files

read_csv_task = PythonOperator(
    task_id='read_csv_files',
    python_callable=read_csv_files,
    dag=dag,
)

# Task 2: Spark job to read CSV files and save as Parquet
submit_job_1 = SparkSubmitOperator(
    task_id='spark_job_1',
    application='/path/to/spark_job1.py',
    conn_id='spark_default',
    dag=dag,
)

submit_job_1.set_upstream(read_csv_task)

# Task 3: Spark job to add extra columns and save as Parquet
submit_job_2 = SparkSubmitOperator(
    task_id='spark_job_2',
    application='/path/to/spark_job2.py',
    conn_id='spark_default',
    dag=dag,
)

submit_job_2.set_upstream(submit_job_1)
