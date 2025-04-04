from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

import os

from include.extract import extract_data, remove_local_file
from include.transform import transform_data

DOC_TYPE = "{{var.value.get('doc_type')}}"
LANGUAGE = "{{ var.value.get('language') }}"

LOCAL_CSV_PATH = os.path.join(os.getcwd(), 'data/dataset.csv')
BUCKET_NAME = 'hudoc-metadata'
DATA_GCS_OBJECT_NAME = f'data/{DOC_TYPE}_{LANGUAGE}.csv'


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}

with DAG(
    "ecthr_etl_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    create_bucket_task = GCSCreateBucketOperator(
        task_id='create_bucket',
        bucket_name=BUCKET_NAME,
    )

    
    extract_data_task = PythonOperator(
        task_id = 'download_data', 
        python_callable=extract_data,
        op_args=[LANGUAGE, DOC_TYPE, LOCAL_CSV_PATH]
    )

    transform_data_task = PythonOperator(
        task_id = 'transform_data', 
        python_callable=transform_data,
        op_args=[LOCAL_CSV_PATH]
    )

    upload_file_task = LocalFilesystemToGCSOperator(
        task_id='upload_file_to_bucket',
        bucket=BUCKET_NAME,
        src=LOCAL_CSV_PATH,
        dst=DATA_GCS_OBJECT_NAME,
    )

    remove_data_task = PythonOperator(
        task_id = "remove_local_data",
        python_callable=remove_local_file,
        op_args=[LOCAL_CSV_PATH]
    )
    (
        [create_bucket_task, extract_data_task] 
        >> transform_data_task 
        >> upload_file_task 
        >> remove_data_task
    )
    




 # sudo service postgresql stop