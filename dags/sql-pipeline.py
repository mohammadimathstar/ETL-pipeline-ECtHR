from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


import os
from datetime import datetime


DOC_TYPE = "JUDGMENTS"
LANGUAGE = "ENG"

TABLE_NAME = f"{DOC_TYPE}_{LANGUAGE}"
GCS_BUCKET = "hudoc-metadata"  # Replace with your GCS bucket name
GCS_FOLDER = "data"  # Folder inside the GCS bucket where CSV files are stored
PROJECT_ID = "de-tutorials-69450"  # Replace with your GCP project ID
DATASET_NAME = "hudoc"  # Replace with your BigQuery dataset name

DATA_GCS_OBJECT_NAME = f"{GCS_FOLDER}/{DOC_TYPE}_{LANGUAGE}.csv"

LOCAL_CSV_PATH = os.path.join(os.getcwd(), 'data/dataset.csv')



default_args = {
    "start_date": datetime(2024, 1, 1),
    "catchup": False,
}

with DAG("create_bigquery_table", schedule_interval=None, default_args=default_args) as dag:

    # Create dataset if it doesn't exist
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        dataset_id=DATASET_NAME,
        exists_ok=True  # Avoid errors if the dataset already exists
    )

    # Function to list all CSV files in the GCS bucket
    def list_csv_files_in_gcs():
        gcs_hook = GCSHook() #gcp_conn_id="google_cloud_storage_connection")  # Define GCS connection ID
        files = gcs_hook.list(GCS_BUCKET, prefix=f"{GCS_FOLDER}/")  # List all objects in the GCS bucket
        csv_files = [file for file in files if file.endswith(".csv")]
        return csv_files

    
    table = DATA_GCS_OBJECT_NAME.split("/")[-1].split('.')[0]

    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        dataset_id=DATASET_NAME,
        table_id=table,
        exists_ok=True  # Avoid errors if the table already exists
    )
    
    load_csv_to_bq = GCSToBigQueryOperator(
        task_id='load_csv_to_bq',
        bucket=GCS_BUCKET,  # Replace with your GCS bucket name
        source_objects=[DATA_GCS_OBJECT_NAME],  # Replace with your CSV file path in GCS
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.{table}", #'your_project.your_dataset.your_table',  
        allow_quoted_newlines=True,
        max_bad_records=10,
        write_disposition='WRITE_APPEND',  # Options: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
        source_format='CSV',
        skip_leading_rows=1,  # Skip header row in CSV
        # field_delimiter=',',  # CSV delimiter
        autodetect=True,  # Set to True if you want BigQuery to auto-detect schema
        allow_jagged_rows=True,  # Allows rows with missing trailing columns
    )
    
    (
        create_dataset 
        >> create_table 
        >> load_csv_to_bq
    )





# def load_csv_to_bq(file_name):
#     # Construct the table ID (use the CSV file name without the extension)
#     table_name = file_name.split("/")[-1].split(".")[0]  # Extract file name without extension
    
#     # Define the source file path in GCS
#     source_file_path = f"gs://{GCS_BUCKET}/{GCS_FOLDER}/{file_name}"
    
#     # BigQuery Load operator to load the file into BigQuery
#     load_task = BigQueryLoadFileOperator(
#         task_id=f"load_{table_name}_to_bq",
#         bucket_name=GCS_BUCKET,
#         source_objects=[source_file_path],
#         destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.{table_name}",
#         source_format="CSV",
#         skip_leading_rows=1,  # Skip the header row of the CSV file
#         autodetect=True,  # Automatically detect the schema
#         write_disposition="WRITE_APPEND",  # Append to the table (change to "WRITE_TRUNCATE" to overwrite)
#         field_delimiter=",",  # CSV delimiter
#     )
    
#     return load_task


# with DAG("load_multiple_csv_to_bigquery", schedule_interval=None, default_args=default_args) as dag:
    
#     # Step 1: List all CSV files in the GCS bucket
#     list_csv_files = PythonOperator(
#         task_id="list_csv_files",
#         python_callable=list_csv_files_in_gcs,
#         provide_context=True,
#     )
    
#     # Step 2: For each file, load it into BigQuery as a separate table
#     csv_files = list_csv_files.output  # The output is a list of CSV files
    # for file_name in csv_files:
    #     load_task = load_csv_to_bq(file_name)
        
    #     # Task dependencies
    #     list_csv_files >> load_task
