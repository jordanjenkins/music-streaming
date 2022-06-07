import os
import pyarrow.csv as pa
import pyarrow.parquet as pq
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

from google.cloud import storage


default_args ={
    'owner' : 'airflow'
}

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow')

URL = 'https://github.com/ankurchavda/streamify/raw/main/dbt/seeds/songs.csv'
CSV_FILENAME = 'songs.csv'
PARQUET_FILENAME = CSV_FILENAME.replace('csv', 'parquet')

CSV_FILE = f'{AIRFLOW_HOME}/{CSV_FILENAME}'
PARQUET_FILE = f'{AIRFLOW_HOME}/{PARQUET_FILENAME}'
TABLE_NAME = 'songs'

GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
GCP_GCS_BUCKET = os.environ.get('GCP_GCS_BUCKET')
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET', 'streams_stg')

def convert_to_parquet(csv_file, parquet_file):
    if not csv_file.endswith('csv'):
        raise ValueError('The input is not a csv')
    
    table = pa.read_csv(csv_file)
    pq.write_table(table, parquet_file)

def upload_to_gcs(file_path, bucket_name, blob_name):
    """
    Upload the downloaded file to GCS
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(file_path)

with DAG(
    dag_id = f'load_songs_dag',
    default_args = default_args,
    description = f'Execute once to initially load songs to bigQuery',
    # schedule_interval = "@once",
    start_date = datetime(2022,6,6),
    end_date = datetime(2022,6,6),
    catchup = True,
    tags = ['music-streams']
) as dag:

    download_songs_file_task = BashOperator(
        task_id = "download_songs_file",
        bash_command = f'curl -sSLf {URL} > {CSV_FILE}'
    )

    convert_to_parquet_task = PythonOperator(
        task_id = "convert_to_parquet",
        python_callable = convert_to_parquet, 
        op_kwargs = {
            'csv_file' : CSV_FILE,
            'parquet_file': PARQUET_FILE
        }
    )

    upload_to_gcs_task = PythonOperator(
        task_id = 'upload_to_gcs',
        python_callable = upload_to_gcs,
        op_kwargs = {
            'file_path' : PARQUET_FILE,
            'bucket_name' : GCP_GCS_BUCKET,
            'blob_name' : f'{TABLE_NAME}/{PARQUET_FILENAME}'
        }
    )

    remove_files_from_local_task = BashOperator(
        task_id = 'remove_files_from_local',
        bash_command = f'rm {CSV_FILE} {PARQUET_FILE}'
    )

    create_external_table_task = BigQueryCreateExternalTableOperator(
        task_id = 'create_external_table_task',
        table_resource = {
            'tableReference': {
            'projectId': GCP_PROJECT_ID,
            'datasetId': BIGQUERY_DATASET,
            'tableId': TABLE_NAME,
            },
            'externalDataConfiguration': {
                'sourceFormat': 'PARQUET',
                'sourceUris': [f'gs://{GCP_GCS_BUCKET}/{TABLE_NAME}/*.parquet'],
            },
        }
    )

download_songs_file_task >> convert_to_parquet_task >> upload_to_gcs_task >> remove_files_from_local_task >> create_external_table_task