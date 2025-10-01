import os
import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.sensors.external_task import ExternalTaskSensor # For more complex scenarios, consider this or GCSObjectExistenceSensor
from airflow.sensors.google.cloud.gcs import GCSObjectExistenceSensor
from airflow.operators.bash import BashOperator

# Load configuration from JSON file
current_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(current_dir, 'config', 'gcp_config.json')
with open(config_path, 'r') as f:
    gcp_config = json.load(f)

GCP_PROJECT_ID = gcp_config['gcp_project_id']
GCS_RAW_DATA_BUCKET = gcp_config['gcs_raw_data_bucket']
GCS_DAGS_FOLDER = gcp_config['gcs_dags_folder'] # This is the folder inside your DAGs bucket

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1), # Use a static start date
    'retries': 1, # Set retries at DAG level
    'retry_delay': timedelta(minutes=5),
    'project_id': GCP_PROJECT_ID,
    'schedule_interval': None, # This DAG is often triggered externally or by a sensor
}

with DAG(
    dag_id='ecom_ingestion_dag',
    default_args=default_args,
    description='Ingests raw e-commerce sales data from a landing GCS bucket to a processed folder.',
    schedule_interval='@daily', # Example: Run daily to check for new files
    catchup=False,
    tags=['ecommerce', 'ingestion', 'gcs'],
    is_paused_upon_creation=True, # Pause by default, enable after setup
) as dag:
    # Use BashOperator to create 'processed' subfolder if it doesn't exist
    # This is a simple way; in a real env, bucket structure might be pre-defined.
    # Note: gsutil might not be directly available in Cloud Composer workers depending on env setup.
    # A more robust approach might be to use PythonOperator with Google Cloud Storage client library.
    create_processed_folder = BashOperator(
        task_id='create_processed_folder_if_not_exists',
        bash_command=f'gsutil ls gs://{GCS_RAW_DATA_BUCKET}/processed/ || gsutil mkdir gs://{GCS_RAW_DATA_BUCKET}/processed/',
    )

    # Sensor to detect new raw files in the landing zone.
    # We are looking for a specific file pattern, assuming daily files for simplicity.
    # In a real-world scenario, you might list objects and process them iteratively.
    raw_file_sensor = GCSObjectExistenceSensor(
        task_id='sense_new_raw_sales_file',
        bucket=GCS_RAW_DATA_BUCKET,
        object=f'raw_sales/{{ ds_nodash }}_sales.csv', # Example: 20230101_sales.csv
        google_cloud_conn_id='google_cloud_default',
        poke_interval=60, # Check every 60 seconds
        timeout=60 * 60 * 24, # Timeout after 24 hours
    )

    # Move the raw file to a 'processed' subfolder to prevent reprocessing.
    # This ensures idempotency - if the DAG runs again, it won't re-process the same file from the landing zone.
    move_raw_to_processed = GCSToGCSOperator(
        task_id='move_raw_to_processed',
        source_bucket=GCS_RAW_DATA_BUCKET,
        source_object=f'raw_sales/{{{{ ds_nodash }}_sales.csv',
        destination_bucket=GCS_RAW_DATA_BUCKET,
        destination_object=f'processed/raw_sales/{{{{ ds_nodash }}_sales.csv',
        move_object=True, # This will delete the source object after copying
        google_cloud_conn_id='google_cloud_default',
    )

    # Define task dependencies
    create_processed_folder >> raw_file_sensor >> move_raw_to_processed