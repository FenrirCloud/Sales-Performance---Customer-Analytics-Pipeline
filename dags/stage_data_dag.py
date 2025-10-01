import os
import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.sensors.external_task import ExternalTaskSensor

# Load configuration from JSON file
current_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(current_dir, 'config', 'gcp_config.json')
with open(config_path, 'r') as f:
    gcp_config = json.load(f)

GCP_PROJECT_ID = gcp_config['gcp_project_id']
GCS_RAW_DATA_BUCKET = gcp_config['gcs_raw_data_bucket']
BIGQUERY_DATASET_STAGING = gcp_config['bigquery_dataset_staging']
GCS_DAGS_FOLDER = gcp_config['gcs_dags_folder']

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': GCP_PROJECT_ID,
    'schedule_interval': None,
}

# Define the schema for the raw CSV data
# This is crucial for GCSToBigQueryOperator to correctly interpret the data types.
# Auto-detection can work but explicit schema is more robust.
raw_sales_schema = [
    {"name": "InvoiceNo", "type": "STRING", "mode": "NULLABLE"},
    {"name": "StockCode", "type": "STRING", "mode": "NULLABLE"},
    {"name": "Description", "type": "STRING", "mode": "NULLABLE"},
    {"name": "Quantity", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "InvoiceDate", "type": "TIMESTAMP", "mode": "NULLABLE", "description": "Format: M/D/YYYY H:MM"},
    {"name": "UnitPrice", "type": "NUMERIC", "mode": "NULLABLE"},
    {"name": "CustomerID", "type": "STRING", "mode": "NULLABLE"},
    {"name": "Country", "type": "STRING", "mode": "NULLABLE"}
]

with DAG(
    dag_id='ecom_staging_dag',
    default_args=default_args,
    description='Loads raw e-commerce data into BigQuery staging table and performs initial cleaning.',
    schedule_interval='@daily',
    catchup=False,
    tags=['ecommerce', 'staging', 'bigquery'],
    is_paused_upon_creation=True,
) as dag:
    # Sensor to wait for the ingestion DAG to complete successfully
    wait_for_ingestion = ExternalTaskSensor(
        task_id='wait_for_ingestion',
        external_dag_id='ecom_ingestion_dag',
        external_task_id='move_raw_to_processed',
        allowed_states=['success'],
        failed_states=['failed'],
        mode='poke',
        poke_interval=5 * 60, # Poke every 5 minutes
        timeout=60 * 60, # Timeout after 1 hour
    )

    # Load raw CSV from GCS to a BigQuery raw staging table
    # This task will overwrite the raw table each run to ensure idempotency.
    load_raw_to_bq_staging = GCSToBigQueryOperator(
        task_id='load_raw_to_bq_staging',
        bucket=GCS_RAW_DATA_BUCKET,
        source_objects=[f'processed/raw_sales/{{ ds_nodash }}_sales.csv'],
        destination_project_dataset_table=f'{GCP_PROJECT_ID}.{BIGQUERY_DATASET_STAGING}.stg_raw_sales',
        schema_fields=raw_sales_schema,
        write_disposition='WRITE_TRUNCATE', # Overwrite table each run
        source_format='CSV',
        field_delimiter=',',
        skip_leading_rows=1, # Skip header row
        autodetect=False, # Use explicit schema for robustness
        google_cloud_conn_id='google_cloud_default',
    )

    # Perform initial cleaning and standardization, load into a cleaned staging table.
    # This demonstrates SQL transformation for data quality.
    clean_and_stage_data = BigQueryInsertJobOperator(
        task_id='clean_and_stage_data',
        configuration={
            "query": {
                "query": "{% include 'sql/staging/create_stg_transactions.sql' %}",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET_STAGING,
                    "tableId": "stg_transactions"
                },
                "createDisposition": "CREATE_IF_NEEDED",
                "writeDisposition": "WRITE_TRUNCATE" # Overwrite cleaned staging table each run
            }
        },
        params={
            'project_id': GCP_PROJECT_ID,
            'staging_dataset': BIGQUERY_DATASET_STAGING
        },
        gcp_conn_id='google_cloud_default',
        # Reference the SQL file from the DAGs folder
        template_searchpath=[f'/home/airflow/gcs/dags/{GCS_DAGS_FOLDER}'], # Adjust path for Cloud Composer
    )

    # Define task dependencies
    wait_for_ingestion >> load_raw_to_bq_staging >> clean_and_stage_data