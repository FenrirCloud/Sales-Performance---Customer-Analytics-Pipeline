import os
import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.sensors.external_task import ExternalTaskSensor

# Load configuration from JSON file
current_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(current_dir, 'config', 'gcp_config.json')
with open(config_path, 'r') as f:
    gcp_config = json.load(f)

GCP_PROJECT_ID = gcp_config['gcp_project_id']
BIGQUERY_DATASET_STAGING = gcp_config['bigquery_dataset_staging']
BIGQUERY_DATASET_DWH = gcp_config['bigquery_dataset_dwh']
GCS_DAGS_FOLDER = gcp_config['gcs_dags_folder']

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': GCP_PROJECT_ID,
    'schedule_interval': None,
}

with DAG(
    dag_id='ecom_dwh_dag',
    default_args=default_args,
    description='Transforms staged data into a dimensional data warehouse model (Star Schema).',
    schedule_interval='@daily',
    catchup=False,
    tags=['ecommerce', 'dwh', 'bigquery', 'datamodeling'],
    is_paused_upon_creation=True,
) as dag:
    # Sensor to wait for the staging DAG to complete successfully
    wait_for_staging = ExternalTaskSensor(
        task_id='wait_for_staging',
        external_dag_id='ecom_staging_dag',
        external_task_id='clean_and_stage_data',
        allowed_states=['success'],
        failed_states=['failed'],
        mode='poke',
        poke_interval=5 * 60,
        timeout=60 * 60,
    )

    # Create/Update Date Dimension
    create_dim_dates = BigQueryInsertJobOperator(
        task_id='create_dim_dates',
        configuration={
            "query": {
                "query": "{% include 'sql/dimensions/dim_dates.sql' %}",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET_DWH,
                    "tableId": "dim_dates"
                },
                "createDisposition": "CREATE_IF_NEEDED",
                "writeDisposition": "WRITE_TRUNCATE" # Overwrite daily, or use MERGE for incremental
            }
        },
        params={
            'project_id': GCP_PROJECT_ID,
            'dwh_dataset': BIGQUERY_DATASET_DWH
        },
        gcp_conn_id='google_cloud_default',
        template_searchpath=[f'/home/airflow/gcs/dags/{GCS_DAGS_FOLDER}'],
    )

    # Create/Update Product Dimension
    create_dim_products = BigQueryInsertJobOperator(
        task_id='create_dim_products',
        configuration={
            "query": {
                "query": "{% include 'sql/dimensions/dim_products.sql' %}",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET_DWH,
                    "tableId": "dim_products"
                },
                "createDisposition": "CREATE_IF_NEEDED",
                "writeDisposition": "WRITE_TRUNCATE" # Overwrite daily, or use MERGE for SCD Type 1
            }
        },
        params={
            'project_id': GCP_PROJECT_ID,
            'staging_dataset': BIGQUERY_DATASET_STAGING,
            'dwh_dataset': BIGQUERY_DATASET_DWH
        },
        gcp_conn_id='google_cloud_default',
        template_searchpath=[f'/home/airflow/gcs/dags/{GCS_DAGS_FOLDER}'],
    )

    # Create/Update Customer Dimension (with RFM logic)
    create_dim_customers = BigQueryInsertJobOperator(
        task_id='create_dim_customers',
        configuration={
            "query": {
                "query": "{% include 'sql/dimensions/dim_customers.sql' %}",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET_DWH,
                    "tableId": "dim_customers"
                },
                "createDisposition": "CREATE_IF_NEEDED",
                "writeDisposition": "WRITE_TRUNCATE" # Overwrite daily, or use MERGE for SCD Type 1
            }
        },
        params={
            'project_id': GCP_PROJECT_ID,
            'staging_dataset': BIGQUERY_DATASET_STAGING,
            'dwh_dataset': BIGQUERY_DATASET_DWH
        },
        gcp_conn_id='google_cloud_default',
        template_searchpath=[f'/home/airflow/gcs/dags/{GCS_DAGS_FOLDER}'],
    )

    # Create/Update Sales Fact Table
    create_fact_sales = BigQueryInsertJobOperator(
        task_id='create_fact_sales',
        configuration={
            "query": {
                "query": "{% include 'sql/facts/fact_sales.sql' %}",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET_DWH,
                    "tableId": "fact_sales"
                },
                "createDisposition": "CREATE_IF_NEEDED",
                "writeDisposition": "WRITE_APPEND" # Append new sales data daily (assuming idempotent source)
            }
        },
        params={
            'project_id': GCP_PROJECT_ID,
            'staging_dataset': BIGQUERY_DATASET_STAGING,
            'dwh_dataset': BIGQUERY_DATASET_DWH,
            'execution_date': '{{ ds }}' # Pass execution date to SQL for filtering
        },
        gcp_conn_id='google_cloud_default',
        template_searchpath=[f'/home/airflow/gcs/dags/{GCS_DAGS_FOLDER}'],
    )

    # Define task dependencies
    wait_for_staging >> [create_dim_dates, create_dim_products, create_dim_customers]
    [create_dim_dates, create_dim_products, create_dim_customers] >> create_fact_sales