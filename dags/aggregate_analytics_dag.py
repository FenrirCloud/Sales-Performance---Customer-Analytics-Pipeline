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
BIGQUERY_DATASET_DWH = gcp_config['bigquery_dataset_dwh']
BIGQUERY_DATASET_ANALYTICS = gcp_config['bigquery_dataset_analytics']
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
    dag_id='ecom_analytics_dag',
    default_args=default_args,
    description='Creates aggregated tables for optimized reporting and advanced analytics.',
    schedule_interval='@daily',
    catchup=False,
    tags=['ecommerce', 'analytics', 'bigquery', 'aggregations'],
    is_paused_upon_creation=True,
) as dag:
    # Sensor to wait for the DWH DAG to complete successfully
    wait_for_dwh = ExternalTaskSensor(
        task_id='wait_for_dwh',
        external_dag_id='ecom_dwh_dag',
        external_task_id='create_fact_sales',
        allowed_states=['success'],
        failed_states=['failed'],
        mode='poke',
        poke_interval=5 * 60,
        timeout=60 * 60,
    )

    # Create/Update Daily Sales Aggregation
    create_agg_daily_sales = BigQueryInsertJobOperator(
        task_id='create_agg_daily_sales',
        configuration={
            "query": {
                "query": "{% include 'sql/aggregations/agg_daily_sales.sql' %}",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET_ANALYTICS,
                    "tableId": "agg_daily_sales"
                },
                "createDisposition": "CREATE_IF_NEEDED",
                "writeDisposition": "WRITE_TRUNCATE" # Overwrite daily or use MERGE for incremental
            }
        },
        params={
            'project_id': GCP_PROJECT_ID,
            'dwh_dataset': BIGQUERY_DATASET_DWH,
            'analytics_dataset': BIGQUERY_DATASET_ANALYTICS,
            'execution_date': '{{ ds }}'
        },
        gcp_conn_id='google_cloud_default',
        template_searchpath=[f'/home/airflow/gcs/dags/{GCS_DAGS_FOLDER}'],
    )

    # Create/Update Customer Segments Monthly Aggregation
    create_agg_customer_segments_monthly = BigQueryInsertJobOperator(
        task_id='create_agg_customer_segments_monthly',
        configuration={
            "query": {
                "query": "{% include 'sql/aggregations/agg_customer_segments_monthly.sql' %}",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET_ANALYTICS,
                    "tableId": "agg_customer_segments_monthly"
                },
                "createDisposition": "CREATE_IF_NEEDED",
                "writeDisposition": "WRITE_TRUNCATE" # Overwrite daily or use MERGE for incremental
            }
        },
        params={
            'project_id': GCP_PROJECT_ID,
            'dwh_dataset': BIGQUERY_DATASET_DWH,
            'analytics_dataset': BIGQUERY_DATASET_ANALYTICS,
            'execution_date': '{{ ds }}'
        },
        gcp_conn_id='google_cloud_default',
        template_searchpath=[f'/home/airflow/gcs/dags/{GCS_DAGS_FOLDER}'],
    )

    # Create/Update Top N Products Monthly Aggregation
    create_top_n_products_monthly = BigQueryInsertJobOperator(
        task_id='create_top_n_products_monthly',
        configuration={
            "query": {
                "query": "{% include 'sql/aggregations/top_n_products_monthly.sql' %}",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET_ANALYTICS,
                    "tableId": "top_n_products_monthly"
                },
                "createDisposition": "CREATE_IF_NEEDED",
                "writeDisposition": "WRITE_TRUNCATE" # Overwrite daily or use MERGE for incremental
            }
        },
        params={
            'project_id': GCP_PROJECT_ID,
            'dwh_dataset': BIGQUERY_DATASET_DWH,
            'analytics_dataset': BIGQUERY_DATASET_ANALYTICS,
            'execution_date': '{{ ds }}'
        },
        gcp_conn_id='google_cloud_default',
        template_searchpath=[f'/home/airflow/gcs/dags/{GCS_DAGS_FOLDER}'],
    )

    # Define task dependencies
    wait_for_dwh >> [
        create_agg_daily_sales,
        create_agg_customer_segments_monthly,
        create_top_n_products_monthly
    ]