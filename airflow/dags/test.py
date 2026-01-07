from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import storage, bigquery
from datetime import datetime, timezone, timedelta
import pandas as pd
import json

PROJECT_ID = "system-monitoring-pipeline"
BUCKET_NAME = "machine-metrics-raw"
MAIN_DATASET = "machine_metrics"
RAW_TABLE = "machine_metrics_raw" 
AGGREGATED_TABLE = "machine_metrics_aggregated_hourly"

default_args = {
    "owner": "airflow",
}

def load_raw_metrics_to_bq():
    storage_hook = GCSHook(gcp_conn_id="google_cloud_default")
    bq_hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql = False)

    now = datetime.now(timezone.utc)
    year = now.strftime("%Y")
    month = now.strftime("%b").upper()
    day = now.strftime("%d")

    blobs_names = storage_hook.list(BUCKET_NAME, prefix= f"raw-metrics/{year}/{month}/{day}") 
    rows = []

    for blob_name in blobs_names:
        if blob_name.endswith(".json"):
            blob_data = json.loads(storage_hook.download(bucket_name = BUCKET_NAME, object_name = blob_name))
            rows.extend(blob_data)
    
    if not rows:
        print("No files found for today ❌")
        return 
    
    
    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)

    table_id = f"{PROJECT_ID}.{MAIN_DATASET}.{RAW_TABLE}"

    bq_client = bq_hook.get_client()
    bq_client.load_table_from_dataframe(df, table_id, job_config= bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"))
    print(f"{len(df)} Records has been loaded successfully ✅")



def aggregate_hourly_metrics():

    bq_hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql = False)
    bq_client = bq_hook.get_client()

    query = f"""
            INSERT INTO `{PROJECT_ID}.{MAIN_DATASET}.{AGGREGATED_TABLE}` 
            SELECT TIMESTAMP_TRUNC(timestamp, HOUR) AS hour,
                AVG(cpu_percent) AS avg_cpu_percent,
                MAX(ram_percent) AS max_ram_percent,
                AVG(cpu_temp_celsius) AS avg_cpu_temp,
                AVG(gpu_temp_celsius) AS avg_gpu_temp,
                SUM(net_bytes_sent) AS total_net_sent,
                SUM(net_bytes_recv) AS total_net_recv,
                COUNT(*) AS record_count
            FROM `{PROJECT_ID}.{MAIN_DATASET}.{RAW_TABLE}` raw
            LEFT JOIN `{PROJECT_ID}.{MAIN_DATASET}.{AGGREGATED_TABLE}` agg
                ON TIMESTAMP_TRUNC(raw.timestamp, HOUR) = agg.hour
            WHERE agg.hour IS NULL
            GROUP BY hour
        """
    

    job = bq_client.query(query, location="us-central1")
    job.result() 
    print("Hourly aggregation completed ✅")


with DAG(
    dag_id = "machine_metrics_pipeline",
    start_date = datetime(2026,1,1 , tzinfo=timezone.utc),
    catchup = False,
    schedule="@hourly",
    default_args = default_args
) as dag:

    load_raw_metrics_to_bq_task = PythonOperator(python_callable = load_raw_metrics_to_bq, task_id = "load_raw_to_bq")
    load_aggregated_hourly_metrics_task = PythonOperator(python_callable = aggregate_hourly_metrics, task_id = "load_aggregated_metrics")


    load_raw_metrics_to_bq_task >> load_aggregated_hourly_metrics_task 
