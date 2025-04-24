from airflow import models
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.utils.dates import days_ago

PROJECT_ID = 'your-gcp-project-id'
REGION = 'us-central1'
GCS_SCRIPT_PATH = 'gs://your-bucket/scripts/dataflow_pubsub_to_bq.py'

with models.DAG(
    dag_id='trigger_dataflow_pubsub_to_bq',
    schedule_interval='@hourly',  # or '*/15 * * * *'
    start_date=days_ago(1),
    catchup=False,
    tags=['dataflow', 'pubsub', 'bigquery'],
) as dag:

    run_dataflow = DataflowStartFlexTemplateOperator(
        task_id='run_dataflow_job',
        project_id=PROJECT_ID,
        region=REGION,
        body={
            "launchParameter": {
                "jobName": "pubsub-to-bq-{{ ds_nodash }}",
                "containerSpecGcsPath": GCS_SCRIPT_PATH,
                "parameters": {
                    "input_topic": f"projects/{PROJECT_ID}/topics/your-topic",
                    "output_table": f"{PROJECT_ID}:your_dataset.your_table",
                    "runner": "DataflowRunner",
                    "temp_location": "gs://your-bucket/temp",
                    "staging_location": "gs://your-bucket/staging",
                    "region": REGION,
                    "project": PROJECT_ID
                },
            }
        },
    )
