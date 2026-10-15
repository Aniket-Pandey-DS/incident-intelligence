from datetime import datetime, timedelta
from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator

PROJECT_DIR = "/opt/incident-intelligence"

@dag(
    dag_id="incident_intelligence_pipeline",
    description="Orchestrate the Incident Intelligence Bronze-Silver-Gold pipeline",
    start_date=datetime(2026, 10, 15),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["incident-intelligence", "pyspark", "medallion"],
)

def incident_intelligence_pipeline():

    bronze_ingestion = BashOperator(
        task_id="bronze_ingestion",
        bash_command="python -m pipeline.ingest_incidents",
        cwd=PROJECT_DIR,
        retries=0,
    )

    silver_transformation = BashOperator(
        task_id="silver_transformation",
        bash_command="python -m pipeline.build_silver",
        cwd=PROJECT_DIR,
        retries=0,
    )

    gold_incident_summary = BashOperator(
        task_id="gold_incident_summary",
        bash_command="python -m pipeline.build_gold",
        cwd=PROJECT_DIR,
        retries=0,
    )

    gold_resolution_performance = BashOperator(
        task_id="gold_resolution_performance",
        bash_command="python -m pipeline.build_resolution_performance",
        cwd=PROJECT_DIR,
        retries=0,
    )

    bronze_ingestion >> silver_transformation >> [gold_incident_summary, gold_resolution_performance]

incident_intelligence_pipeline()