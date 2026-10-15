from datetime import datetime, timedelta
from airflow.sdk import dag, task

@dag(
    dag_id="incident_intelligence_first_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["incident-intelligence", "learning"],
)
def incident_intelligence_first_dag():

    @task
    def start_pipeline():
        print("Incident Intelligence pipeline started!")

    @task(retries=2, retry_delay=timedelta(seconds=10))
    def validate_input():
        # print("Input validation completed!")
        print("Checking input data...")
        raise ValueError("Simulated validation failure!")

    @task
    def finish_pipeline():
        print("Incident Intelligence pipeline completed!")

    start = start_pipeline()
    validate = validate_input()
    finish = finish_pipeline()

    start >> validate >> finish


incident_intelligence_first_dag()
