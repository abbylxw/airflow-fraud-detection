import pendulum
from airflow.sdk import dag, task

@dag(
    'day7_reporting_dag',
    start_date=pendulum.datetime(2025, 11, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=['day7', 'reporting']
)
def reporting_dag():
    
    @task
    def generate_report():
        print("📊 Generating fraud report...")
        print("✅ Report complete!")
    
    generate_report()

reporting_dag()