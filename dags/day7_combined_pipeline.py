import pendulum
from airflow.sdk import dag, task
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

FLAG_FILE = "/opt/airflow/dags/data/batch_ready.flag"

@dag(
    'day7_combined_fraud_pipeline',
    start_date=pendulum.datetime(2025, 11, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=['day7', 'combined', 'fraud-detection']
)
def combined_fraud_pipeline():
    
    # 1. FileSensor - wait for batch ready flag
    wait_for_file = FileSensor(
        task_id='wait_for_flag',
        filepath=FLAG_FILE,
        fs_conn_id='fs_default',
        poke_interval=30,
        timeout=300,
        mode='reschedule',  # Releases worker between checks
        soft_fail=True,     # Don't fail DAG, just skip downstream
    )
    
    # 2. Task to return list of regions
    @task
    def get_regions():
        """Return list of regions to process in parallel."""
        regions = ['us', 'eu', 'apac','latam']
        print(f"📍 Processing regions: {regions}")
        return regions
    
    # 3. Dynamic task to process each region (use .expand())
    @task
    def process_region(region: str):
        print(f"Processing {region}...")
        # fraud detection logic here
        return {"region": region, "flagged": 5}
    
    # 4. Aggregate results
    @task
    def aggregate_summary(results):
        print("\n" + "="*70)
        print("GLOBAL FRAUD DETECTION SUMMARY")
        print("="*70)     
        print(f"\n🌍 REGIONAL BREAKDOWN")
        print("-"*70)
    
    # 5. Trigger downstream DAG
    trigger_ingestion = TriggerDagRunOperator(
        task_id='trigger_ingestion',
        trigger_dag_id='day7_reporting_dag',
        wait_for_completion=True,   # Waits for it to finish!
    )

    regions = get_regions()
    region_results = process_region.expand(
                    region=regions  
                    )
    agg = aggregate_summary(region_results)
    wait_for_file >> regions >> region_results >> agg >> trigger_ingestion

combined_fraud_pipeline()