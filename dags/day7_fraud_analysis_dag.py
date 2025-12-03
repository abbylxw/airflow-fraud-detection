import pendulum
from datetime import timedelta
from airflow.sdk import dag, task
#from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.session import provide_session
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


@dag(
    'fraud_analysis_post_ingestion',
    start_date=pendulum.datetime(2025, 11, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=['day7', 'External file sensor']
)
def fraud_analysis_post_ingestion():

    # wait_for_ingestion = ExternalTaskSensor(
    #     task_id='wait_for_data_ingestion',
    #     external_dag_id='data_ingestion_dag',      # DAG to watch
    #     external_task_id='ingestion_complete',     # Task to watch (None = whole DAG)
    #     poke_interval=30,
    #     execution_delta=timedelta(minutes=5), 
    #     timeout=600,
    #     mode='reschedule',
    # )

    trigger_ingestion = TriggerDagRunOperator(
        task_id='trigger_ingestion',
        trigger_dag_id='data_ingestion_dag',
        wait_for_completion=True,   # Waits for it to finish!
    )

    @task
    def process_after_upstream():
        print("Now safe to run because upstream task completed.")

    trigger_ingestion >> process_after_upstream()

fraud_analysis_post_ingestion()

