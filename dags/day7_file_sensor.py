import pendulum
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.operators.python import PythonOperator

import pandas as pd


csv_path = "/opt/airflow/data/sensor_txns.csv"

def process_csv(filepath):
    data = pd.read_csv(filepath)
    print(f"Loaded {len(data)} rows")
    print(data.head())

@dag(
    'day7_file_sensor',
    start_date=pendulum.datetime(2025, 11, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=['day7', 'file_sensor']
)
def file_sensor_dag():

    wait_for_file = FileSensor(
        task_id='wait_for_transactions',
        filepath=csv_path,
        fs_conn_id='fs_default',
        poke_interval=30,
        timeout=300,
        mode='reschedule',  # Releases worker between checks
        soft_fail=True,     # Don't fail DAG, just skip downstream
    )

    process_csv_task = PythonOperator(
        task_id="process_csv",
        python_callable=process_csv,
        op_args=[csv_path],
    )

    wait_for_file >> process_csv_task

file_sensor_dag()

    