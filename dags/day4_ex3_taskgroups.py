from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

def dummy_task(message, **context):
    print(f"✅ {message}")

with DAG(
    'day4_ex3_taskgroups',
    default_args={'owner': 'Xiaowan(Abby) Liu', 'retries': 1},
    description='Exercise: Organize fraud pipeline with TaskGroups',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['day4', 'exercise', 'taskgroups'],
) as dag:
    # Create 3 TaskGroups:
    # 1. 'data_ingestion' with tasks: ingest_transactions, ingest_user_profiles
    # 2. 'fraud_checks' with tasks: check_amount, check_location, check_device
    # 3. 'action' with tasks: block_transaction, send_alert
    with TaskGroup('data_ingestion') as data_collection:
        ingest_transaction = PythonOperator(
            task_id = 'ingest_transactions',
            python_callable = dummy_task,
            op_kwargs = {'message': 'Data collection: Ingesting transactions'}
        )
        ingest_user_profile = PythonOperator(
            task_id = 'ingest_user_profiles',
            python_callable = dummy_task,
            op_kwargs = {'message': 'Data collection: Ingesting user profiles'}
        )
        [ingest_transaction, ingest_user_profile]
    
    with TaskGroup('fraud_checks') as fraud_analysis:
        check_amount = PythonOperator(
            task_id = 'check_amount',
            python_callable = dummy_task,
            op_kwargs = {'message': 'Fraud check: Checking amount'}
        )
        check_location = PythonOperator(
            task_id = 'check_location',
            python_callable = dummy_task,
            op_kwargs = {'message': 'Fraud check: Checking location'}
        )
        check_device = PythonOperator(
            task_id = 'check_device',
            python_callable = dummy_task,
            op_kwargs = {'message': 'Fraud check: Checking device'}
        )
        [check_amount, check_location, check_device]

    with TaskGroup('action') as alerting:
        block_transaction = PythonOperator(
            task_id = 'block_transaction',
            python_callable = dummy_task,
            op_kwargs = {'message': 'Action: Block transaction'}
        )
        send_alert = PythonOperator(
            task_id = 'send_alert',
            python_callable = dummy_task,
            op_kwargs = {'message': 'Action: Send alert'}
        )

        [block_transaction, send_alert]

    # Set dependencies: 
    data_collection >> fraud_analysis >> alerting