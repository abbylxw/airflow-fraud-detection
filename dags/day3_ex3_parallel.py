from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random
import time

default_args = {
    'owner': 'Xiaowan(Abby) Liu',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

def fetch_api_1(**context):
    """Simulate API call to payment processor"""
    time.sleep(2)  # Simulate API latency
    data = {'source': 'API_1', 'transactions': random.randint(100, 500)}
    print(f"✅ API_1 fetched: {data['transactions']} transactions")
    return data

def fetch_api_2(**context):
    """Simulate API call to fraud detection system"""
    time.sleep(3)  # Simulate API latency
    data = {'source': 'API_2', 'flagged_transactions': random.randint(5, 50)}
    print(f"✅ API_2 fetched: {data['flagged_transactions']} flagged transactions")
    return data

def fetch_api_3(**context):
    """Simulate API call to customer database"""
    time.sleep(2)  # Simulate API latency
    data = {'source': 'API_3', 'active_users': random.randint(1000, 5000)}
    print(f"✅ API_3 fetched: {data['active_users']} active users")
    return data

def combine_results(**context):
    """Combine all API results into a summary"""
    api1_data = context['ti'].xcom_pull(task_ids='fetch_api_1')
    api2_data = context['ti'].xcom_pull(task_ids='fetch_api_2')
    api3_data = context['ti'].xcom_pull(task_ids='fetch_api_3')
    
    summary = {
        'total_transactions': api1_data['transactions'],
        'fraud_rate': (api2_data['flagged_transactions'] / api1_data['transactions']) * 100,
        'active_users': api3_data['active_users'],
        'transactions_per_user': api1_data['transactions'] / api3_data['active_users']
    }
    
    print("📊 Combined Summary:")
    print(f"   Total Transactions: {summary['total_transactions']}")
    print(f"   Fraud Rate: {summary['fraud_rate']:.2f}%")
    print(f"   Active Users: {summary['active_users']}")
    print(f"   Transactions per User: {summary['transactions_per_user']:.2f}")
    
    return summary

with DAG(
    dag_id='day3_ex3_parallel_processing',
    default_args=default_args,
    description='Parallel API fetching',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:
    
    api1 = PythonOperator(
        task_id='fetch_api_1',
        python_callable=fetch_api_1
    )
    
    api2 = PythonOperator(
        task_id='fetch_api_2',
        python_callable=fetch_api_2
    )
    
    api3 = PythonOperator(
        task_id='fetch_api_3',
        python_callable=fetch_api_3
    )
    
    combine = PythonOperator(
        task_id='combine_results',
        python_callable=combine_results
    )
    
    # All APIs run in PARALLEL, then combine
    [api1, api2, api3] >> combine