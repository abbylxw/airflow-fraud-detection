from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random

default_args = {
    'owner': 'Xiaowan(Abby) Liu',
    'retries': 1,  # Retry 1 times
    'retry_delay': timedelta(seconds=10)  # Wait 10 seconds between retries
}

def unreliable_api_call(**context):
    """Simulates an API that fails 60% of the time"""
    success_rate = 0.4  # 40% success rate
    
    attempt = context['ti'].try_number  # Which retry attempt is this?
    print(f"🔄 Attempt #{attempt}: Calling unreliable API...")
    
    if random.random() < success_rate:
        print(f"✅ Success on attempt #{attempt}!")
        return {'status': 'success', 'data': 'Important fraud data', 'attempt': attempt}
    else:
        print(f"❌ Failed on attempt #{attempt}")
        raise Exception(f"API call failed (attempt {attempt}/3)")

def process_data(**context):
    """Only runs if API call succeeded"""
    result = context['ti'].xcom_pull(task_ids='api_call')
    print(f"📊 Processing data from API (succeeded on attempt {result['attempt']})")
    print(f"   Data: {result['data']}")
    return "Processing complete"

def send_alert(**context):
    """Runs only if API call ultimately failed after all retries"""
    print("🚨 ALERT: API call failed after all retries!")
    print("   Action: Notify engineering team")
    return "Alert sent"

with DAG(
    dag_id='day3_ex4_error_handling',
    default_args=default_args,
    description='Handling failures and retries',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:
    
    api_call = PythonOperator(
        task_id='api_call',
        python_callable=unreliable_api_call
    )
    
    process = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
        trigger_rule='all_success'  # Only run if api_call succeeded
    )
    
    alert = PythonOperator(
        task_id='send_alert',
        python_callable=send_alert,
        trigger_rule='all_failed'  # Only run if api_call failed all retries
    )
    
    api_call >> [process, alert]
