from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def extract_data(**context):
    print("📥 Extracting transaction data...")
    return {'extracted': 100}

def validate_data(**context):
    print("✅ Validating data quality...")
    return {'valid': True}

def fraud_check_1(**context):
    print("🔍 Running fraud check 1...")
    return {'suspicious': 5}

def fraud_check_2(**context):
    print("🔍 Running fraud check 2...")
    return {'suspicious': 3}

def risk_scoring(**context):
    print("📊 Calculating risk scores...")
    return {'high_risk': 8}

def consolidate_results(**context):
    ti = context['ti']
    fc1 = ti.xcom_pull(task_ids='fraud_check_1')
    fc2 = ti.xcom_pull(task_ids='fraud_check_2')
    risk = ti.xcom_pull(task_ids='risk_scoring')
    print(f"📋 Consolidated: {fc1}, {fc2}, {risk}")
    return {'total_flagged': fc1['suspicious'] + fc2['suspicious']}

def generate_report(**context):
    results = context['ti'].xcom_pull(task_ids='consolidate_results')
    print(f"📄 Report: {results['total_flagged']} transactions flagged")

with DAG(
    'day4_ex1_complex_deps',
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    description='Exercise: Complex dependency patterns',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['day4', 'exercise', 'dependencies'],
) as dag:
    
    extract = PythonOperator(task_id='extract_data', python_callable=extract_data)
    validate = PythonOperator(task_id='validate_data', python_callable=validate_data)
    fraud1 = PythonOperator(task_id='fraud_check_1', python_callable=fraud_check_1)
    fraud2 = PythonOperator(task_id='fraud_check_2', python_callable=fraud_check_2)
    risk = PythonOperator(task_id='risk_scoring', python_callable=risk_scoring)
    consolidate = PythonOperator(task_id='consolidate_results', python_callable=consolidate_results)
    report = PythonOperator(task_id='generate_report', python_callable=generate_report)
    
    # Define the dependencies following this pattern:
    # 1. extract → validate
    # 2. validate → [fraud1, fraud2, risk] (fan-out)
    # 3. [fraud1, fraud2] → consolidate (partial fan-in)
    # 4. risk → consolidate (also feeds consolidate)
    # 5. consolidate → report
    
    extract >> validate
    validate >> [fraud1, fraud2, risk] 
    [fraud1, fraud2] >> consolidate
    risk >> consolidate
    consolidate >> report