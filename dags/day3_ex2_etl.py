from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random

default_args = {
    'owner': 'you',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

def extract_data(**context):
    """Extract: Simulate pulling data from an API"""
    # Simulating raw sales data with some messy values
    raw_data = [
        {'product': 'Laptop', 'amount': 1200, 'quantity': 2},
        {'product': 'Mouse', 'amount': None, 'quantity': 5},  # Missing data
        {'product': 'Keyboard', 'amount': 80, 'quantity': 3},
        {'product': '', 'amount': 50, 'quantity': 1},  # Missing product name
    ]
    print(f"📥 Extracted {len(raw_data)} records")
    return raw_data

def transform_data(**context):
    """Transform: Clean and enrich the data"""
    raw_data = context['ti'].xcom_pull(task_ids='extract')
    
    cleaned_data = []
    for record in raw_data:
        # Skip records with missing critical fields
        if not record.get('product') or record.get('amount') is None:
            print(f"⚠️ Skipping invalid record: {record}")
            continue
        
        # Calculate total revenue
        record['total_revenue'] = record['amount'] * record['quantity']
        cleaned_data.append(record)
    
    print(f"✨ Transformed {len(cleaned_data)} valid records")
    return cleaned_data

def load_data(**context):
    """Load: Save to database (simulated)"""
    clean_data = context['ti'].xcom_pull(task_ids='transform')
    
    total_revenue = sum(record['total_revenue'] for record in clean_data)
    
    print(f"💾 Loading {len(clean_data)} records to database...")
    for record in clean_data:
        print(f"   INSERT: {record}")
    
    print(f"✅ Total Revenue: ${total_revenue}")
    return f"Loaded {len(clean_data)} records, Total Revenue: ${total_revenue}"

with DAG(
    dag_id='day3_ex2_etl_pipeline',
    default_args=default_args,
    description='ETL pattern with data cleaning',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:
    
    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_data
    )
    
    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_data
    )
    
    load = PythonOperator(
        task_id='load',
        python_callable=load_data
    )
    
    # ETL flow
    extract >> transform >> load