from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from datetime import datetime, timedelta
import random

def fetch_transaction(**context):
    """Simulate fetching a transaction"""
    transaction = {
        'id': f'TXN{random.randint(1000, 9999)}',
        'amount': random.choice([500, 3000, 8000, 15000, 50000]),
        'user_id': f'USER{random.randint(100, 999)}'
    }
    print(f"📥 Fetched transaction: {transaction}")
    return transaction

def route_by_amount(**context):
    """Branch based on transaction amount"""
    txn = context['ti'].xcom_pull(task_ids='fetch_transaction')
    amount = txn['amount']
    
    print(f"💰 Transaction amount: ${amount}")
    
    # YOUR TASK: Implement routing logic
    if amount < 1000:
        print("✅ Amount < $1,000 → Auto approve")
        return 'auto_approve'
    elif amount < 10000:
        print("⚠️ Amount $1,000-$10,000 → Manual review")
        return 'manual_review'
    else:
        print("🚨 Amount >= $10,000 → Enhanced verification")
        return 'enhanced_verification'
    # returned values consumed by Airflow scheduler, not by downstream tasks
    # Airflow scheduler receives the returned string, and looks up the task with task_id = returned string
    # Scheduler marks that task to run and others as "skipped"
    # can return a list ['task_a','task_b'] as well.

def auto_approve(**context):
    txn = context['ti'].xcom_pull(task_ids='fetch_transaction')
    print(f"✅ AUTO APPROVED: Transaction {txn['id']} for ${txn['amount']}")

def manual_review(**context):
    txn = context['ti'].xcom_pull(task_ids='fetch_transaction')
    print(f"⚠️ MANUAL REVIEW: Transaction {txn['id']} for ${txn['amount']} flagged for review")

def enhanced_verification(**context):
    txn = context['ti'].xcom_pull(task_ids='fetch_transaction')
    print(f"🚨 ENHANCED VERIFICATION: High-value transaction {txn['id']} for ${txn['amount']}")
    print(f"   Running additional checks...")
    print(f"   Requiring manager approval...")

with DAG(
    'day5_exercise2_branching',
    default_args={'owner': 'Xiaowan(Abby) Liu', 'retries': 1},
    description='Exercise: Branch by transaction amount',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['day5', 'exercise', 'branching'],
) as dag:
    
    fetch = PythonOperator(
        task_id='fetch_transaction',
        python_callable=fetch_transaction
    )
    
    route = BranchPythonOperator(
        task_id='route_by_amount',
        python_callable=route_by_amount
    )
    
    approve = PythonOperator(
        task_id='auto_approve',
        python_callable=auto_approve
    )
    
    review = PythonOperator(
        task_id='manual_review',
        python_callable=manual_review
    )
    
    verify = PythonOperator(
        task_id='enhanced_verification',
        python_callable=enhanced_verification
    )
    
    fetch >> route >> [approve, review, verify]


