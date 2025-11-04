from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import csv

def process_fraud_file(**context):
    """Process fraud transactions from CSV file"""
    filepath = '/opt/airflow/dags/fraud_input/transactions.csv'
    
    print(f"📄 Processing file: {filepath}")
    
    # Simulate reading and analyzing
    high_risk_count = 0
    total_transactions = 0
    
    try:
        with open(filepath, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                total_transactions += 1
                amount = float(row.get('amount', 0))
                if amount > 5000:
                    high_risk_count += 1
                    print(f"🚨 High risk transaction: {row}")
    except Exception as e:
        print(f"Error reading file: {e}")
        raise
    
    print(f"📊 Processed {total_transactions} transactions")
    print(f"🚨 Found {high_risk_count} high-risk transactions")
    
    return {
        'total': total_transactions,
        'high_risk': high_risk_count
    }

def send_report(**context):
    """Send fraud report"""
    results = context['ti'].xcom_pull(task_ids='process_fraud_file')
    print(f"📧 Sending fraud report:")
    print(f"   Total transactions: {results['total']}")
    print(f"   High-risk flagged: {results['high_risk']}")

with DAG(
    'day5_ex1_file_sensor',
    default_args={
        'owner': 'Xiaowan(Abby) Liu',
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    description='Exercise: Wait for fraud data file and process',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['day5', 'exercise', 'sensors'],
) as dag:
    
    # Create directory
    create_dir = BashOperator(
        task_id='create_directory',
        bash_command='mkdir -p /opt/airflow/dags/fraud_input'
    )
    
    # Task 1: Wait for file to arrive
    wait_for_file = FileSensor(
        task_id='wait_for_transactions_file',
        filepath='/opt/airflow/dags/fraud_input/transactions.csv',
        fs_conn_id='fs_default',
        poke_interval=5,  # Check every 5 seconds
        timeout=120,      # Wait max 2 minutes
        mode='poke'
    )
    
    # Task 2: Process the file
    process = PythonOperator(
        task_id='process_fraud_file',
        python_callable=process_fraud_file
    )
    
    # Task 3: Send report
    report = PythonOperator(
        task_id='send_report',
        python_callable=send_report
    )
    
    # Task 4: Cleanup
    cleanup = BashOperator(
        task_id='cleanup_file',
        bash_command='rm -f /opt/airflow/dags/fraud_input/transactions.csv'
    )
    
    create_dir >> wait_for_file >> process >> report >> cleanup