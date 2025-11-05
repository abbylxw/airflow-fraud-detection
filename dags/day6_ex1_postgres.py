from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random

def generate_fraud_transactions(**context):
    """Generate sample fraud transaction data"""
    transactions = []
    for i in range(10):
        transaction = {
            'transaction_id': f'TXN{1000 + i}',
            'user_id': f'USER{random.randint(100, 999)}',
            'amount': random.choice([500, 2500, 5000, 8500, 15000, 50000]),
            'merchant_id': f'MERCH{random.randint(1, 50)}',
            'risk_score': round(random.uniform(0.1, 0.99), 2),
            'status': random.choice(['approved', 'flagged', 'blocked'])
        }
        transactions.append(transaction)
    
    print(f"Generated {len(transactions)} transactions")
    return transactions

def insert_transactions(**context):
    """Insert transactions into database"""
    ti = context['ti']
    transactions = ti.xcom_pull(task_ids='generate_transactions')
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    for txn in transactions:
        sql = """
            INSERT INTO fraud_detection.transactions 
            (transaction_id, user_id, amount, merchant_id, risk_score, status)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        hook.run(sql, parameters=(
            txn['transaction_id'],
            txn['user_id'],
            txn['amount'],
            txn['merchant_id'],
            txn['risk_score'],
            txn['status']
        ))
    
    print(f"✅ Inserted {len(transactions)} transactions into database")

def analyze_fraud_patterns(**context):
    """Analyze fraud patterns from database"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Query 1: High-risk transactions
    print("\n🚨 HIGH-RISK TRANSACTIONS (score > 0.8):")
    high_risk = hook.get_records("""
        SELECT transaction_id, amount, risk_score, status
        FROM fraud_detection.transactions
        WHERE risk_score > 0.8
        ORDER BY risk_score DESC
    """)
    for row in high_risk:
        print(f"  {row[0]}: ${row[1]:,.2f} (Risk: {row[2]}, Status: {row[3]})")
    
    # Query 2: Status breakdown
    print("\n📊 TRANSACTION STATUS BREAKDOWN:")
    status_counts = hook.get_records("""
        SELECT status, COUNT(*), AVG(risk_score)::NUMERIC(10,2)
        FROM fraud_detection.transactions
        GROUP BY status
    """)
    for row in status_counts:
        print(f"  {row[0]}: {row[1]} transactions (Avg risk: {row[2]})")
    
    # Query 3: High-value transactions
    print("\n💰 HIGH-VALUE TRANSACTIONS (> $5000):")
    high_value = hook.get_records("""
        SELECT transaction_id, amount, user_id, risk_score
        FROM fraud_detection.transactions
        WHERE amount > 5000
        ORDER BY amount DESC
    """)
    for row in high_value:
        print(f"  {row[0]}: ${row[1]:,.2f} by {row[2]} (Risk: {row[3]})")

with DAG(
    'day6_exercise1_postgres',
    default_args={
        'owner': 'Xiaowan(Abby) Liu',
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    description='Exercise: PostgreSQL fraud database operations',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['day6', 'exercise', 'postgres'],
) as dag:
    
    # Task 1: Create table
    create_table = SQLExecuteQueryOperator(
        task_id='create_transactions_table',
        conn_id='postgres_default',
        sql="""
            CREATE SCHEMA IF NOT EXISTS fraud_detection;
            
            DROP TABLE IF EXISTS fraud_detection.transactions;
            
            CREATE TABLE fraud_detection.transactions (
                id SERIAL PRIMARY KEY,
                transaction_id VARCHAR(50) UNIQUE NOT NULL,
                user_id VARCHAR(50) NOT NULL,
                amount DECIMAL(10, 2) NOT NULL,
                merchant_id VARCHAR(50),
                risk_score DECIMAL(3, 2),
                status VARCHAR(20),
                created_at TIMESTAMP DEFAULT NOW()
            );
            
            CREATE INDEX idx_risk_score ON fraud_detection.transactions(risk_score);
            CREATE INDEX idx_transaction_id ON fraud_detection.transactions(transaction_id);
        """
    )
    
    # Task 2: Generate sample data
    generate = PythonOperator(
        task_id='generate_transactions',
        python_callable=generate_fraud_transactions
    )
    
    # Task 3: Insert data
    insert = PythonOperator(
        task_id='insert_transactions',
        python_callable=insert_transactions
    )
    
    # Task 4: Analyze patterns
    analyze = PythonOperator(
        task_id='analyze_fraud_patterns',
        python_callable=analyze_fraud_patterns
    )
    
    create_table >> generate >> insert >> analyze
