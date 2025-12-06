import pendulum
import json
import os
from airflow.sdk import dag, task

DATA_DIR = "/opt/airflow/dags/data"

@dag(
    'day8_file_passing',
    start_date=pendulum.datetime(2025, 11, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=['day8', 'xcom', 'file-passing']
)
def file_passing_pipeline():
    
    @task
    def generate_transactions():
        """Generate data and save to file, return file path."""
        import random
        
        txns = [
            {"id": i, "amount": random.randint(100, 10000), "region": random.choice(["us", "eu", "apac"])}
            for i in range(1000)  # 1000 transactions
        ]
        
        # Write to file
        filepath = f"{DATA_DIR}/transactions_batch.json"
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(filepath, 'w') as f:
            json.dump(txns, f)
        
        print(f"✅ Wrote {len(txns)} transactions to {filepath}")
        return filepath  # Pass path, not data.
    
    @task
    def analyze_fraud(filepath: str):
        """Read from file path, analyze."""
        # Read the file and count high-risk transactions (amount > 5000)
        with open(filepath, 'r') as f:
            data = json.load(f)
        high_risk_txns = [txn for txn in data if txn['amount'] > 5000]
        return len(high_risk_txns)
    
    @task
    def generate_report(filepath: str, high_risk_count: int):
        """Generate summary report."""
        # Print summary
        with open(filepath, 'r') as f:
            txns = json.load(f)
        total_cnt = len(txns)
        high_risk_pct = high_risk_count*100.0/total_cnt if total_cnt > 0 else 0
        print(f"📊 FRAUD ANALYSIS REPORT")
        print(f"From {total_cnt} transactions, in total {high_risk_count} high risk transactions with amount > 5000.")
        print(f"{high_risk_pct:.1f}% risky transactions.")
        
    
    # Wire it up
    filepath = generate_transactions()
    high_risk = analyze_fraud(filepath)
    generate_report(filepath, high_risk)

file_passing_pipeline()