import os
import pendulum
from datetime import datetime, timedelta
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

def tag_risk_tiers(transactions):
    for t in transactions:
        if t['risk_score'] > 50:
            t['risk_tier'] = 'HIGH'
        elif t['risk_score'] > 25:
            t['risk_tier'] = 'MEDIUM'
        else:
            t['risk_tier'] = 'LOW'
    return transactions

@dag(
    dag_id = 'challenge2_fraud_detection_smarter',
    schedule = None,
    start_date = pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
)

def challenge2():
    @task
    def extract_transactions():
        import random
        transactions = []
        for i in range(20):
            transaction = {
                'transaction_id': f'TXN_{i+1:03}',
                'amount': random.randint(10, 10000),
                'merchant': random.choice(["Amazon", "Walmart", "Shell", "Starbucks", "Apple", "Target", "BestBuy"])
            }
            transactions.append(transaction)
        return transactions

    @task
    def calculate_risk_scores(transactions):
        for transaction in transactions:
            transaction['risk_score'] = (transaction['amount'] / 100) + (10 if transaction['merchant'] in ["Apple", "Amazon"] else 5)

        return transactions
    
    @task.branch
    def route_by_risk(transactions):
        import json
        from airflow.sdk import Variable

        risk_scores = [d['risk_score'] for d in transactions]
        avg_risk_score = sum(risk_scores)/len(risk_scores)
        Variable.set('current_transactions', json.dumps(transactions))
        if avg_risk_score > 50:
            return 'process_high_risk'
        elif avg_risk_score > 25:
            return 'process_medium_risk'
        else:
            return 'process_low_risk'



    ### process_high/medium/low_risk tasks do the same thing, just mimic branching  
    @task
    def process_high_risk():
        import json
        from airflow.sdk import Variable
        transactions = json.loads(Variable.get('current_transactions'))
        tagged = tag_risk_tiers(transactions)
        Variable.set('tagged_transactions', json.dumps(tagged))
        return tagged
    
    @task
    def process_medium_risk():
        import json
        from airflow.sdk import Variable
        transactions = json.loads(Variable.get('current_transactions'))
        tagged = tag_risk_tiers(transactions)
        Variable.set('tagged_transactions', json.dumps(tagged))
        return tagged
    
    @task
    def process_low_risk():
        import json
        from airflow.sdk import Variable
        transactions = json.loads(Variable.get('current_transactions'))
        tagged = tag_risk_tiers(transactions)
        Variable.set('tagged_transactions', json.dumps(tagged))
        return tagged
    

    @task(trigger_rule='one_success')
    def run_fraud_checks():
        import json
        from airflow.sdk import Variable     
        # Read tagged data from Variable (whichever branch set it)
        transactions = json.loads(Variable.get('tagged_transactions'))
        return transactions
    
    @task
    def velocity_check(transactions):
        m_cnt = {}
        for t in transactions:
            merch = t['merchant']
            m_cnt[merch] = m_cnt.get(merch, 0) + 1
        for t in transactions:
            t['velocity_flag'] = (True if m_cnt[t['merchant']] > 3 else False)
        return transactions
    
    @task
    def merchant_validation(transactions):
        blacklist = ['Shell','Starbucks']
        for t in transactions:
            t['merchant_blacklisted'] = (True if t['merchant'] in blacklist else False)
        return transactions
    

    @task(trigger_rule='all_success')
    def merge_results(velocity_data, merchant_data):
        # Step 1: Create lookup dictionary 
        merchant_lookup = {
            t['transaction_id']: t['merchant_blacklisted'] 
            for t in merchant_data
        }
        
        # Step 2: Merge using lookup 
        for t in velocity_data:
            txn_id = t['transaction_id']
            t['merchant_blacklisted'] = merchant_lookup[txn_id]
            t['final_fraud_flag'] = t['velocity_flag'] or t['merchant_blacklisted']
        
        return velocity_data
        
    
    @task
    def load_to_database(transactions):
        import csv
        hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
        
        # create tables
        tiers = ['high','medium','low']
        for tier in tiers:
            sql = f"""CREATE TABLE IF NOT EXISTS fraud_{tier}_risk (
                transaction_id VARCHAR(50) PRIMARY KEY,
                amount INTEGER,
                merchant VARCHAR(100),
                timestamp VARCHAR(50),
                risk_score FLOAT,
                risk_tier VARCHAR(10),
                velocity_flag BOOLEAN,
                merchant_blacklisted BOOLEAN,
                final_fraud_flag BOOLEAN
            );
            """
            hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
            hook.run(f"TRUNCATE TABLE fraud_{tier}_risk;")
            hook.run(sql)
            print(f"✅ Created table: fraud_{tier}_risk")

        # split trasnactions by risk levels
        high_txns = [t for t in transactions if t['risk_tier'] == 'HIGH']
        medium_txns = [t for t in transactions if t['risk_tier'] == 'MEDIUM']
        low_txns = [t for t in transactions if t['risk_tier'] == 'LOW']

        tier_data = {
            'high': high_txns,
            'medium': medium_txns,
            'low': low_txns
        }

        # load to DB
        data_dir = "/opt/airflow/dags/files"
        os.makedirs(data_dir, exist_ok=True)
    
        field_order = [
            "transaction_id", "amount", "merchant", "timestamp",
            "risk_score", "risk_tier", "velocity_flag",
            "merchant_blacklisted", "final_fraud_flag"
        ]
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                for tier, txns in tier_data.items():
                    if not txns:
                        continue # skip if empty files
                    csv_path = f"{data_dir}/{tier}_risk.csv"
                    with open(csv_path, 'w', newline='') as f:
                        writer = csv.DictWriter(f, fieldnames=field_order)
                        writer.writeheader()
                        writer.writerows(txns)

                    with open(csv_path,"r") as f:
                        cur.copy_expert(
                            f"COPY fraud_{tier}_risk ({', '.join(field_order)}) "
                            f"FROM STDIN WITH CSV HEADER",
                            f
                        )
                    conn.commit()


    @task
    def send_tiered_summary():
        hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
        
        tiers = ['high','medium','low']   
        for tier in tiers:
            sql = f"""
                    SELECT 
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE final_fraud_flag = TRUE) as flagged,
                    AVG(risk_score)::NUMERIC(10,2) as avg_risk
                    FROM fraud_{tier}_risk;
                """
            total_cnt, flagged_cnt, avg_risk = hook.get_first(sql)
            print(f"=== {tier} risk tier ===")
            print(f"Total: {total_cnt}")
            print(f"Flagged: {flagged_cnt}")
            print(f"Avg Risk: {avg_risk}")


    # Sequential flow
    txns = extract_transactions()
    scored = calculate_risk_scores(txns)
    route = route_by_risk(scored)

    # Branching (only one executes)
    high = process_high_risk()
    medium = process_medium_risk()
    low = process_low_risk()

    # Synchronization point
    checks = run_fraud_checks()

    # Parallel fraud checks
    velocity = velocity_check(checks)
    merchant = merchant_validation(checks)

    # Merge and load
    merged = merge_results(velocity, merchant)
    loaded = load_to_database(merged)
    summary = send_tiered_summary()

    # Dependencies
    route >> [high, medium, low] >> checks
    checks >> [velocity, merchant] >> merged >> loaded >> summary

dag = challenge2()




