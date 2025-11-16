import os
import pendulum
from datetime import timedelta
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

@dag(
    dag_id = 'challenge3_production_fraud_detection',
    schedule = None, #'@hourly'  # Run every hour
    start_date = pendulum.datetime(2025, 11, 1, tz="UTC"),
    catchup = False,
    dagrun_timeout = timedelta(minutes=15),
    default_args = {
        'retries': 2,
        'retry_delay': timedelta(minutes=1),
    }
)
def challenge3():
    @task(retries=3, retry_delay=timedelta(seconds=30))
    def fetch_transactions():
        import random
        import requests
        from datetime import datetime
        url = 'https://jsonplaceholder.typicode.com/users'
        response = requests.get(url)
        response.raise_for_status()
        users = response.json()

        transactions = []
        for user in users:
            transaction = {
                'transaction_id': f'TXN_{user['id']:03d}',
                'amount': user['id']*100 + random.randint(10,500),
                'merchant': user['company']['name'],
                'customer_name': user['name'],
                'email': user['email'],
                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            transactions.append(transaction)

        return transactions


    @task
    def validate_data_quality(transactions):
        for transaction in transactions:
            transaction['quality_score'] = (
                (0 if any(value == '' for value in transaction.values()) else 1) +
                (1 if 0 < transaction['amount'] < 100000 else 0) +
                (1 if '@' in transaction['email'] else 0) + 
                (1 if transaction['merchant'] != "" else 0)
            ) * 25
            transaction['has_quality_issues'] = (transaction['quality_score'] < 100)
        return transactions
    
    @task.branch
    def route_data_quality(transactions):
        import json
        from airflow.sdk import Variable
        Variable.set('current_transactions', json.dumps(transactions))
        issue_pct = sum(t['has_quality_issues'] == True for t in transactions)/len(transactions)*100
        if issue_pct > 20:
            return 'quarantine_dirty_data'
        else:
            return 'process_clean_data'
        
    @task
    def process_clean_data():
        import json
        from airflow.sdk import Variable
        transactions = json.loads(Variable.get('current_transactions'))
        for transaction in transactions:
            if transaction['quality_score'] >= 75:
                transaction['data_status'] = 'CLEAN'
            else:
                transaction['data_status'] = 'QUARANTINED'
        Variable.set('tagged_transactions', json.dumps(transactions))
        return transactions
    
    @task
    def quarantine_dirty_data():
        import json
        import csv
        from airflow.sdk import Variable
        transactions = json.loads(Variable.get('current_transactions'))
        for transaction in transactions:
            if transaction['quality_score'] >= 75:
                transaction['data_status'] = 'CLEAN'
            else:
                transaction['data_status'] = 'QUARANTINED'
        Variable.set('tagged_transactions', json.dumps(transactions))

        data_dir = "/opt/airflow/dags/files"
        os.makedirs(data_dir, exist_ok=True)
        quarantined = [t for t in transactions if t['data_status'] == 'QUARANTINED']
        field_order = ['transaction_id','amount','merchant',
                       'customer_name','email','timestamp']
        csv_path = f"{data_dir}/quarantined_data.csv"
        if quarantined:
            with open(csv_path, 'w', newline = '') as f:
                writer = csv.DictWriter(f, fieldnames=field_order)
                writer.writeheader()
                writer.write(quarantined)
            print(f"⚠️ {len(quarantined)} transactions quarantined for review")

        return transactions
    
    @task(trigger_rule='one_success')
    def enrich_merchant_data():
        import random
        import requests
        import json
        from datetime import datetime
        from airflow.sdk import Variable
        transactions = json.loads(Variable.get('tagged_transactions'))

        merch_url = 'https://jsonplaceholder.typicode.com/posts/{post_id}' 
        
        for transaction in transactions:
            try:
                post_id = random.randint(1, 100)
                response = requests.get(merch_url)

                if response.status_code == 200:
                    post = response.json()
                    merchant_risk = min(len(post['body']) / 10, 100)
                    transaction['merchant_risk_score'] = merchant_risk
                    transaction['enrichment_status'] = 'SUCCESS'
                else:
                    transaction['merchant_risk_score'] = 50  # Neutral default
                    transaction['enrichment_status'] = 'API_ERROR'

            except Exception as e:
            # Network error - use default
                transaction['merchant_risk_score'] = 50
                transaction['enrichment_status'] = 'NETWORK_ERROR'

        return transactions
    
    @task
    def calculate_composite_risk(transactions):
        import random
        for transaction in transactions:
            amount_risk = min(transaction['amount']/250, 40)
            merchant_risk = transaction['merchant_risk_score'] * 0.2
            quality_penalty = (100 - transaction['quality_score']) * 0.1
            composite_risk = amount_risk + merchant_risk - quality_penalty
            composite_risk = random.randint(0,100)  # generate random numbers for fraud score simulation
            transaction['composite_risk_score'] = composite_risk
            transaction['severity'] = (
                'CRITICAL' if composite_risk >= 80
                else 'HIGH' if composite_risk >= 60
                else 'MEDIUM' if composite_risk >= 40
                else 'LOW'
            )
        return transactions
       
    
    @task.branch
    def route_by_severity(transactions):
        import json
        from airflow.sdk import Variable
        Variable.set('tiered_transactions', json.dumps(transactions))
        severities = [t['severity'] for t in transactions]
        if 'CRITICAL' in severities:
            return 'handle_critical'
        elif 'HIGH' in severities:
            return 'handle_high'
        elif 'MEDIUM' in severities:
            return 'handle_medium'
        else:
            return 'handle_low'
        
    @task(trigger_rule='one_success')
    def handle_critical():
        import json
        from airflow.sdk import Variable
        transactions = json.loads(Variable.get('tiered_transactions'))
        fraud_critical = [t for t in transactions if t['severity'] == 'CRITICAL']
        count_critical = len(fraud_critical)
        avg_score = sum([t['composite_risk_score'] for t in fraud_critical])/len(fraud_critical)
        txn_id_lst = [t['transaction_id'] for t in fraud_critical]
        print("**Print Urgent Alert:**")
        print("🚨 CRITICAL FRAUD ALERT 🚨")
        print(f"{count_critical} critical risk transactions detected!")
        print("Transaction IDs: ", txn_id_lst)
        print(f"Avg Risk Score: {avg_score}")
        print("Requires immediate manual review!")
        for t in transactions:
            t['alert_sent'] = True
        Variable.set('scored_transactions', json.dumps(transactions))
        return transactions
    
    @task
    def handle_high():
        import json
        from airflow.sdk import Variable
        transactions = json.loads(Variable.get('tiered_transactions'))
        fraud_critical = [t for t in transactions if t['severity'] == 'HIGH']
        count_critical = len(fraud_critical)
        avg_score = sum([t['composite_risk_score'] for t in fraud_critical])/len(fraud_critical)
        txn_id_lst = [t['transaction_id'] for t in fraud_critical]
        print("**Print High Alert:**")
        print("🚨 HIGH FRAUD ALERT 🚨")
        print(f"{count_critical} high risk transactions detected!")
        print("Transaction IDs: ", txn_id_lst)
        print(f"Avg Risk Score: {avg_score}")
        print("Requires immediate manual review!")
        for t in transactions:
            t['alert_sent'] = True
        Variable.set('scored_transactions', json.dumps(transactions))
        return transactions
    
    @task
    def handle_medium():
        import json
        from airflow.sdk import Variable
        transactions = json.loads(Variable.get('tiered_transactions'))
        fraud_critical = [t for t in transactions if t['severity'] == 'MEDIUM']
        count_critical = len(fraud_critical)
        avg_score = sum([t['composite_risk_score'] for t in fraud_critical])/len(fraud_critical)
        txn_id_lst = [t['transaction_id'] for t in fraud_critical]
        print("**Print Medium Alert:**")
        print("🚨 MEDIUM FRAUD ALERT 🚨")
        print(f"{count_critical} medium risk transactions detected!")
        print("Transaction IDs: ", txn_id_lst)
        print(f"Avg Risk Score: {avg_score}")
        for t in transactions:
            t['alert_sent'] = False
        Variable.set('scored_transactions', json.dumps(transactions))
        return transactions
    
    @task
    def handle_low():
        import json
        from airflow.sdk import Variable
        transactions = json.loads(Variable.get('tiered_transactions'))
        fraud_critical = [t for t in transactions if t['severity'] == 'LOW']
        count_critical = len(fraud_critical)
        avg_score = sum([t['composite_risk_score'] for t in fraud_critical])/len(fraud_critical)
        txn_id_lst = [t['transaction_id'] for t in fraud_critical]
        print("**Print Low Alert:**")
        print("🚨 LOW FRAUD ALERT 🚨")
        print(f"{count_critical} low risk transactions detected!")
        print("Transaction IDs: ", txn_id_lst)
        print(f"Avg Risk Score: {avg_score}")
        for t in transactions:
            t['alert_sent'] = False
        Variable.set('scored_transactions', json.dumps(transactions))
        return transactions
    


    @task(trigger_rule='one_success')
    def load_to_database():
        import csv
        import psycopg2
        from psycopg2.extras import execute_values
        import json
        from airflow.sdk import Variable
        transactions = json.loads(Variable.get('scored_transactions'))
        hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
        txn_groups = {
            "critical": [t for t in transactions if t["severity"] == "CRITICAL"],
            "high":     [t for t in transactions if t["severity"] == "HIGH"],
            "medium":   [t for t in transactions if t["severity"] == "MEDIUM"],
            "low":      [t for t in transactions if t["severity"] == "LOW"],
        }
        sql_create = """
            DROP TABLE IF EXISTS fraud_{tier};
            CREATE TABLE IF NOT EXISTS fraud_{tier}(
                transaction_id VARCHAR(50) PRIMARY KEY,
                amount INTEGER,
                merchant VARCHAR(100),
                customer_name VARCHAR(100),
                email VARCHAR(100),
                timestamp VARCHAR(50),
                quality_score FLOAT,
                merchant_risk_score FLOAT,
                composite_risk_score FLOAT,
                severity VARCHAR(20),
                data_status VARCHAR(20),
                enrichment_status VARCHAR(20),
                alert_sent BOOLEAN
            );
            """

        sql_insert = """
            INSERT INTO fraud_{tier} (transaction_id, amount, merchant,
            customer_name, email, timestamp, quality_score, merchant_risk_score,
            composite_risk_score, severity, data_status, enrichment_status, alert_sent)
            VALUES %s
            ON CONFLICT (transaction_id)
            DO UPDATE SET
                amount = EXCLUDED.amount,
                merchant = EXCLUDED.merchant,
                customer_name = EXCLUDED.customer_name,
                email = EXCLUDED.email,
                timestamp = EXCLUDED.timestamp,
                quality_score = EXCLUDED.quality_score,
                merchant_risk_score = EXCLUDED.merchant_risk_score,
                composite_risk_score = EXCLUDED.composite_risk_score,
                severity = EXCLUDED.severity,
                data_status = EXCLUDED.data_status,
                enrichment_status = EXCLUDED.enrichment_status,
                alert_sent = EXCLUDED.alert_sent;
            """
            
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                for tier, fraud_data in txn_groups.items():
                    cur.execute(sql_create.format(tier = tier))
                    print(f" Created table: fraud_{tier}")
                    if not fraud_data:
                        print(f"No rows for tier {tier}, skipping. ")
                        continue
                    else:
                        values = [
                        (
                            t["transaction_id"],
                            t["amount"],
                            t["merchant"],
                            t["customer_name"],
                            t["email"],
                            t["timestamp"],
                            t["quality_score"],
                            t["merchant_risk_score"],
                            t["composite_risk_score"],
                            t["severity"],
                            t["data_status"],
                            t["enrichment_status"],
                            t["alert_sent"],
                        )
                        for t in fraud_data
                        ]
                        execute_values(cur, sql_insert.format(tier=tier), values)
                        print(f"✅ Loaded {len(values)} rows into fraud_{tier}")
        
        print("🎉 Bulk load completed.")
        return transactions

    

    @task
    def send_summary_report():
        hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
        tiers = ['critical','high','medium','low']
        for tier in tiers:
            total_cnt, avg_risk = hook.get_first(f"""
                SELECT COUNT(*) as total_cnt,
                        AVG(composite_risk_score)::NUMERIC(10,2) as avg_risk
                FROM fraud_{tier}
                """)
            print(f"{tier.upper()}: {total_cnt} transactions (Avg Risk: {avg_risk})")
        total, clean_cnt, quarantined_cnt, success_cnt, api_cnt, network_cnt = hook.get_first("""
                    SELECT count(*) as total,
                    count(*) FILTER(WHERE DATA_STATUS = 'CLEAN') as clean_cnt,
                    COUNT(*) FILTER(WHERE DATA_STATUS = 'QUARANTINED') as quarantined_cnt,
                    COUNT(*) FILTER(WHERE enrichment_status = 'SUCCESS') as success_cnt,
                    COUNT(*) FILTER(WHERE enrichment_status = 'API_ERROR') as api_cnt,
                    COUNT(*) FILTER(WHERE enrichment_status = 'NETWORK_ERROR') as network_cnt   
                    FROM (
                        SELECT * FROM fraud_critical
                        UNION ALL
                        SELECT * FROM fraud_high
                        UNION ALL
                        SELECT * FROM fraud_medium
                        UNION ALL
                        SELECT * FROM fraud_low
                    ) AS merged;
                    """)
        print(f"TOTAL PROCESSED: {total}")
        print(f"DATA QUALITY:\n - Clean: {clean_cnt} transactions")
        print(f"- Quarantined: {quarantined_cnt} transactions")
        print(f"ENRICHMENT STATUS:")
        print(f"- Success: {success_cnt} ")
        print(f"- API Errors: {api_cnt} ")
        print(f"- Network Errors: {network_cnt} ")



    txns = fetch_transactions()
    validated = validate_data_quality(txns)
    route_quality = route_data_quality(validated)

    clean = process_clean_data()
    quarantine = quarantine_dirty_data()

    enriched = enrich_merchant_data()  
    risk_scored = calculate_composite_risk(enriched)
    route_severity = route_by_severity(risk_scored)

    critical = handle_critical()
    high = handle_high()
    medium = handle_medium()
    low = handle_low()

    loaded = load_to_database()  
    summary = send_summary_report()

    # Set dependencies
    route_quality >> [clean, quarantine] >> enriched >> risk_scored >> route_severity
    route_severity >> [critical, high, medium, low] >> loaded >> summary


dag = challenge3()

        
        
        
        




        
        
