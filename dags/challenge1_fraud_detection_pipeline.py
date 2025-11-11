import os
import pendulum
from datetime import datetime, timedelta
import random
import csv
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


@dag(
    dag_id="challenge1_fraud_detection_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
)

def challenge_1():
    @task
    def extract_transactions():
        # real api call
        # response = requests.get(URL)
        # transactions = response.json()
        # return transactions

        # we'll simulate fake data locally for now
        transactions = []
        for i in range(10):
            transaction = {
                'transaction_id': f"TXN_{i+1:03}",
                'amount': random.randint(50, 5000),
                'merchant': random.choice(["Amazon", "Netflix", "Meta", "Google", "Apple"]),
                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            transactions.append(transaction)
        return transactions
    
    @task
    def risk_score(transactions):
        for transaction in transactions:
            transaction['risk_score'] = (transaction['amount'] / 100) + (10 if transaction['merchant'] in ["Apple", "Amazon"] else 5)
        return transactions
    
    @task
    def flag_high_risk(transactions):
        for transaction in transactions:
            transaction['is_high_risk'] = (transaction['risk_score'] > 40)
        return transactions

    @task
    def create_transaction_tables():
        sql_main = """
        CREATE TABLE IF NOT EXISTS fraud_transactions (
            transaction_id VARCHAR(50) PRIMARY KEY,
            amount INTEGER,
            merchant VARCHAR(100),
            timestamp VARCHAR(50),
            risk_score FLOAT,
            is_high_risk BOOLEAN
         );
        """
        sql_temp = """
        DROP TABLE IF EXISTS transaction_temp;
        CREATE TABLE transaction_temp (
            transaction_id VARCHAR(50) PRIMARY KEY,
            amount INTEGER,
            merchant VARCHAR(100),
            timestamp VARCHAR(50),
            risk_score FLOAT,
            is_high_risk BOOLEAN
        );
        """
        hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
        hook.run(sql_main)
        hook.run(sql_temp)
        print("✅ Transaction tables ready.")

    @task
    def load_to_db(transactions):
        hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
        conn = hook.get_conn()
        cur = conn.cursor()

        # write to a temp CSV for fast COPY
        data_path = "/opt/airflow/dags/files/transactions.csv"
        os.makedirs(os.path.dirname(data_path), exist_ok=True)
        field_order = [
            "transaction_id",
            "amount",
            "merchant",
            "timestamp",
            "risk_score",
            "is_high_risk",
        ]
        with open(data_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=field_order)
            writer.writeheader()
            writer.writerows(transactions)

        cur.execute("TRUNCATE TABLE transaction_temp;")
        with open(data_path, "r") as f:
            cur.copy_expert("COPY transaction_temp (transaction_id, amount, merchant, timestamp, risk_score, is_high_risk)" \
            " FROM STDIN WITH CSV HEADER DELIMITER ','", f)
        conn.commit()

        merge_sql = """
        INSERT INTO fraud_transactions
        SELECT * FROM transaction_temp
        WHERE is_high_risk = TRUE
        ON CONFLICT (transaction_id) DO UPDATE
        SET
            amount = EXCLUDED.amount,
            merchant = EXCLUDED.merchant,
            timestamp = EXCLUDED.timestamp,
            risk_score = EXCLUDED.risk_score,
            is_high_risk = EXCLUDED.is_high_risk;
        """
        cur.execute(merge_sql)
        conn.commit()
        cur.close()
        conn.close()
        print("✅ High-risk transactions merged into fraud_transactions.")

    @task
    def send_summary():
        hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
        risk_summary = hook.get_records("""
                        SELECT COUNT(*) as total_cnt,
                               AVG(risk_score)::NUMERIC(10,2) as avg_risk,
                               MAX(risk_score)::NUMERIC(10,2) as max_risk
                        FROM fraud_transactions
                        """)
        print("Overall Summary")
        print("="*60)
        for row in risk_summary:
            print(f"Total flagged transactions: {row[0]} \n Average Risk score: {row[1]} \n Highest risk score: {row[2]}")
        print("="*60)



    txn0 = extract_transactions()
    txn1 = risk_score(txn0)
    txn_flagged = flag_high_risk(txn1)
    create_tables = create_transaction_tables()
    summary = send_summary()
    create_tables >> load_to_db(txn_flagged) >> summary

dag = challenge_1()


        

