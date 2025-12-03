import pendulum
from airflow.sdk import dag, task


@dag(
    'data_ingestion_dag',
    start_date=pendulum.datetime(2025, 11, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=['day7', 'External file sensor']
)
def data_ingestion_dag():
    @task
    def ingestion_complete():
            import random
            
            # Convert posts to mock transactions with regions
            transactions = []
            for i in range(20): 
                transaction = {
                    'txn_id': f'TXN{100 + i}',
                    'amount': random.randint(100, 30000),
                    'region': random.choice(['us', 'eu', 'apac', 'latam']),
                    'merchant_risk': random.randint(10, 100),
                    'quality_score': random.randint(40, 100)
                }
                transactions.append(transaction)
                print(transaction)
            
            print(f"✅ Prepared {len(transactions)} transactions")
            return transactions
data_ingestion_dag()