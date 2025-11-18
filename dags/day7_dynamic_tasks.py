import os
import pendulum
from datetime import timedelta
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


@dag(
    dag_id = 'day7_dynamic_tasks',
    start_date= pendulum.datetime(2025, 11, 1, tz="UTC"),
    schedule=None,
    catchup=False,
) 

def dynamic_tasks():

    @task
    def fetch_all_transactions():
        import random
        transactions = []
        for i in range(20):
            transaction = {
                'txn_id': i,
                'amount': random.randint(100, 30000),
                'region': random.choice(['us','eu','apac','latam']),
                'flagged': random.choice([True, False])
            }
            transactions.append(transaction)
        return transactions
    
    @task
    def process_region(region, transactions):
        hook = PostgresHook(postgres_conn_id = 'tutorial_pg_conn')
        data_region = [t for t in transactions if t['region'] == region]
        if not data_region:
            print(f"⚠️ No transactions found for region: {region}")
            return {
                'region': region,
                'total_count': 0,
                'flagged_count': 0,
                'total_amount': 0,
                'avg_amount': 0
            }
        
        total_count = len(data_region)
        flagged_count = len([t for t in data_region if t['flagged'] == True])
        total_amount = sum([t['amount'] for t in data_region])
        avg_amount = total_amount/total_count
        output = {'region': region,
                  'total_count': total_count,
                  'flagged_count': flagged_count,
                  'total_amount': total_amount,
                  'avg_amount': avg_amount
                  }
        sql_create = f"""
                    DROP TABLE IF EXISTS fraud_stats_{region};
                    CREATE TABLE IF NOT EXISTS fraud_stats_{region}(
                        total_count INT,
                        flagged_count INT,
                        total_amount NUMERIC,
                        avg_amount nUMERIC
                    )
                    """
        hook.run(sql_create)
        hook.run(
            f"""
            INSERT INTO fraud_stats_{region}
            VALUES (%s, %s, %s, %s)
            """,
            parameters=(total_count, flagged_count, total_amount, avg_amount)
        )
        return output
    
    @task
    def aggregate_summary(results):
        print("\n" + "="*70)
        print("GLOBAL FRAUD DETECTION SUMMARY")
        print("="*70)
        
        total_txns = sum([r['total_count'] for r in results])
        total_flagged = sum([r['flagged_count'] for r in results])
        total_amount = sum([r['total_amount'] for r in results])
        
        print(f"\n📊 OVERALL STATISTICS")
        print(f"  Total Transactions: {total_txns}")
        print(f"  Total Flagged: {total_flagged} ({total_flagged/total_txns*100:.1f}%)")
        print(f"  Total Amount: ${total_amount:,.2f}")
        print(f"  Average Amount: ${total_amount/total_txns:.2f}")
        
        print(f"\n🌍 REGIONAL BREAKDOWN")
        print("-"*70)
        for r in results:
            print(f"{r['region'].upper():>6}:  {r['total_count']} txns")
            
    
    regions = ['us','eu','apac','latam']
    transactions = fetch_all_transactions()
    region_results = process_region.partial(
                    transactions=transactions  # Same for all - use .partial()
                    ).expand(
                    region=regions  # Different per instance - use .expand()
                    )
    aggregate_summary(region_results)

dag = dynamic_tasks()  
    
    



        




        





    


