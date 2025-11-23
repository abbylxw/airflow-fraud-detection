"""
Refactored multi-region fraud detection using custom operators
"""
import pendulum
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from operators.fraud_api_operator import FraudAPIOperator
from operators.risk_scoring_operator import RiskScoringOperator


@dag(
    'day7_dynamic_with_operators',
    start_date=pendulum.datetime(2025, 11, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=['day7', 'dynamic', 'custom-operators', 'fraud-detection'],
    description='Multi-region fraud detection using custom operators'
)
def dynamic_with_operators():
    # Step 1: Fetch transactions from API (using custom operator!)
    fetch_transactions = FraudAPIOperator(
        task_id='fetch_transactions',
        endpoint='https://jsonplaceholder.typicode.com/posts',  # Mock API
        method='GET',
        timeout=10,
        max_retries=3
    )
    
    @task
    def prepare_transactions(**context):
        """Convert API response to transaction format with regions"""
        import random
        
        ti = context['ti']
        api_response = ti.xcom_pull(task_ids='fetch_transactions')
        
        # Convert posts to mock transactions with regions
        transactions = []
        for post in api_response['data'][:20]:  # Use first 20 posts
            transaction = {
                'txn_id': post['id'],
                'amount': random.randint(100, 30000),
                'region': random.choice(['us', 'eu', 'apac', 'latam']),
                'merchant_risk': random.randint(10, 100),
                'quality_score': random.randint(40, 100),
                'user_id': post['userId']
            }
            transactions.append(transaction)
        
        print(f"✅ Prepared {len(transactions)} transactions")
        return transactions
    
    # Step 2: Score risk (using custom operator!)
    score_risk = RiskScoringOperator(
        task_id='score_all_transactions',
        upstream_task_id = 'prepare_transactions',
        amount_threshold_high=10000,
        amount_threshold_medium=5000
    )
    
    @task
    def process_region(region: str, **context):
        """Process transactions for a specific region"""
        hook = PostgresHook(postgres_conn_id='tutorial_pg_conn')

        ti = context['ti']
        transactions = ti.xcom_pull(task_ids='score_all_transactions')
        
        # Filter for this region
        regional_txns = [t for t in transactions if t['region'] == region]
        
        if not regional_txns:
            print(f"⚠️ No transactions for region: {region}")
            final_data = {
                'region': region,
                'total_count': 0,
                'critical_count': 0,
                'high_count': 0,
                'avg_risk': 0
            }
            return final_data
        else:
            # Calculate statistics
            total_count = len(regional_txns)
            critical_count = len([t for t in regional_txns if t['severity'] == 'CRITICAL'])
            high_count = len([t for t in regional_txns if t['severity'] == 'HIGH'])
            avg_risk = sum([t['risk_score'] for t in regional_txns]) / total_count
            
            # Create table
            hook.run(f"""
                DROP TABLE IF EXISTS fraud_regional_{region};
                CREATE TABLE fraud_regional_{region} (
                    txn_id INT,
                    amount NUMERIC,
                    risk_score NUMERIC,
                    severity VARCHAR(20),
                    merchant_risk INT,
                    quality_score INT
                );
            """)
        
            # Insert transactions
            for txn in regional_txns:
                hook.run(f"""
                    INSERT INTO fraud_regional_{region} 
                    (txn_id, amount, risk_score, severity, merchant_risk, quality_score)
                    VALUES ({txn['txn_id']}, {txn['amount']}, {txn['risk_score']}, 
                            '{txn['severity']}', {txn.get('merchant_risk', 50)}, 
                            {txn.get('quality_score', 100)});
                """)
            
            result = {
                'region': region,
                'total_count': total_count,
                'critical_count': critical_count,
                'high_count': high_count,
                'avg_risk': round(avg_risk, 2)
            }
            
            print(f"✅ {region.upper()}: {total_count} txns, {critical_count} critical, avg risk={avg_risk:.1f}")
            return result
    
    @task
    def generate_global_report(regional_results):
        """Aggregate regional results into global report"""
        print("\n" + "="*70)
        print("GLOBAL FRAUD DETECTION REPORT")
        print("="*70)
        
        total_txns = sum([r['total_count'] for r in regional_results])
        total_critical = sum([r['critical_count'] for r in regional_results])
        total_high = sum([r['high_count'] for r in regional_results])
        
        print(f"\n📊 GLOBAL SUMMARY")
        print(f"  Total Transactions: {total_txns}")
        print(f"  Critical Alerts: {total_critical}")
        print(f"  High Risk: {total_high}")
        print(f"  Alert Rate: {(total_critical + total_high)/total_txns*100:.1f}%")
        
        print(f"\n🌍 REGIONAL BREAKDOWN")
        print("-"*70)
        for r in regional_results:
            if r['total_count'] > 0:
                alert_rate = (r['critical_count'] + r['high_count']) / r['total_count'] * 100
                print(f"  {r['region'].upper():6} | Txns: {r['total_count']:3} | "
                      f"Critical: {r['critical_count']:2} | High: {r['high_count']:2} | "
                      f"Avg Risk: {r['avg_risk']:5.1f} | Alert Rate: {alert_rate:5.1f}%")
        
        print("="*70 + "\n")
        
        return {
            'total_transactions': total_txns,
            'total_critical': total_critical,
            'total_high': total_high
        }
    
    # Build the workflow
    regions = ['us', 'eu', 'apac', 'latam']
    
    # Fetch → Prepare → Score
    api_data = fetch_transactions
    prepared_txns = prepare_transactions()
    scored_txns = score_risk
    
    # Dynamic regional processing
    regional_results = process_region.expand(
        region=regions
    )
    
    # Generate report
    report = generate_global_report(regional_results)
    
    # Dependencies
    api_data >> prepared_txns >> scored_txns >> regional_results >> report

dynamic_with_operators()