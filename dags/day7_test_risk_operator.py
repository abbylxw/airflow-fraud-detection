import pendulum
from airflow.sdk import dag, task
from operators.risk_scoring_operator import RiskScoringOperator


@dag(
    'day7_test_risk_operator',
    start_date=pendulum.datetime(2025, 11, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=['day7', 'custom-operator', 'risk'],
)
def test_risk_operator():
    
    @task
    def generate_test_transactions():
        """Generate sample transactions for testing"""
        test_data = [
            {'txn_id': 1, 'amount': 500, 'merchant_risk': 30, 'quality_score': 95},
            {'txn_id': 2, 'amount': 15000, 'merchant_risk': 80, 'quality_score': 60},
            {'txn_id': 3, 'amount': 7500, 'merchant_risk': 50, 'quality_score': 85},
            {'txn_id': 4, 'amount': 200, 'merchant_risk': 20, 'quality_score': 40},
            {'txn_id': 5, 'amount': 25000, 'merchant_risk': 90, 'quality_score': 70},
        ]
        return test_data
    
    @task
    def display_results(**context):
        """Display the risk-scored transactions"""
        ti = context['ti']
        scored_txns = ti.xcom_pull(task_ids='score_risk')
        
        print("\n" + "="*70)
        print("RISK SCORING RESULTS")
        print("="*70)
        
        for txn in scored_txns:
            print(f"\nTransaction {txn['txn_id']}:")
            print(f"  Amount: ${txn['amount']:,}")
            print(f"  Risk Score: {txn['risk_score']}")
            print(f"  Severity: {txn['severity']}")
        
        print("="*70 + "\n")
    
    # Workflow
    transactions = generate_test_transactions()
    
    score_risk = RiskScoringOperator(
        task_id='score_risk',
        upstream_task_id='generate_test_transactions', 
        amount_threshold_high=10000,
        amount_threshold_medium=5000
    )
    
    transactions >> score_risk >> display_results()

test_risk_operator()