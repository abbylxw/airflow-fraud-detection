import pendulum
from airflow.sdk import dag, task, Variable


@dag(
    'day9_variables_demo',
    start_date=pendulum.datetime(2025, 11, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=['day9', 'variables']
)
def variables_demo():
    
    @task
    def get_config():
        """Fetch configuration from Airflow Variables."""
        # Method 1: Get with default (won't fail if missing)
        threshold = Variable.get("fraud_score_threshold", default_var=70)
        high_risk = Variable.get("high_risk_amount", default_var=10000)
        env = Variable.get("environment", default_var="unknown")
        
        config = {
            "fraud_score_threshold": int(threshold),
            "high_risk_amount": int(high_risk),
            "environment": env
        }
        print(f"📋 Loaded config: {config}")
        return config
    
    @task
    def analyze_with_config(config):
        """Use config in fraud analysis."""
        # Simulated transactions
        transactions = [
            {"id": 1, "amount": 3000, "score": 60},
            {"id": 2, "amount": 8000, "score": 85},
            {"id": 3, "amount": 6000, "score": 78},
        ]
        
        flagged = []
        for txn in transactions:
            if txn["score"] >= config["fraud_score_threshold"] or txn["amount"] >= config["high_risk_amount"]:
                flagged.append(txn)
        
        print(f"🚨 Flagged {len(flagged)} transactions using threshold={config['fraud_score_threshold']}")
        return flagged
    
    config = get_config()
    analyze_with_config(config)

variables_demo()