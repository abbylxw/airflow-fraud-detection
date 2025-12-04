import pendulum
from airflow.sdk import dag, task
from airflow.utils.task_group import TaskGroup


@dag(
    'day8_taskgroups_fraud',
    start_date=pendulum.datetime(2025, 11, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=['day8', 'taskgroups']
)
def taskgroups_fraud_pipeline():
    
    # TaskGroup 1: Ingestion
    with TaskGroup(group_id='ingestion') as ingestion_group:
        
        @task
        def fetch_transactions():
            print("📥 Fetching transactions...")
            return [{"id": 1, "amount": 500}, {"id": 2, "amount": 15000}]
        
        @task
        def validate_data(txns):
            print(f"✅ Validating {len(txns)} transactions...")
            return txns
        
        @task
        def dedupe_records(txns):
            print("🔄 Deduplicating records...")
            return txns
        
        # Chain within group
        txns = fetch_transactions()
        validated = validate_data(txns)
        deduped = dedupe_records(validated)
    
    # TaskGroup 2 - Analysis (rule_check, ml_score, aggregate)
    with TaskGroup(group_id='analysis') as analysis_group:
        import random 

        @task
        def rule_check(txns):
            print("Rule Engine Logic for fraud check")
            for txn in txns:
                txn['rule_flagged'] = random.choice([0,1])
            return txns
        
        @task
        def ml_score(txns):
            print("Fraud Scoring for fraud check")
            for txn in txns:
                txn['ml_score'] = random.randint(0,100)
            return txns
        
        @task
        def aggregate(txn_rule, txn_score):
            print("Aggregating rule check and ml scores")
            merged = []
            map1 = {item["id"]: item for item in txn_rule}
            map2 = {item["id"]: item for item in txn_score}
            for _id in map1.keys() | map2.keys():   # union of keys
                merged_row = {}
                if _id in map1:
                    merged_row.update(map1[_id])
                if _id in map2:
                    merged_row.update(map2[_id])
                merged.append(merged_row)
            return merged
        txn1 = rule_check(deduped)
        txn2 = ml_score(deduped)
        merged = aggregate(txn1, txn2)
                
    # TaskGroup 3 - Alerting (send_alerts, update_dashboard, log_results)
    with TaskGroup(group_id='alerting') as alerting_group:
        
        @task
        def send_alerts(merged):
            alerted = []
            for txn in merged:
                if txn['ml_score'] >= 50 or txn['rule_flagged'] == 1:
                    alerted.append(txn)
            print(f"{len(alerted)} transactions are alerted.")
            return alerted
        
        @task
        def update_dashboard(txns):
            print("Updating dashboard.")
        
        @task
        def log_results(txns):
            print("Logging results.")

        alerted = send_alerts(merged) 
        update_dashboard(alerted)
        log_results(alerted)

    # Connect the groups: ingestion_group >> analysis_group >> alerting_group
    # not needed since each group is using upstream group output as input

taskgroups_fraud_pipeline()