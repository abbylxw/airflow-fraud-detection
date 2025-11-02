from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Merchants to process
MERCHANTS = ['MERCHANT_A', 'MERCHANT_B', 'MERCHANT_C', 'MERCHANT_D', 'MERCHANT_E']

def fetch_merchant_data(merchant_id, **context):
    print(f"📥 Fetching data for {merchant_id}...")
    # Simulate different transaction counts
    txn_count = hash(merchant_id) % 100
    return{merchant_id: txn_count}

def analyze_merchant_fraud(merchant_id, **context):
    data = context['ti'].xcom_pull(task_ids = f'fetch_{merchant_id}')
    txn_count = data[merchant_id]
    fraud_rate = (txn_count * 0.05) # Simulate 5% fraud rate
    print(f"🔍 {merchant_id}: {txn_count} transactions, {fraud_rate:.1f} suspected fraud")
    return{merchant_id: fraud_rate}

def generate_summary_report(**context):
    ti = context['ti']
    summary = {}
    for merchant in MERCHANTS:
        result = ti.xcom_pull(task_ids = f'analyze_{merchant}')
        summary.update(result)

    total_fraud = sum(summary.values())
    print(f"📊 SUMMARY REPORT:")
    print(f"   Total suspected fraud across all merchants: {total_fraud:.1f}")
    print(f"   Merchant breakdown: {summary}")

with DAG(
    'day4_ex2_dynamic_merchants',
    default_args={
        'owner': 'Xiaowan(Abby) Liu',
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    description='Exercise: Dynamic merchant fraud processing',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['day4', 'exercise', 'dynamic'],
) as dag:
    
    fetch_tasks = []
    analyze_tasks = []
    for merchant in MERCHANTS:
        fetch = PythonOperator(
            task_id = f"fetch_{merchant}",
            python_callable = fetch_merchant_data,
            op_kwargs = {'merchant_id': merchant}
        )
        analyze = PythonOperator(
            task_id = f"analyze_{merchant}",
            python_callable = analyze_merchant_fraud,
            op_kwargs = {'merchant_id': merchant}
        )

    fetch >> analyze

    fetch_tasks.append(fetch)
    analyze_tasks.append(analyze)

    # Create the summary report task
    summary = PythonOperator(
        task_id='generate_summary_report',
        python_callable=generate_summary_report
    )
    
    # Set dependency so all analyze tasks >> summary
    analyze_tasks >> summary
    