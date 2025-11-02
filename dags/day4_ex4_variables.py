from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta

def check_transaction_fraud(**context):
    # Get variables
    threshold = int(Variable.get("fraud_amount_threshold", default_var=5000))
    alert_email = Variable.get("fraud_alert_email", default_var="lxw9301@gmail.com")
    
    # Simulate transaction
    transaction_amount = 7500
    
    print(f"🔍 Checking transaction: ${transaction_amount}")
    print(f"🎯 Threshold: ${threshold}")
    print(f"📧 Alert email: {alert_email}")
    
    if transaction_amount > threshold:
        print(f"🚨 FRAUD ALERT! Transaction exceeds threshold!")
        return {'is_fraud': True, 'amount': transaction_amount}
    else:
        print(f"✅ Transaction approved")
        return {'is_fraud': False, 'amount': transaction_amount}

def send_fraud_alert(**context):
    result = context['ti'].xcom_pull(task_ids='check_transaction_fraud')
    alert_email = Variable.get("fraud_alert_email")
    
    if result['is_fraud']:
        print(f"📧 Sending fraud alert to {alert_email}")
        print(f"   Transaction amount: ${result['amount']}")
    else:
        print(f"ℹ️ No alert needed - transaction approved")

with DAG(
    'day4_ex4_variables',
    default_args={'owner': 'Xiaowan(Abby) Liu', 'retries': 1},
    description='Exercise: Using Variables for fraud configuration',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['day4', 'exercise', 'variables'],
) as dag:
    
    # Task 1: Log execution info with Jinja template
    log_info = BashOperator(
        task_id='log_execution_info',
        bash_command='echo "Fraud check for date: {{ ds }} at {{ ts }}"'
    )
    
    # Task 2: Check transaction
    check_fraud = PythonOperator(
        task_id='check_transaction_fraud',
        python_callable=check_transaction_fraud
    )
    
    # Task 3: Send alert if needed
    send_alert = PythonOperator(
        task_id='send_fraud_alert',
        python_callable=send_fraud_alert
    )
    
    log_info >> check_fraud >> send_alert