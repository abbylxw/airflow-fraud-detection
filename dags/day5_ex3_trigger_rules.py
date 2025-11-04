from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random

def fraud_check_amount(**context):
    """Check transaction amount - might fail"""
    print("🔍 Checking transaction amount...")
    if random.choice([True, False, True]):  # 66% success rate
        print("✅ Amount check passed")
        return {'check': 'amount', 'status': 'pass'}
    else:
        print("❌ Amount check failed")
        raise Exception("Amount check failed - suspicious pattern detected")

def fraud_check_location(**context):
    """Check transaction location - might fail"""
    print("🌍 Checking transaction location...")
    if random.choice([True, False, True]):  # 66% success rate
        print("✅ Location check passed")
        return {'check': 'location', 'status': 'pass'}
    else:
        print("❌ Location check failed")
        raise Exception("Location check failed - suspicious location")

def fraud_check_velocity(**context):
    """Check transaction velocity - might fail"""
    print("⚡ Checking transaction velocity...")
    if random.choice([True, False, True]):  # 66% success rate
        print("✅ Velocity check passed")
        return {'check': 'velocity', 'status': 'pass'}
    else:
        print("❌ Velocity check failed")
        raise Exception("Velocity check failed - too many transactions")

def generate_report(**context):
    """Generate report - runs if ANY check succeeded"""
    ti = context['ti']
    
    print("📊 Generating fraud report...")
    
    # Try to pull results from all checks
    checks = ['fraud_check_amount', 'fraud_check_location', 'fraud_check_velocity']
    results = []
    
    for check in checks:
        try:
            result = ti.xcom_pull(task_ids=check)
            if result:
                results.append(result)
                print(f"   ✅ {result['check']}: {result['status']}")
        except:
            print(f"   ❌ {check}: No data (likely failed)")
    
    print(f"📄 Report generated with {len(results)} successful checks")

def send_failure_alert(**context):
    """Send alert - runs if ANY check failed"""
    print("🚨 FRAUD ALERT!")
    print("   One or more fraud checks failed")
    print("   Manual investigation required")
    print("   Notifying fraud team...")

def cleanup(**context):
    """Cleanup - ALWAYS runs"""
    print("🧹 Cleanup: Releasing resources...")
    print("   Closing database connections")
    print("   Clearing cache")
    print("   Updating audit logs")

with DAG(
    'day5_exercise3_trigger_rules',
    default_args={'owner': 'Xiaowan(Abby) Liu', 'retries': 0},
    description='Exercise: Trigger rules in fraud detection',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['day5', 'exercise', 'trigger_rules'],
) as dag:
    
    # Three parallel fraud checks (each might fail)
    check_amount = PythonOperator(
        task_id='fraud_check_amount',
        python_callable=fraud_check_amount
    )
    
    check_location = PythonOperator(
        task_id='fraud_check_location',
        python_callable=fraud_check_location
    )
    
    check_velocity = PythonOperator(
        task_id='fraud_check_velocity',
        python_callable=fraud_check_velocity
    )
    
    # Generate report if ANY check succeeded
    report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report,
        trigger_rule='one_success'  
    )
    
    # Send alert if ANY check failed
    alert = PythonOperator(
        task_id='send_failure_alert',
        python_callable=send_failure_alert,
        trigger_rule='one_failed'  
    )
    
    # Cleanup ALWAYS runs
    cleanup_task = PythonOperator(
        task_id='cleanup',
        python_callable=cleanup,
        trigger_rule='all_done'  
    )
    

    # Pattern:
    # - Three checks run in parallel
    # - Report needs results from checks
    # - Alert monitors for failures
    # - Cleanup runs after everything
    [check_amount, check_location, check_velocity] >> report
    [check_amount, check_location, check_velocity] >> alert
    [report, alert] >> cleanup_task