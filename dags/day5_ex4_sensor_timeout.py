from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

def process_external_report(**context):
    """Process report from external system"""
    print("📄 Processing external fraud report...")
    print("   Analyzing data from partner system")
    print("   Extracting high-risk transactions")
    return {'source': 'external', 'transactions': 150}

def process_internal_data(**context):
    """Fallback: Process internal data if external report doesn't arrive"""
    print("📊 External report not available")
    print("   Processing internal fraud data instead")
    print("   Using historical patterns")
    return {'source': 'internal', 'transactions': 100}

def generate_final_report(**context):
    """Generate final report using whichever data source was available"""
    ti = context['ti']
    
    # Try to get external report data
    external_data = ti.xcom_pull(task_ids='process_external_report')
    internal_data = ti.xcom_pull(task_ids='process_internal_data')
    
    print("📋 Generating Final Fraud Report")
    print("=" * 50)
    
    if external_data:
        print(f"✅ Using EXTERNAL data source")
        print(f"   Transactions analyzed: {external_data['transactions']}")
    elif internal_data:
        print(f"⚠️ Using INTERNAL data source (fallback)")
        print(f"   Transactions analyzed: {internal_data['transactions']}")
    else:
        print(f"❌ No data available!")
    
    print("=" * 50)

def notify_team(**context):
    """Notify team about report generation"""
    ti = context['ti']
    external_data = ti.xcom_pull(task_ids='process_external_report')
    
    if external_data:
        print("📧 Email: Fraud report generated using partner data")
    else:
        print("📧 Email: Fraud report generated using internal data (partner data unavailable)")


def route_by_external_status(**context):
    """Branch based on whether external report is available"""  
    ti = context['ti']
    external_data = ti.xcom_pull(task_ids='process_external_report')
    
    if external_data:
        print("External File Received: Processed using partner data")
        return 'process_external_report'
    else:
        print("External File Not Received: Processed using internal data")
        return 'process_internal_data'
    

with DAG(
    'day5_ex4_sensor_timeout',
    default_args={
        'owner': 'Xiaowan(Abby) Liu',
        'retries': 0,
    },
    description='Exercise: Sensor with timeout and soft_fail',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['day5', 'exercise', 'sensors', 'timeout'],
) as dag:
    
    # Create directory
    create_dir = BashOperator(
        task_id='create_directory',
        bash_command='mkdir -p /opt/airflow/dags/fraud_reports'
    )
    
    # Wait for external report (with timeout and soft_fail)
    wait_for_external = FileSensor(
        task_id='wait_for_external_report',
        filepath='/opt/airflow/dags/fraud_reports/external_report.csv',
        fs_conn_id='fs_default',
        poke_interval=5,
        timeout=30,  # Only wait 30 seconds
        soft_fail=True,  # Don't fail DAG if timeout - just skip
        mode='poke'
    )
    
    route_by_external = BranchPythonOperator(
        task_id='route_by_external_status',
        python_callable= route_by_external_status,
        trigger_rule='all_done'
    )

    # Process external report (only if file arrived)
    process_external = PythonOperator(
        task_id='process_external_report',
        python_callable=process_external_report
    )
    
    # Fallback: Process internal data (runs if external times out)
    process_internal = PythonOperator(
        task_id='process_internal_data',
        python_callable=process_internal_data,
#       trigger_rule='all_failed'  # Runs if wait_for_external is skipped/failed
    )
    
    # Generate final report (runs if EITHER external or internal succeeds)
    final_report = PythonOperator(
        task_id='generate_final_report',
        python_callable=generate_final_report,
        trigger_rule='one_success'  # Runs if either processing succeeded
    )
    
    # Notify team (always runs)
    notify = PythonOperator(
        task_id='notify_team',
        python_callable=notify_team,
        trigger_rule='all_done'  # Always notify, regardless of outcome
    )
    
    # Cleanup
    cleanup = BashOperator(
        task_id='cleanup',
        bash_command='rm -f /opt/airflow/dags/fraud_reports/external_report.csv',
        trigger_rule='all_done'
    )
    
    # Dependencies
    create_dir >> wait_for_external >> route_by_external >> [process_external,process_internal] >> final_report
    final_report >> notify >> cleanup