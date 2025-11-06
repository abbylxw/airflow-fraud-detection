from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import random

def generate_sample_data(**context):
    """Generate sample fraud data (some with quality issues)"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Create varied data - some good, some with issues
    sample_data = [
        # Good records
        ('TXN_VALID_001', 'USER_001', 5000.00, 0.75, 'flagged'),
        ('TXN_VALID_002', 'USER_002', 15000.00, 0.90, 'blocked'),
        ('TXN_VALID_003', 'USER_003', 500.00, 0.25, 'approved'),
        
        # Records with potential issues
        ('TXN_NULL_001', None, 3000.00, 0.60, 'approved'),  # NULL user_id
        ('TXN_INVALID_001', 'USER_004', -500.00, 0.40, 'approved'),  # Negative amount
        ('TXN_RANGE_001', 'USER_005', 8000.00, 1.50, 'flagged'),  # Invalid risk score > 1
        ('TXN_VALID_004', 'USER_006', 2000.00, 0.45, 'approved'),
        ('TXN_NULL_002', 'USER_007', 7000.00, None, 'flagged'),  # NULL risk score
    ]
    
    # Randomly decide whether to include problematic data
    include_bad_data = context['dag_run'].conf.get('include_bad_data', True) if context['dag_run'].conf else True
    
    if not include_bad_data:
        # Filter out bad records for testing "pass" scenario
        sample_data = [d for d in sample_data if 'VALID' in d[0]]
    
    # Insert data
    for record in sample_data:
        sql = """
            INSERT INTO fraud_detection.quality_check_transactions 
            (transaction_id, user_id, amount, risk_score, status)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (transaction_id) DO NOTHING
        """
        hook.run(sql, parameters=record)
    
    print(f"✅ Generated {len(sample_data)} sample transactions")
    return {'count': len(sample_data)}

def check_completeness(**context):
    """Check for NULL values in critical fields"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    print("\n" + "="*60)
    print("🔍 DATA QUALITY CHECK 1: COMPLETENESS")
    print("="*60)
    
    checks_failed = []
    
    # Check for NULL user_id
    null_users = hook.get_first("""
        SELECT COUNT(*) 
        FROM fraud_detection.quality_check_transactions 
        WHERE user_id IS NULL
    """)[0]
    
    print(f"NULL user_id: {null_users} records")
    if null_users > 0:
        checks_failed.append(f"Found {null_users} transactions with NULL user_id")
    
    # Check for NULL risk_score
    null_risk = hook.get_first("""
        SELECT COUNT(*) 
        FROM fraud_detection.quality_check_transactions 
        WHERE risk_score IS NULL
    """)[0]
    
    print(f"NULL risk_score: {null_risk} records")
    if null_risk > 0:
        checks_failed.append(f"Found {null_risk} transactions with NULL risk_score")
    
    # Check for NULL amount
    null_amount = hook.get_first("""
        SELECT COUNT(*) 
        FROM fraud_detection.quality_check_transactions 
        WHERE amount IS NULL
    """)[0]
    
    print(f"NULL amount: {null_amount} records")
    if null_amount > 0:
        checks_failed.append(f"Found {null_amount} transactions with NULL amount")
    
    return {
        'check_name': 'completeness',
        'passed': len(checks_failed) == 0,
        'issues': checks_failed
    }

def check_validity(**context):
    """Check for values outside expected ranges"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    print("\n" + "="*60)
    print("🔍 DATA QUALITY CHECK 2: VALIDITY")
    print("="*60)
    
    checks_failed = []
    
    # Check for negative amounts
    negative_amounts = hook.get_first("""
        SELECT COUNT(*) 
        FROM fraud_detection.quality_check_transactions 
        WHERE amount < 0
    """)[0]
    
    print(f"Negative amounts: {negative_amounts} records")
    if negative_amounts > 0:
        checks_failed.append(f"Found {negative_amounts} transactions with negative amounts")
    
    # Check for invalid risk scores (should be 0-1)
    invalid_risk = hook.get_first("""
        SELECT COUNT(*) 
        FROM fraud_detection.quality_check_transactions 
        WHERE risk_score < 0 OR risk_score > 1
    """)[0]
    
    print(f"Invalid risk scores: {invalid_risk} records")
    if invalid_risk > 0:
        checks_failed.append(f"Found {invalid_risk} transactions with risk_score outside 0-1 range")
    
    # Check for invalid status values
    invalid_status = hook.get_first("""
        SELECT COUNT(*) 
        FROM fraud_detection.quality_check_transactions 
        WHERE status NOT IN ('approved', 'flagged', 'blocked')
    """)[0]
    
    print(f"Invalid status: {invalid_status} records")
    if invalid_status > 0:
        checks_failed.append(f"Found {invalid_status} transactions with invalid status")
    
    return {
        'check_name': 'validity',
        'passed': len(checks_failed) == 0,
        'issues': checks_failed
    }

def check_uniqueness(**context):
    """Check for duplicate records"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    print("\n" + "="*60)
    print("🔍 DATA QUALITY CHECK 3: UNIQUENESS")
    print("="*60)
    
    checks_failed = []
    
    # Check for duplicate transaction IDs
    duplicates = hook.get_first("""
        SELECT COUNT(*) - COUNT(DISTINCT transaction_id)
        FROM fraud_detection.quality_check_transactions
    """)[0]
    
    print(f"Duplicate transaction_ids: {duplicates} records")
    if duplicates > 0:
        checks_failed.append(f"Found {duplicates} duplicate transaction IDs")
    
    return {
        'check_name': 'uniqueness',
        'passed': len(checks_failed) == 0,
        'issues': checks_failed
    }

def evaluate_quality_checks(**context):
    """Evaluate all quality checks and decide next step"""
    ti = context['ti']
    
    # Pull results from all checks
    completeness = ti.xcom_pull(task_ids='check_completeness')
    validity = ti.xcom_pull(task_ids='check_validity')
    uniqueness = ti.xcom_pull(task_ids='check_uniqueness')
    
    checks = [completeness, validity, uniqueness]
    
    print("\n" + "="*60)
    print("📊 DATA QUALITY SUMMARY")
    print("="*60)
    
    all_passed = True
    total_issues = []
    
    for check in checks:
        status = "✅ PASS" if check['passed'] else "❌ FAIL"
        print(f"{check['check_name'].upper()}: {status}")
        
        if not check['passed']:
            all_passed = False
            total_issues.extend(check['issues'])
            for issue in check['issues']:
                print(f"  ⚠️ {issue}")
    
    print("="*60)
    
    if all_passed:
        print("✅ ALL QUALITY CHECKS PASSED - Data is ready for production")
        return 'quality_passed'
    else:
        print(f"❌ QUALITY CHECKS FAILED - Found {len(total_issues)} issues")
        return 'quality_failed'

def send_quality_alert(**context):
    """Send alert when quality checks fail"""
    ti = context['ti']
    
    completeness = ti.xcom_pull(task_ids='check_completeness')
    validity = ti.xcom_pull(task_ids='check_validity')
    uniqueness = ti.xcom_pull(task_ids='check_uniqueness')
    
    print("\n" + "="*60)
    print("🚨 DATA QUALITY ALERT")
    print("="*60)
    print("TO: data-engineering@company.com")
    print("SUBJECT: Data Quality Issues Detected in Fraud Detection Pipeline")
    print("\nISSUES FOUND:")
    
    all_issues = []
    for check in [completeness, validity, uniqueness]:
        if not check['passed']:
            all_issues.extend(check['issues'])
    
    for i, issue in enumerate(all_issues, 1):
        print(f"  {i}. {issue}")
    
    print("\nACTION REQUIRED: Please investigate and fix data quality issues")
    print("="*60)

def proceed_with_processing(**context):
    """Continue with downstream processing (quality passed)"""
    print("\n✅ Quality checks passed - continuing with fraud analysis")
    print("   → Running ML models")
    print("   → Generating reports")
    print("   → Sending notifications")
    print("   → Updating dashboards")

with DAG(
    'day6_exercise4_data_quality',
    default_args={
        'owner': 'Xiaowan(Abby) Liu',
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    description='Exercise: Data quality validation for fraud detection',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['day6', 'exercise', 'data-quality'],
) as dag:
    
    # Setup: Create table
    create_table = SQLExecuteQueryOperator(
        task_id='create_quality_check_table',
        conn_id='postgres_default',
        sql="""
            CREATE SCHEMA IF NOT EXISTS fraud_detection;
            
            DROP TABLE IF EXISTS fraud_detection.quality_check_transactions;
            
            CREATE TABLE fraud_detection.quality_check_transactions (
                id SERIAL PRIMARY KEY,
                transaction_id VARCHAR(50) UNIQUE NOT NULL,
                user_id VARCHAR(50),
                amount DECIMAL(10, 2),
                risk_score DECIMAL(3, 2),
                status VARCHAR(20),
                created_at TIMESTAMP DEFAULT NOW()
            );
        """
    )
    
    # Generate sample data
    generate_data = PythonOperator(
        task_id='generate_sample_data',
        python_callable=generate_sample_data
    )
    
    # Quality checks (run in parallel)
    check_complete = PythonOperator(
        task_id='check_completeness',
        python_callable=check_completeness
    )
    
    check_valid = PythonOperator(
        task_id='check_validity',
        python_callable=check_validity
    )
    
    check_unique = PythonOperator(
        task_id='check_uniqueness',
        python_callable=check_uniqueness
    )
    
    # Evaluate results and branch
    evaluate = BranchPythonOperator(
        task_id='evaluate_quality_checks',
        python_callable=evaluate_quality_checks
    )
    
    # Quality passed - continue processing
    quality_passed = PythonOperator(
        task_id='quality_passed',
        python_callable=proceed_with_processing
    )
    
    # Quality failed - send alert
    quality_failed = PythonOperator(
        task_id='quality_failed',
        python_callable=send_quality_alert
    )
    
    # End marker
    end = EmptyOperator(
        task_id='end',
        trigger_rule='none_failed_min_one_success'
    )
    
    # Dependencies
    create_table >> generate_data >> [check_complete, check_valid, check_unique]
    [check_complete, check_valid, check_unique] >> evaluate
    evaluate >> [quality_passed, quality_failed] >> end