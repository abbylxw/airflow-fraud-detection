from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import random

def extract_and_transform(**context):
    """
    EXTRACT: Get data from API
    TRANSFORM: Calculate fraud risk scores
    """
    ti = context['ti']
    
    # Extract - Get users from API
    users_response = ti.xcom_pull(task_ids='extract_users_from_api')
    posts_response = ti.xcom_pull(task_ids='extract_posts_from_api')
    
    users = json.loads(users_response) if users_response else []
    posts = json.loads(posts_response) if posts_response else []
    
    print(f"📥 EXTRACT: Fetched {len(users)} users and {len(posts)} posts")
    
    # Transform - Calculate fraud risk scores
    print(f"🔄 TRANSFORM: Calculating fraud risk scores...")
    
    # Count posts per user
    user_activity = {}
    for post in posts:
        user_id = post.get('userId')
        user_activity[user_id] = user_activity.get(user_id, 0) + 1
    
    # Calculate risk scores for each user
    fraud_records = []
    for user in users:
        user_id = user.get('id')
        activity_count = user_activity.get(user_id, 0)
        
        # Risk scoring logic
        # High activity = higher risk (spam/bot behavior)
        activity_risk = min(activity_count / 15.0, 0.6)
        
        # Add some randomness for other factors
        behavioral_risk = random.uniform(0, 0.4)
        
        total_risk = min(activity_risk + behavioral_risk, 0.99)
        
        # Determine status based on risk
        if total_risk >= 0.8:
            status = 'blocked'
        elif total_risk >= 0.5:
            status = 'flagged'
        else:
            status = 'approved'
        
        fraud_record = {
            'user_id': f"API_USER_{user_id}",
            'username': user.get('username', 'unknown'),
            'email': user.get('email', 'unknown'),
            'activity_count': activity_count,
            'risk_score': round(total_risk, 2),
            'status': status,
            'company': user.get('company', {}).get('name', 'Unknown')
        }
        
        fraud_records.append(fraud_record)
        
        print(f"   User {user_id}: Activity={activity_count}, Risk={fraud_record['risk_score']}, Status={status}")
    
    print(f"✅ TRANSFORM: Processed {len(fraud_records)} fraud records")
    
    # Return transformed data
    return fraud_records

def load_to_database(**context):
    """
    LOAD: Insert transformed data into PostgreSQL
    """
    ti = context['ti']
    fraud_records = ti.xcom_pull(task_ids='transform_data')
    
    if not fraud_records:
        print("⚠️ No data to load!")
        return
    
    print(f"📤 LOAD: Inserting {len(fraud_records)} records into database...")
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Insert each record
    inserted_count = 0
    for record in fraud_records:
        sql = """
            INSERT INTO fraud_detection.user_risk_scores 
            (user_id, username, email, activity_count, risk_score, status, company)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE SET
                activity_count = EXCLUDED.activity_count,
                risk_score = EXCLUDED.risk_score,
                status = EXCLUDED.status,
                updated_at = NOW()
        """
        
        hook.run(sql, parameters=(
            record['user_id'],
            record['username'],
            record['email'],
            record['activity_count'],
            record['risk_score'],
            record['status'],
            record['company']
        ))
        inserted_count += 1
    
    print(f"✅ LOAD: Successfully inserted/updated {inserted_count} records")
    
    # Query to verify
    verification = hook.get_records("""
        SELECT status, COUNT(*), AVG(risk_score)::NUMERIC(10,2)
        FROM fraud_detection.user_risk_scores
        GROUP BY status
    """)
    
    print(f"\n📊 DATABASE SUMMARY:")
    for row in verification:
        print(f"   {row[0]}: {row[1]} users (Avg risk: {row[2]})")

def generate_fraud_report(**context):
    """Generate fraud report from database"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    print("\n" + "="*60)
    print("📋 FRAUD DETECTION REPORT")
    print("="*60)
    
    # High-risk users
    print("\n🚨 HIGH-RISK USERS (Risk Score > 0.7):")
    high_risk = hook.get_records("""
        SELECT user_id, username, email, risk_score, status, activity_count
        FROM fraud_detection.user_risk_scores
        WHERE risk_score > 0.7
        ORDER BY risk_score DESC
        LIMIT 10
    """)
    
    for row in high_risk:
        print(f"   {row[1]} ({row[0]}): Risk={row[3]}, Status={row[4]}, Activity={row[5]}")
    
    # Status breakdown
    print("\n📊 OVERALL STATUS BREAKDOWN:")
    status_summary = hook.get_records("""
        SELECT 
            status,
            COUNT(*) as count,
            AVG(risk_score)::NUMERIC(10,2) as avg_risk,
            MIN(risk_score)::NUMERIC(10,2) as min_risk,
            MAX(risk_score)::NUMERIC(10,2) as max_risk
        FROM fraud_detection.user_risk_scores
        GROUP BY status
        ORDER BY avg_risk DESC
    """)
    
    for row in status_summary:
        print(f"   {row[0].upper():10} | Count: {row[1]:3} | Avg: {row[2]} | Range: {row[3]}-{row[4]}")
    
    # Total summary
    total = hook.get_first("""
        SELECT 
            COUNT(*) as total,
            AVG(risk_score)::NUMERIC(10,2) as avg_risk
        FROM fraud_detection.user_risk_scores
    """)
    
    print(f"\n📈 TOTAL USERS: {total[0]} | AVERAGE RISK: {total[1]}")
    print("="*60)

with DAG(
    'day6_exercise3_api_to_db',
    default_args={
        'owner': 'Xiaowan(Abby) Liu',
        'retries': 2,
        'retry_delay': timedelta(minutes=1),
    },
    description='Exercise: Complete ETL pipeline (API → Transform → Database)',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['day6', 'exercise', 'etl', 'api', 'postgres'],
) as dag:
    
    # SETUP: Create database table
    create_table = SQLExecuteQueryOperator(
        task_id='create_user_risk_table',
        conn_id='postgres_default',
        sql="""
            CREATE SCHEMA IF NOT EXISTS fraud_detection;
            
            CREATE TABLE IF NOT EXISTS fraud_detection.user_risk_scores (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(100) UNIQUE NOT NULL,
                username VARCHAR(100),
                email VARCHAR(200),
                activity_count INTEGER,
                risk_score DECIMAL(3, 2),
                status VARCHAR(20),
                company VARCHAR(200),
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            );
            
            CREATE INDEX IF NOT EXISTS idx_user_risk_score 
                ON fraud_detection.user_risk_scores(risk_score);
            CREATE INDEX IF NOT EXISTS idx_user_status 
                ON fraud_detection.user_risk_scores(status);
        """
    )
    
    # EXTRACT: Fetch users from API
    extract_users = SimpleHttpOperator(
        task_id='extract_users_from_api',
        http_conn_id='jsonplaceholder_api',
        endpoint='/users',
        method='GET',
        headers={'Content-Type': 'application/json'},
        log_response=True
    )
    
    # EXTRACT: Fetch user activity (posts)
    extract_posts = SimpleHttpOperator(
        task_id='extract_posts_from_api',
        http_conn_id='jsonplaceholder_api',
        endpoint='/posts',
        method='GET',
        headers={'Content-Type': 'application/json'},
        log_response=True
    )
    
    # TRANSFORM: Calculate fraud scores
    transform = PythonOperator(
        task_id='transform_data',
        python_callable=extract_and_transform
    )
    
    # LOAD: Insert into database
    load = PythonOperator(
        task_id='load_to_database',
        python_callable=load_to_database
    )
    
    # REPORT: Generate summary
    report = PythonOperator(
        task_id='generate_fraud_report',
        python_callable=generate_fraud_report
    )
    
    # Dependencies - Classic ETL flow
    create_table >> [extract_users, extract_posts] >> transform >> load >> report


