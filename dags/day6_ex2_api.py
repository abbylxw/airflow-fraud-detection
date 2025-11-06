from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json

def process_user_data(**context):
    """Process user data from API"""
    ti = context['ti']
    
    # Get data from multiple API calls
    users_response = ti.xcom_pull(task_ids='fetch_users')
    posts_response = ti.xcom_pull(task_ids='fetch_posts')
    
    users = json.loads(users_response) if users_response else []
    posts = json.loads(posts_response) if posts_response else []
    
    print(f"📊 API Data Summary:")
    print(f"   Total users: {len(users)}")
    print(f"   Total posts: {len(posts)}")
    
    # Simulate fraud risk scoring based on activity
    print(f"\n🔍 Fraud Risk Analysis:")
    
    # Count posts per user
    user_posts = {}
    for post in posts:
        user_id = post.get('userId')
        user_posts[user_id] = user_posts.get(user_id, 0) + 1
    
    # Flag suspicious activity
    suspicious_users = []
    for user in users[:5]:  # Analyze first 5 users
        user_id = user.get('id')
        post_count = user_posts.get(user_id, 0)
        
        # Simple fraud logic: too many posts might be spam/fraud
        risk_score = min(post_count / 20.0, 0.99)  # Normalize to 0-1
        
        print(f"   User {user_id} ({user.get('name')}): {post_count} posts, Risk: {risk_score:.2f}")
        
        if risk_score > 0.5:
            suspicious_users.append({
                'user_id': user_id,
                'name': user.get('name'),
                'risk_score': risk_score,
                'reason': f'{post_count} posts (high activity)'
            })
    
    if suspicious_users:
        print(f"\n🚨 {len(suspicious_users)} suspicious users detected!")
    
    return {'users': len(users), 'posts': len(posts), 'suspicious': len(suspicious_users)}

def fetch_with_hook(**context):
    """Example: Using HttpHook for more control"""
    hook = HttpHook(http_conn_id='jsonplaceholder_api', method='GET')
    
    # Fetch specific user
    response = hook.run(endpoint='/users/1')
    user_data = json.loads(response.text)
    
    print(f"📥 Fetched user via Hook:")
    print(f"   Name: {user_data.get('name')}")
    print(f"   Email: {user_data.get('email')}")
    print(f"   Company: {user_data.get('company', {}).get('name')}")
    
    return user_data

with DAG(
    'day6_ex2_api',
    default_args={
        'owner': 'Xiaowan(Abby) Liu',
        'retries': 2,
        'retry_delay': timedelta(seconds=30),
    },
    description='Exercise: Fetch fraud data from API',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['day6', 'exercise', 'api'],
) as dag:
    
    # Fetch users from API
    fetch_users = SimpleHttpOperator(
        task_id='fetch_users',
        http_conn_id='jsonplaceholder_api',
        endpoint='/users',
        method='GET',
        headers={'Content-Type': 'application/json'},
        log_response=True
    )
    
    # Fetch posts (user activity)
    fetch_posts = SimpleHttpOperator(
        task_id='fetch_posts',
        http_conn_id='jsonplaceholder_api',
        endpoint='/posts',
        method='GET',
        headers={'Content-Type': 'application/json'},
        log_response=True
    )
    
    # Process combined data
    process = PythonOperator(
        task_id='process_api_data',
        python_callable=process_user_data
    )
    
    # Example using Hook
    fetch_hook = PythonOperator(
        task_id='fetch_with_hook_example',
        python_callable=fetch_with_hook
    )
    
    [fetch_users, fetch_posts] >> process >> fetch_hook