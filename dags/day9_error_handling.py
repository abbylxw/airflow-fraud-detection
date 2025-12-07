import pendulum
import random
from datetime import timedelta
from airflow.sdk import dag, task


def on_failure_callback(context):
    """Called when a task fails."""
    task_id = context['task_instance'].task_id
    dag_id = context['task_instance'].dag_id
    logical_date = context['logical_date']
    error = context.get('exception', 'Unknown error')
    
    print(f"""
    🚨 TASK FAILURE ALERT 🚨
    ========================
    DAG: {dag_id}
    Task: {task_id}
    Logical Date: {logical_date}
    Error: {error}
    
    Action: Check logs and investigate!
    """)
    
    # In production, you'd send to Slack, PagerDuty, email, etc.
    # Example: requests.post(slack_webhook_url, json={"text": message})


def on_success_callback(context):
    """Called when a task succeeds."""
    task_id = context['task_instance'].task_id
    print(f"✅ Task {task_id} completed successfully!")

def on_retry_callback(context):
    task_id = context['task_instance'].task_id
    attempt = context['task_instance'].try_number
    print(f"🔄 Retrying {task_id} - attempt #{attempt}")


@dag(
    'day9_error_handling',
    start_date=pendulum.datetime(2025, 11, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=['day9', 'error-handling'],
    default_args={
        'on_failure_callback': on_failure_callback,  # Applied to all tasks
    }
)
def error_handling_demo():
    
    @task(
        retries=2,
        retry_delay=timedelta(seconds=5),
        on_success_callback=on_success_callback,  # Task-specific callback
        on_retry_callback = on_retry_callback,
    )
    def flaky_api_call():
        """Simulates an unreliable API."""
        if random.random() < 0.7:
            raise Exception("API timeout - server not responding")
        
        print("✅ API call succeeded!")
        return {"status": "success", "data": [1, 2, 3]}
    
    @task
    def process_data(api_response):
        print(f"📊 Processing: {api_response}")
        return "processed"
    
    @task(
        on_success_callback=on_success_callback,
        on_retry_callback = on_retry_callback,
    )
    def always_succeeds():
        print("🎯 This task always works!")
        return "done"
    
    data = flaky_api_call()
    process_data(data)
    always_succeeds()

error_handling_demo()