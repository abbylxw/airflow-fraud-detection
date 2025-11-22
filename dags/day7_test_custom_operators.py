import pendulum
from datetime import timedelta
from airflow.sdk import dag, task
from operators.fraud_api_operator import FraudAPIOperator


@dag(
    'day7_test_custom_operator',
    start_date=pendulum.datetime(2025, 11, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=['day7', 'custom-operator', 'test'],
)
def test_custom_operator():
    
    # Test 1: Successful API call
    fetch_data = FraudAPIOperator(
        task_id='fetch_posts',
        endpoint='https://jsonplaceholder.typicode.com/posts',
        method='GET',
        timeout=10,
        max_retries=3
    )
    
    # Test 2: API call that will retry (bad endpoint)
    fetch_bad = FraudAPIOperator(
        task_id='fetch_bad_endpoint',
        endpoint='https://jsonplaceholder.typicode.com/nonexistent',
        method='GET',
        timeout=5,
        max_retries=2,
        retry_delay=2
    )
    
    @task
    def process_response(**context):
        ti = context['ti']
        api_response = ti.xcom_pull(task_ids='fetch_posts')
        """Process the API response"""
        print(f"Status: {api_response['status']}")
        print(f"Attempts: {api_response['attempts']}")
        print(f"Data items: {len(api_response['data'])}")
        return f"Processed {len(api_response['data'])} items"
    
    # Workflow
    fetch_data >> process_response()

test_custom_operator()