"""
XCom Demo DAG
=============
Demonstrates how to pass data between tasks using XComs

Flow: generate_number → double_number → print_result
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'Xiaowan(Abby) Liu',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    dag_id='xcom_demo',
    default_args=default_args,
    description='XCom demonstration - passing data between tasks',
    schedule=None,  # Manual trigger only
    start_date=datetime(2025, 11, 1),
    catchup=False,
    tags=['learning', 'xcom', 'day3'],
)


# ============================================================================
# TASK FUNCTIONS
# ============================================================================

def generate_number():
    """
    Task 1: Generate a random number
    Returns the number (automatically stored in XCom)
    """
    import random
    number = random.randint(1, 100)
    print(f"Generated number: {number}")
    return number  # Automatically pushed to XCom with key 'return_value'


def double_number(**context):
    """
    Task 2: Get number from previous task and double it
    """
    # Get task instance from context
    ti = context['task_instance']
    
    # Pull data from previous task
    original_number = ti.xcom_pull(task_ids='generate_number')
    
    print(f"Original number: {original_number}")
    
    # Double it
    doubled = original_number * 2
    print(f"Doubled number: {doubled}")
    
    # Return the doubled value
    return doubled


def print_result(**context):
    """
    Task 3: Print both numbers
    """
    ti = context['task_instance']
    
    # Pull from BOTH previous tasks
    original = ti.xcom_pull(task_ids='generate_number')
    doubled = ti.xcom_pull(task_ids='double_number')
    
    print("=" * 50)
    print("FINAL RESULTS")
    print("=" * 50)
    print(f"Original number: {original}")
    print(f"Doubled number: {doubled}")
    print(f"Difference: {doubled - original}")
    print("=" * 50)


def task_with_multiple_outputs(**context):
    ti = context['task_instance']
    
    # Push multiple values with different keys
    ti.xcom_push(key='count', value=100)
    ti.xcom_push(key='average', value=25.5)
    ti.xcom_push(key='status', value='success')
    
    # Also return (stored with key='return_value')
    return {'summary': 'Processed 100 records'}

def task_that_pulls(**context):
    ti = context['task_instance']
    
    # Pull specific keys
    count = ti.xcom_pull(key='count', task_ids='task_with_multiple_outputs')
    avg = ti.xcom_pull(key='average', task_ids='task_with_multiple_outputs')
    status = ti.xcom_pull(key='status', task_ids='task_with_multiple_outputs')
    
    # Pull return value
    summary = ti.xcom_pull(task_ids='task_with_multiple_outputs')

    print(f"Count: {count}, Average: {avg}, Status: {status}")
    print(f"Summary: {summary}")


# ============================================================================
# TASK DEFINITIONS
# ============================================================================

task1 = PythonOperator(
    task_id='generate_number',
    python_callable=generate_number,
    dag=dag,
)

task2 = PythonOperator(
    task_id='double_number',
    python_callable=double_number,
    dag=dag,
)

task3 = PythonOperator(
    task_id='print_result',
    python_callable=print_result,
    dag=dag,
)

task_push = PythonOperator(
    task_id = 'task_with_multiple_outputs',
    python_callable=task_with_multiple_outputs,
    dag=dag,
)

task_pull = PythonOperator(
    task_id = 'task_pull',
    python_callable=task_that_pulls,
    dag=dag,
)

# ============================================================================
# DEPENDENCIES
# ============================================================================

task1 >> task2 >> task3
task_push >> task_pull