"""
My First Airflow DAG
====================
Author: Xiaowan(Abby) Liu
Created: 2025-10-31
Description: Learning Airflow basics - task dependencies and simple operations

This DAG demonstrates:
- Basic task creation
- Task dependencies
- PythonOperator and BashOperator
- Simple data flow
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# ============================================================================
# DEFAULT ARGUMENTS
# ============================================================================
# These apply to all tasks in the DAG unless overridden

default_args = {
    'owner': 'Xiaowan(Abby) Liu',  # Change this to your name
    'depends_on_past': False,  # Tasks don't depend on previous run's success
    'email_on_failure': False,  # Don't send email if task fails (no email setup yet)
    'email_on_retry': False,
    'retries': 1,  # Retry failed tasks once
    'retry_delay': timedelta(minutes=5),  # Wait 5 minutes before retry
}

# ============================================================================
# DAG DEFINITION
# ============================================================================

dag = DAG(
    dag_id='my_first_dag',  # Unique identifier for this DAG
    default_args=default_args,
    description='My first Airflow DAG - learning the basics',
    schedule=timedelta(days=1),  # Run once per day
    start_date=datetime(2025, 10, 31),  # When to start scheduling
    catchup=False,  # Don't run for past dates
    tags=['learning', 'basics', 'first-dag'],  # Tags for organization
)

# ============================================================================
# PYTHON FUNCTIONS (Task Logic)
# ============================================================================

def print_welcome():
    """
    Simple function that prints a welcome message
    """
    print("=" * 60)
    print("🚀 Welcome to Airflow!")
    print("=" * 60)
    print("This is my first task running in Airflow")
    print("Task execution time:", datetime.now())
    print("=" * 60)
    return "Welcome task completed!"


def print_date():
    """
    Function that prints current date and time information
    """
    now = datetime.now()
    print("=" * 60)
    print("📅 Current Date & Time Information")
    print("=" * 60)
    print(f"Current datetime: {now}")
    print(f"Date: {now.strftime('%Y-%m-%d')}")
    print(f"Time: {now.strftime('%H:%M:%S')}")
    print(f"Day of week: {now.strftime('%A')}")
    print("=" * 60)
    return now


def print_random_quote():
    """
    Function that prints a random motivational quote
    """
    import random
    
    quotes = [
        "The best time to start was yesterday. The next best time is now.",
        "Data is the new oil. - Clive Humby",
        "In God we trust. All others must bring data. - W. Edwards Deming",
        "Without data, you're just another person with an opinion.",
        "Learning never exhausts the mind. - Leonardo da Vinci"
    ]
    
    quote = random.choice(quotes)
    print("=" * 60)
    print("💡 Quote of the Day")
    print("=" * 60)
    print(f"\n{quote}\n")
    print("=" * 60)
    return quote


# ============================================================================
# TASK DEFINITIONS
# ============================================================================

# Task 1: Print welcome message using Bash
start_task = BashOperator(
    task_id='start',  # Unique task identifier
    bash_command='echo "🎬 Starting My First DAG..." && date',
    dag=dag,
)

# Task 2: Print welcome using Python
welcome_task = PythonOperator(
    task_id='print_welcome',
    python_callable=print_welcome,  # Function to call (no parentheses!)
    dag=dag,
)

# Task 3: Print current date/time
date_task = PythonOperator(
    task_id='print_date',
    python_callable=print_date,
    dag=dag,
)

# Task 4: Print random quote
quote_task = PythonOperator(
    task_id='print_quote',
    python_callable=print_random_quote,
    dag=dag,
)

# Task 5: End task using Bash
end_task = BashOperator(
    task_id='end',
    bash_command='echo "✅ DAG Completed Successfully!" && echo "Total time: $(($(date +%s) - $(date +%s)))"',
    dag=dag,
)

# ============================================================================
# TASK DEPENDENCIES (Execution Order)
# ============================================================================
# This defines the order in which tasks execute

# Method 1: Chain style (readable for linear flow)
start_task >> welcome_task >> date_task >> quote_task >> end_task

# This creates the following flow:
#
#  start → welcome → date → quote → end
#
# Each task waits for the previous one to complete successfully

# Alternative ways to write dependencies:
#
# Method 2: Using set_upstream/set_downstream
# welcome_task.set_upstream(start_task)
# date_task.set_upstream(welcome_task)
#
# Method 3: Using list notation for multiple dependencies
# end_task.set_upstream([date_task, quote_task])  # Wait for both
#
# Method 4: Reverse direction
# start_task << welcome_task  # welcome comes before start


