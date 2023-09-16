from datetime import datetime
from airflow.operators.python import BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG

def check_minute():
    current_minute = datetime.now().minute
    if current_minute % 2 == 0:
        return 'branch_a'  # Go to Branch A for even minutes
    else:
        return 'branch_false'  # Go to Branch False for odd minutes

default_args = {
    "depends_on_past": False,
    "email": ["mookinthuorn@gmail.com"],
}

with DAG(
    "import",
    default_args=default_args,
    description="A simple tutorial DAG",
    schedule_interval=None,  
    start_date=days_ago(1),  # You can set the start date to an appropriate value
    tags=["example"],
) as dag:
    A_task = DummyOperator(task_id='branch_a', dag=dag)
    B_task = DummyOperator(task_id='branch_false', dag=dag)

    branch_task = BranchPythonOperator(
        task_id='branching',
        python_callable=check_minute,
        dag=dag,
    )

    branch_task >> A_task
    branch_task >> B_task
