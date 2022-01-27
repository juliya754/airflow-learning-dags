"""
First DAG consist of 4 operations:
show current directry,
paused or 5 sec,
paused for 20 sec,
print "completed" using bash operator
"""

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

our_first_dag = DAG(
    "first_dag",
    start_date=days_ago(0, 0, 0, 0, 0),
    tags=['bash']
)

operation_1 = BashOperator(
    bash_command="pwd",
    dag=our_first_dag,
    task_id='operation_1'
)

operation_2 = BashOperator(
    bash_command="sleep 5",
    dag=our_first_dag,
    task_id='operation_2'
)

operation_3 = BashOperator(
    bash_command="sleep 20",
    dag=our_first_dag,
    task_id='operation_3'
)

operation_4 = BashOperator(
    bash_command="""echo "completed" """,
    dag=our_first_dag,
    task_id='operation_4'
)

operation_1 >> [operation_2, operation_3]

[operation_2, operation_3] >> operation_4
