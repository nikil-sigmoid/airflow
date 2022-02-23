# Reference: https://marclamberti.com/blog/airflow-branchpythonoperator/


from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'Nikhil',
    'start_date': datetime(2020, 1, 1)
}


def choose_best_model_util(**kwargs):
    print(kwargs["params"])
    score = 0.8
    if score > 0.75:
        return ['task_group.accurate', 'task_group.task_group_task_2']

    return 'inaccurate'


with DAG('branch_python_op_task_group_demo', schedule_interval='@daily', default_args=default_args) as dag:

    best_model = BranchPythonOperator(
        task_id='choose_best_model',
        python_callable=choose_best_model_util
    )

    with TaskGroup('task_group') as task_group:
        task1 = BashOperator(task_id='task_group_task_1', bash_command='sleep 5')

        task2 = BashOperator(task_id='task_group_task_2', bash_command='sleep 5')

        task3 = BashOperator(task_id='task_group_task_3', bash_command='sleep 5')

        task4 = BashOperator(task_id='task_group_task_4', bash_command='sleep 5')

        task5 = BashOperator(task_id='task_group_task_5', bash_command='sleep 5')

        accurate = DummyOperator(task_id='accurate')

    inaccurate = DummyOperator(
        task_id='inaccurate'
    )

    start = DummyOperator(
        task_id='start'
    )

    saving = DummyOperator(task_id='saving')

    start >> best_model >> [task_group, inaccurate]
    accurate >> saving