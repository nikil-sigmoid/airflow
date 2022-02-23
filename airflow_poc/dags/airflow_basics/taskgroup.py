# taskgroup.py
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago

# from subdag_factory import subdag_factory

default_args = {
    'owner': 'Nikhil',
    'start_date': days_ago(1)
}

id_list = ["ds1", "ds-2", "ds-3"]

with DAG('task_group', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
    start = DummyOperator(task_id='start')

    with TaskGroup('demo_task_group') as demo_task_group:
        task1 = BashOperator(task_id='task_1', bash_command='sleep 5')

        task2 = BashOperator(task_id='task_2', bash_command='sleep 5')

        task3 = BashOperator(task_id='task_3', bash_command='sleep 5')

    end = DummyOperator(task_id='end')

    start >> demo_task_group >> end
