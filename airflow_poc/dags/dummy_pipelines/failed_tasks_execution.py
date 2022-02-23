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
    'start_date': days_ago(2)
}

TASK_IDS = ['ds_1', 'ds_2', 'ds_3', 'ds_4', 'ds_5']
KEY = "task_ids"


def extract_tasks_ids(kwargs, id):
    conf = kwargs['dag_run'].conf
    if KEY not in conf.keys():
        return [f'ds_task_group{id}.{task}' for task in TASK_IDS]
    else:
        return [f'ds_task_group{id}.{task}' for task in conf[KEY]]


def _choose_tasks_1(**kwargs):
    return extract_tasks_ids(kwargs, 1)


def _choose_tasks_2(**kwargs):
    return extract_tasks_ids(kwargs, 2)


def _choose_tasks_3(**kwargs):
    return extract_tasks_ids(kwargs, 3)


with DAG('failed_tasks_execution', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:

    with TaskGroup('ds_task_group1') as ds_group1:
        task1 = BashOperator(task_id='ds_1', bash_command='sleep 5', trigger_rule='one_success')
        task2 = BashOperator(task_id='ds_2', bash_command='sleep 5', trigger_rule='one_success')
        task3 = BashOperator(task_id='ds_3', bash_command='sleep 5', trigger_rule='one_success')
        task4 = BashOperator(task_id='ds_4', bash_command='sleep 5', trigger_rule='one_success')
        task5 = BashOperator(task_id='ds_5', bash_command='sleep 5', trigger_rule='one_success')

    with TaskGroup('ds_task_group2') as ds_group2:
        task1 = BashOperator(task_id='ds_1', bash_command='sleep 5', trigger_rule='one_success')
        task2 = BashOperator(task_id='ds_2', bash_command='sleep 5', trigger_rule='one_success')
        task3 = BashOperator(task_id='ds_3', bash_command='sleep 5', trigger_rule='one_success')
        task4 = BashOperator(task_id='ds_4', bash_command='sleep 5', trigger_rule='one_success')
        task5 = BashOperator(task_id='ds_5', bash_command='sleep 5', trigger_rule='one_success')

    with TaskGroup('ds_task_group3') as ds_group3:
        task1 = BashOperator(task_id='ds_1', bash_command='sleep 5', trigger_rule='one_success')
        task2 = BashOperator(task_id='ds_2', bash_command='sleep 5', trigger_rule='one_success')
        task3 = BashOperator(task_id='ds_3', bash_command='sleep 5', trigger_rule='one_success')
        task4 = BashOperator(task_id='ds_4', bash_command='sleep 5', trigger_rule='one_success')
        task5 = BashOperator(task_id='ds_5', bash_command='sleep 5', trigger_rule='one_success')

    choose_task_1 = BranchPythonOperator(task_id='choose_tasks_1', python_callable=_choose_tasks_1)
    choose_task_2 = BranchPythonOperator(task_id='choose_tasks_2', python_callable=_choose_tasks_2, trigger_rule='one_success')
    choose_task_3 = BranchPythonOperator(task_id='choose_tasks_3', python_callable=_choose_tasks_3, trigger_rule='one_success')

    skip_task1 = DummyOperator(task_id='skip_task1')
    skip_task2 = DummyOperator(task_id='skip_task2')
    skip_task3 = DummyOperator(task_id='skip_task3')

    loading = DummyOperator(task_id='loading', trigger_rule='one_success')

    choose_task_1 >> [ds_group1, skip_task1]
    ds_group1 >> choose_task_2 >> [ds_group2, skip_task2]
    ds_group2 >> choose_task_3 >> [ds_group3, skip_task3]
    ds_group3 >> loading