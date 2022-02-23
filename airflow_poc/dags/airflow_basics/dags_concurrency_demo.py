from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago

default_args = {
    'start_date': datetime(2022, 1, 1)
}

with DAG('dags_concurrency_demo', schedule_interval='@daily', default_args=default_args, catchup=False, concurrency=7) as dag:
    with TaskGroup('processing_task_group') as processing_group:
        for i in range(20):
            BashOperator(task_id=f'processing_{i}', bash_command='sleep 10')

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    start >> processing_group >> end
