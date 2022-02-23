from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Nikhil',
    'start_date': days_ago(1)
}

with DAG('process', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:

    extracting = BashOperator(task_id='extracting', bash_command='sleep 10')

    with TaskGroup('processing_task_group') as processing_group:

        task1 = BashOperator(task_id='processing_1', bash_command='sleep 10')

        task2 = BashOperator(task_id='processing_2', bash_command='slekep 10')

        task3 = BashOperator(task_id='processing_3', bash_command='sleep 10')

    saving = BashOperator(task_id='saving', bash_command='sleep 10', trigger_rule='one_success')

    trigger_save = TriggerDagRunOperator(
        task_id='trigger_save',
        trigger_dag_id='save'
    )

    extracting >> processing_group >> saving >> trigger_save