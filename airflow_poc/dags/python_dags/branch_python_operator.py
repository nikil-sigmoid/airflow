from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2020, 1, 1),
    'owner': 'Nikhil'
}

def _choose_best_model(ti):
    score = 0.80
    if score > 0.75:
        return 'grade_a'

    return 'grade_b'

with DAG('branch_python_operator', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:

    choose_best_model = BranchPythonOperator(
        task_id='choose_best_model',
        python_callable=_choose_best_model
    )

    grade_a = DummyOperator(
        task_id='grade_a'
    )

    grade_b = DummyOperator(
        task_id='grade_b'
    )

    saving = DummyOperator(
        task_id='saving',
        trigger_rule='none_failed_or_skipped'
    )

    tasktemp1 = DummyOperator(
        task_id='tasktemp1',
        trigger_rule='none_failed_or_skipped'
    )


    tasktemp2 = DummyOperator(
        task_id='tasktemp2',
        trigger_rule='none_failed_or_skipped'
    )


    choose_best_model >> [grade_a, grade_b]
    grade_a >> [tasktemp1, tasktemp2] >> saving
    grade_b >> saving