from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import datetime
from airflow.utils.dates import timedelta
from airflow.operators.python import PythonOperator

import os

# os.chdir("/Users/nik/Desktop/simple_dbt_project/sde_dbt_tutorial")

default_args = {
    'owner': 'astronomer',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email': ['noreply@astronomer.io'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
dag = DAG(
    'test_dbt_dag',
    default_args=default_args,
    description='An Airflow DAG to invoke simple dbt commands',
    schedule_interval=timedelta(days=1),
)


def print_os():
    import os
    print(os.getcwd())

dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /Users/nik/dbt_sample && pwd && dbt run --select my_second_dbt_model',
    # bash_command='dbt run',
    dag=dag
)

dbt_test = PythonOperator(
    task_id='dbt_test',
    python_callable=print_os,
    # bash_command='dbt test',

    dag=dag
)

dbt_run >> dbt_test
