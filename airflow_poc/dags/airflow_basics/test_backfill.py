# STEP-1

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# STEP-2

default_args = {
    'owner':'Airflow',
    'depends_on_past':False,
    'start_date':days_ago(10),
    'retries':0,

}

dag = DAG(dag_id='Dag-backfill', default_args=default_args, catchup=False, schedule_interval='@daily')

# STEP-4

dummy_start = DummyOperator(task_id='dummy_start', dag=dag)
# dummy_mid = BashOperator(task_id='third_task', bash_command="echo value: {{ dag_run.conf['Name'] }}", dag=dag)
dummy_mid = BashOperator(task_id='third_task', bash_command="echo task_2", dag=dag)
dummy_end = DummyOperator(task_id='dummy_end', dag=dag)


# STEP-5
dummy_start >> dummy_mid >> dummy_end


