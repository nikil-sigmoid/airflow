from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(seconds=3),
    'start_date': days_ago(4)
}

with DAG(
    dag_id='task_level_retry',
    default_args=args,
    schedule_interval='0 0 * * *',
    params={"Name": "Airflow1"},
    # start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),

) as dag:
    start = BashOperator(task_id='start', bash_command="echo starting", dag=dag)
    task1 = BashOperator(task_id='first_task', bash_command="echo starting", dag=dag)
    task2 = BashOperator(task_id='second_task', bash_command="sleep 5;fdw ", dag=dag)
    task3 = BashOperator(task_id='third_task', bash_command="sleep 5; dsfsdf", dag=dag, retries=5)
    end = BashOperator(task_id='end', bash_command="echo ending", dag=dag)
    start >> task1 >> task2 >> task3 >> end