from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'Nikhil',
}

with DAG(
    dag_id='test_bash_operator',
    default_args=args,
    schedule_interval='0 0 * * *',
    params={"Name": "Airflow1"},
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60)
) as dag:
    task1 = BashOperator(task_id='first_task', bash_command="sleep 10", dag=dag)
    task2 = BashOperator(task_id='second_task', bash_command="sleep 10", dag=dag)
    task3 = BashOperator(task_id='third_task', bash_command="echo value: {{ dag_run.conf['Name'] }}", dag=dag)
    task4 = BashOperator(task_id='fourth_task', bash_command="echo value: {{ params.Name }}", dag=dag)

    task1 >> task2 >> task3 >> task4