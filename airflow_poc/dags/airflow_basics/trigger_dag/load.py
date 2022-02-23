from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'Nikhil',
}

with DAG(
    dag_id='load',
    default_args=args,
    schedule_interval='0 0 * * *',
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60)
) as dag:
    task1 = BashOperator(task_id='dag-1_first_task', bash_command="sleep 5", dag=dag)

    trigger_process = TriggerDagRunOperator(
        task_id='trigger_process',
        trigger_dag_id='process'
    )

    task1 >> trigger_process

