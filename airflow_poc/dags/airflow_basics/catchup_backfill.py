from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
# from datetime import date,
from airflow.utils.dates import days_ago

# today = date.today()


default_args = {
    'owner': "Nikhil",
    'retries': 2,
    'retry_delay': timedelta(seconds=5),
    'depends_on_past': False,
}

with DAG(
    dag_id="catchup_backfill",
    default_args=default_args,
    start_date=datetime(2021, 12, 28),
    # start_date=days_ago(5),
    # end_date=datetime(2022, 1, 2),
    schedule_interval='@daily',
    catchup=True
) as dag:

    task_1 = BashOperator(
        task_id='task_1',
        bash_command='echo simple bash command {{ds}}'
    )


# airflow dags backfill -s 2022-01-01 -e 2022-01-04 catchup_backfill