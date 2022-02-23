# taskgroup.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'start_date': days_ago(1)
}

id_list_step1 = ["ds-1", "ds-2", "ds-3"]

with DAG('sfmc_pipeline_step1', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
    with TaskGroup('step1_sftp_to_gcs') as sftp_to_gcs_bucket_step1:
        for id in id_list_step1:
            task1 = BashOperator(task_id=id, bash_command='sleep 10')

    with TaskGroup('csv_to_parquet_snappy') as csv_to_parquet_snappy:
        for id in id_list_step1:
            task1 = BashOperator(task_id=id, bash_command='sleep 10')

    gcs_to_snowflow = BashOperator(task_id='gcs_to_snowflow', bash_command='sleep 10')

    trigger_step2 = TriggerDagRunOperator(
        task_id='trigger_step2',
        trigger_dag_id='sfmc_pipeline_step2'
    )

    sftp_to_gcs_bucket_step1 >> csv_to_parquet_snappy >> gcs_to_snowflow >> trigger_step2