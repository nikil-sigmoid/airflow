# taskgroup.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    'start_date': days_ago(1)
}

id_list_step2 = ["ds-4", "ds-5", "ds-6", "ds-7", "ds-8"]
task_group_id1 = 'sftp_to_gcs'
task_group_id2 = 'csv_to_parquet_snappy'
task_group_id3 = 'bu1'
task_group_id4 = 'bu2'
task_group_id5 = 'bu3'
task_group_id6 = 'bu1_'
task_group_id7 = 'bu2_'
task_group_id8 = 'bu3_'

task_group_id_ds1 = "sftp_to_gcs_ds1"
task_group_id_ds2 = "sftp_to_gcs_ds2"
task_group_id_ds3 = "sftp_to_gcs_ds3"

task_group_id_pq_ds1 = "csv_to_parquet_snappy_ds1"
task_group_id_pq_ds2 = "csv_to_parquet_snappy_ds2"
task_group_id_pq_ds3 = "csv_to_parquet_snappy_ds3"

bu = ["bu1", "bu2", "bu3", "bu4"]

with DAG('sfmc_pipeline_step1_alternative_approach1', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:

    with TaskGroup(f"{task_group_id_ds1}_bu") as sftp_to_gcs_ds1_bu:
        for id in bu:
            task1 = BashOperator(task_id=f'{task_group_id_ds1}_{id}', bash_command='sleep 10')

    with TaskGroup(f"{task_group_id_ds2}_bu") as sftp_to_gcs_ds2_bu:
        for id in bu:
            task1 = BashOperator(task_id=f'{task_group_id_ds2}_{id}', bash_command='sleep 10')

    with TaskGroup(f"{task_group_id_ds3}_bu") as sftp_to_gcs_ds3_bu:
        for id in bu:
            task1 = BashOperator(task_id=f'{task_group_id_ds3}_{id}', bash_command='sleep 10')

    gcs_to_snowflow = BashOperator(task_id='gcs_to_snowflow', bash_command='sleep 10')

    sftp_to_gcs_ds1 = BashOperator(task_id='sftp_to_gcs_ds1', bash_command='sleep 10')
    sftp_to_gcs_ds2 = BashOperator(task_id='sftp_to_gcs_ds2', bash_command='sleep 10')
    sftp_to_gcs_ds3 = BashOperator(task_id='sftp_to_gcs_ds3', bash_command='sleep 10')

    trigger_step2 = TriggerDagRunOperator(
        task_id='trigger_step2',
        trigger_dag_id='sfmc_pipeline_step2_alternative_approach1'
    )


    with TaskGroup(f"{task_group_id_pq_ds1}_bu") as csv_to_parquet_snappy_ds1_bu:
        for id in bu:
            task1 = BashOperator(task_id=f'{task_group_id_pq_ds1}_{id}', bash_command='sleep 10')

    with TaskGroup(f"{task_group_id_pq_ds2}_bu") as csv_to_parquet_snappy_ds2_bu:
        for id in bu:
            task1 = BashOperator(task_id=f'{task_group_id_pq_ds2}_{id}', bash_command='sleep 10')

    with TaskGroup(f"{task_group_id_pq_ds3}_bu") as csv_to_parquet_snappy_ds3_bu:
        for id in bu:
            task1 = BashOperator(task_id=f'{task_group_id_pq_ds3}_{id}', bash_command='sleep 10')

    step_1_start = DummyOperator(task_id="step-1_start")
    step_1_end = DummyOperator(task_id="step-1_end")

    step_1_start >> sftp_to_gcs_ds1 >> sftp_to_gcs_ds1_bu >> csv_to_parquet_snappy_ds1_bu >> gcs_to_snowflow >> step_1_end >> trigger_step2
    step_1_start >> sftp_to_gcs_ds2 >> sftp_to_gcs_ds2_bu >> csv_to_parquet_snappy_ds2_bu >> gcs_to_snowflow >> step_1_end >> trigger_step2
    step_1_start >> sftp_to_gcs_ds3 >> sftp_to_gcs_ds3_bu >> csv_to_parquet_snappy_ds3_bu >> gcs_to_snowflow >> step_1_end >> trigger_step2