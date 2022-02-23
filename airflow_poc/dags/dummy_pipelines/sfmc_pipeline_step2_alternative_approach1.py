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

task_group_id_ds4 = "sftp_to_gcs_ds4"
task_group_id_ds5 = "sftp_to_gcs_ds5"
task_group_id_ds6 = "sftp_to_gcs_ds6"
task_group_id_ds7 = "sftp_to_gcs_ds7"
task_group_id_ds8 = "sftp_to_gcs_ds8"


task_group_id_pq_ds4 = "csv_to_parquet_snappy_ds4"
task_group_id_pq_ds5 = "csv_to_parquet_snappy_ds5"
task_group_id_pq_ds6 = "csv_to_parquet_snappy_ds6"
task_group_id_pq_ds7 = "csv_to_parquet_snappy_ds7"
task_group_id_pq_ds8 = "csv_to_parquet_snappy_ds8"

bu = ["bu1", "bu2", "bu3", "bu4"]

with DAG('sfmc_pipeline_step2_alternative_approach1', schedule_interval='@daily', default_args=default_args,
         catchup=False) as dag:
    with TaskGroup(f"{task_group_id_ds4}_bu") as sftp_to_gcs_ds4_bu:
        for id in bu:
            task1 = BashOperator(task_id=f'{task_group_id_ds4}_{id}', bash_command='sleep 10')

    with TaskGroup(f"{task_group_id_ds5}_bu") as sftp_to_gcs_ds5_bu:
        for id in bu:
            task1 = BashOperator(task_id=f'{task_group_id_ds5}_{id}', bash_command='sleep 10')

    with TaskGroup(f"{task_group_id_ds6}_bu") as sftp_to_gcs_ds6_bu:
        for id in bu:
            task1 = BashOperator(task_id=f'{task_group_id_ds6}_{id}', bash_command='sleep 10')

    with TaskGroup(f"{task_group_id_ds7}_bu") as sftp_to_gcs_ds7_bu:
        for id in bu:
            task1 = BashOperator(task_id=f'{task_group_id_ds7}_{id}', bash_command='sleep 10')

    with TaskGroup(f"{task_group_id_ds8}_bu") as sftp_to_gcs_ds8_bu:
        for id in bu:
            task1 = BashOperator(task_id=f'{task_group_id_ds8}_{id}', bash_command='sleep 10')

    gcs_to_snowflow = BashOperator(task_id='gcs_to_snowflow', bash_command='sleep 10')

    sftp_to_gcs_ds4 = BashOperator(task_id='sftp_to_gcs_ds4', bash_command='sleep 10')
    sftp_to_gcs_ds5 = BashOperator(task_id='sftp_to_gcs_ds5', bash_command='sleep 10')
    sftp_to_gcs_ds6 = BashOperator(task_id='sftp_to_gcs_ds6', bash_command='sleep 10')
    sftp_to_gcs_ds7 = BashOperator(task_id='sftp_to_gcs_ds7', bash_command='sleep 10')
    sftp_to_gcs_ds8 = BashOperator(task_id='sftp_to_gcs_ds8', bash_command='sleep 10')


    with TaskGroup(f"{task_group_id_pq_ds4}_bu") as csv_to_parquet_snappy_ds4_bu:
        for id in bu:
            task1 = BashOperator(task_id=f'{task_group_id_pq_ds4}_{id}', bash_command='sleep 10')

    with TaskGroup(f"{task_group_id_pq_ds5}_bu") as csv_to_parquet_snappy_ds5_bu:
        for id in bu:
            task1 = BashOperator(task_id=f'{task_group_id_pq_ds5}_{id}', bash_command='sleep 10')

    with TaskGroup(f"{task_group_id_pq_ds6}_bu") as csv_to_parquet_snappy_ds6_bu:
        for id in bu:
            task1 = BashOperator(task_id=f'{task_group_id_pq_ds6}_{id}', bash_command='sleep 10')

    with TaskGroup(f"{task_group_id_pq_ds7}_bu") as csv_to_parquet_snappy_ds7_bu:
        for id in bu:
            task1 = BashOperator(task_id=f'{task_group_id_pq_ds7}_{id}', bash_command='sleep 10')

    with TaskGroup(f"{task_group_id_pq_ds8}_bu") as csv_to_parquet_snappy_ds8_bu:
        for id in bu:
            task1 = BashOperator(task_id=f'{task_group_id_pq_ds8}_{id}', bash_command='sleep 10')

    step_2_start = DummyOperator(task_id="step-2_start")
    step_2_end = DummyOperator(task_id="step-2_end")

    step_2_start >> sftp_to_gcs_ds4 >> sftp_to_gcs_ds4_bu >> csv_to_parquet_snappy_ds4_bu >> gcs_to_snowflow >> step_2_end
    step_2_start >> sftp_to_gcs_ds5 >> sftp_to_gcs_ds5_bu >> csv_to_parquet_snappy_ds5_bu >> gcs_to_snowflow >> step_2_end
    step_2_start >> sftp_to_gcs_ds6 >> sftp_to_gcs_ds6_bu >> csv_to_parquet_snappy_ds6_bu >> gcs_to_snowflow >> step_2_end
    step_2_start >> sftp_to_gcs_ds7 >> sftp_to_gcs_ds7_bu >> csv_to_parquet_snappy_ds7_bu >> gcs_to_snowflow >> step_2_end
    step_2_start >> sftp_to_gcs_ds8 >> sftp_to_gcs_ds8_bu >> csv_to_parquet_snappy_ds8_bu >> gcs_to_snowflow >> step_2_end

