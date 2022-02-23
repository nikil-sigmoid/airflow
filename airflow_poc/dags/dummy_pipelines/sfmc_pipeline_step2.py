# taskgroup.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago

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

with DAG('sfmc_pipeline_step2', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
    with TaskGroup(task_group_id1) as sftp_to_gcs:
        for id in id_list_step2:
            task1 = BashOperator(task_id=f'{task_group_id1}_{id}', bash_command='sleep 10')

    with TaskGroup(task_group_id2) as csv_to_parquet_snappy:
        for id in id_list_step2:
            task1 = BashOperator(task_id=f'{task_group_id2}_{id}', bash_command='sleep 10')

    with TaskGroup(task_group_id3) as sftp_to_gcs_bu1:
        for id in id_list_step2:
            task1 = BashOperator(task_id=f'{task_group_id1}_{task_group_id3}_{id}', bash_command='sleep 10')

    with TaskGroup(task_group_id4) as sftp_to_gcs_bu2:
        for id in id_list_step2:
            task1 = BashOperator(task_id=f'{task_group_id1}_{task_group_id4}_{id}', bash_command='sleep 10')

    with TaskGroup(task_group_id5) as sftp_to_gcs_bu3:
        for id in id_list_step2:
            task1 = BashOperator(task_id=f'{task_group_id1}_{task_group_id5}_{id}', bash_command='sleep 10')

    with TaskGroup(task_group_id6) as csv_to_parquet_snappy_bu1:
        for id in id_list_step2:
            task1 = BashOperator(task_id=f'{task_group_id2}_{task_group_id3}_{id}', bash_command='sleep 10')

    with TaskGroup(task_group_id7) as csv_to_parquet_snappy_bu2:
        for id in id_list_step2:
            task1 = BashOperator(task_id=f'{task_group_id2}_{task_group_id4}_{id}', bash_command='sleep 10')

    with TaskGroup(task_group_id8) as csv_to_parquet_snappy_bu3:
        for id in id_list_step2:
            task1 = BashOperator(task_id=f'{task_group_id2}_{task_group_id5}_{id}', bash_command='sleep 10')

    gcs_to_snowflow = BashOperator(task_id='gcs_to_snowflow', bash_command='sleep 10')

    sftp_to_gcs >> [sftp_to_gcs_bu1, sftp_to_gcs_bu2, sftp_to_gcs_bu3] >> csv_to_parquet_snappy >> [csv_to_parquet_snappy_bu1, csv_to_parquet_snappy_bu2, csv_to_parquet_snappy_bu3] >> gcs_to_snowflow