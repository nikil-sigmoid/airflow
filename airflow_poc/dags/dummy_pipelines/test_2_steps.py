from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow import configuration as conf

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('test_2_steps',
          schedule_interval='@once',
          default_args=default_args)



with dag:
    task1 = KubernetesPodOperator(
        namespace='default',
        image="localhost:5000/step_1:v2",
        labels={"foo": "bar"},
        name="airflow-test-pod1",
        image_pull_policy='Always',
        task_id="task-one",
        in_cluster=False,  # if set to true, will look in the cluster, if false, looks for file
        cluster_context='docker-desktop',  # is ignored when in_cluster is set to True
        get_logs=True,
        annotations={"some-key": "some-value"},
        is_delete_operator_pod=False,
        # cmds=["python", "-c", "./hello_world.py"],
        # cmds=["python", "-c", "print('hello task 1 ..................')"],
        arguments=[
            '--input_bucket_name',
            'bucket-name',
            '--output_bucket_name',
            'working-bucket-name',
            '--input_path_filenames',
            'output_file_json',
            '--output_path',
            'appended_csv',
            '--output_filename',
            'csv combined filename'
        ],
        service_account_name='build-robot',
    )


    task2 = KubernetesPodOperator(
        namespace='default',
        image="localhost:5000/step_2:v2",
        labels={"foo": "bar"},
        name="airflow-test-pod2",
        image_pull_policy='Always',
        task_id="task-two",
        in_cluster=False,  # if set to true, will look in the cluster, if false, looks for file
        cluster_context='docker-desktop',  # is ignored when in_cluster is set to True
        get_logs=True,
        annotations={"some-key": "some-value"},
        is_delete_operator_pod=False,
        # cmds=["python", "-c", "print('hello task 2 ..................')"],
        arguments=[
            '--input_bucket_name',
            'bucket-name',
            '--output_bucket_name',
            'working-bucket-name',
            '--input_path_filenames',
            'output_file_json',
            '--output_path',
            'appended_csv',
            '--output_filename',
            'csv combined filename'
        ],
        service_account_name='build-robot',
    )

    task1 >> task2