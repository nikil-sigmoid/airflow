from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow import configuration as conf
# from airflow.kubernetes.volume import Volume
from airflow.kubernetes.volume_mount import VolumeMount
from airflow.providers.cncf.kubernetes.backcompat.volume import Volume
from airflow.operators.bash import BashOperator

# airflow.providers.cncf.kubernetes.backcompat.volume.Volume'> or <class 'kubernetes.client.models.v1_volume.V1Volume'>, got <class 'str'>

default_args = {
    'owner': 'Nikhil',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('xcom_2_steps_test',
          schedule_interval='@once',
          default_args=default_args)



# with dag:
#     task1 = KubernetesPodOperator(
#         namespace='default',
#         image="localhost:5000/step_1:v3",
#         labels={"foo": "bar"},
#         name="airflow-test-pod1",
#         image_pull_policy='Always',
#         task_id="task-one",
#         in_cluster=False,  # if set to true, will look in the cluster, if false, looks for file
#         cluster_context='docker-desktop',  # is ignored when in_cluster is set to True
#         get_logs=True,
#         annotations={"some-key": "some-value"},
#         volumes=[volume],
#         volume_mounts=[volume_mount],
#         # cmds=["python", "-c", "./hello_world.py"],
#         # cmds=["python", "-c", "print('hello task 1 ..................')"],
#         arguments=[
#             '--input_bucket_name',
#             'bucket-name',
#             '--output_bucket_name',
#             'working-bucket-name',
#             '--input_path_filenames',
#             'output_file_json',
#             '--output_path',
#             'appended_csv',
#             '--output_filename',
#             'csv combined filename'
#         ],
#         service_account_name='build-robot',
#     )
#
#
#     task2 = KubernetesPodOperator(
#         namespace='default',
#         image="localhost:5000/step_2:v3",
#         labels={"foo": "bar"},
#         name="airflow-test-pod287",
#         image_pull_policy='Always',
#         task_id="task-two",
#         in_cluster=False,  # if set to true, will look in the cluster, if false, looks for file
#         cluster_context='docker-desktop',  # is ignored when in_cluster is set to True
#         get_logs=True,
#         annotations={"some-key": "some-value"},
#         volumes=[volume],
#         volume_mounts=[volume_mount],
#         # cmds=["python", "-c", "./hello_world.py"],
#         # cmds=["python", "-c", "print('hello task 1 ..................')"],
#         arguments=[
#             '--input_bucket_name',
#             'bucket-name',
#             '--output_bucket_name',
#             'working-bucket-name',
#             '--input_path_filenames',
#             'output_file_json',
#             '--output_path',
#             'appended_csv',
#             '--output_filename',
#             'csv combined filename'
#         ],
#         service_account_name='build-robot',
#     )


with dag:
    write_xcom = KubernetesPodOperator(
        namespace='default',
        image='localhost:5000/xcomtest:v1',
        # cmds=["sh", "-c", "mkdir -p /airflow/xcom/;echo '[1,2,3,4]' > /airflow/xcom/return.json"],
        name="write-xcom",
        do_xcom_push=True,
        is_delete_operator_pod=True,
        image_pull_policy='Always',
        in_cluster=False,
        # cluster_context='docker-desktop',
        task_id="write-xcom",
        get_logs=True,
    )

    # pod_task_xcom_result = BashOperator(
    #     bash_command="echo \"{{ task_instance.xcom_pull('write-xcom')['rollno'] }}\"",
    #     task_id="pod_task_xcom_result",
    # )

    pod_task_xcom_result = KubernetesPodOperator(
        namespace='default',
        image='localhost:5000/xcompull:v1',
        # cmds=["sh", "-c", "mkdir -p /airflow/xcom/;echo '[1,2,3,4]' > /airflow/xcom/return.json"],
        name="write-xcom-pull",
        do_xcom_push=True,
        is_delete_operator_pod=True,
        image_pull_policy='Always',
        in_cluster=False,
        # cluster_context='docker-desktop',
        task_id="read-xcom-pull",
        get_logs=True,
    )


    write_xcom >> pod_task_xcom_result
