from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow import configuration as conf
# from airflow.kubernetes.volume import Volume
from airflow.kubernetes.volume_mount import VolumeMount
from airflow.providers.cncf.kubernetes.backcompat.volume import Volume

# airflow.providers.cncf.kubernetes.backcompat.volume.Volume'> or <class 'kubernetes.client.models.v1_volume.V1Volume'>, got <class 'str'>


volume_config= {
    'persistentVolumeClaim':
      {
        'claimName': 'pvc-vol-2'
      }
    }

volume = Volume(name='pv-vol-2', configs=volume_config)

volume_mount = VolumeMount('pv-vol-2',
                            mount_path='/xyz',
                            sub_path=None,
                            read_only=False)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('test_volume',
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
        volumes=[volume],
        volume_mounts=[volume_mount],
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
        image="localhost:5000/step_1test:v2",
        labels={"foo": "bar"},
        name="airflow-test-pod287",
        image_pull_policy='Always',
        task_id="task-two",
        in_cluster=False,  # if set to true, will look in the cluster, if false, looks for file
        cluster_context='docker-desktop',  # is ignored when in_cluster is set to True
        get_logs=True,
        annotations={"some-key": "some-value"},
        volumes=[volume],
        volume_mounts=[volume_mount],
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

    task1 >> task2

#
# from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
# from airflow.kubernetes.secret import Secret
# from airflow.contrib.kubernetes.secret import Secret
# from airflow.kubernetes.volume import Volume
# from airflow.kubernetes.volume_mount import VolumeMount
# from airflow.contrib.kubernetes.pod import Port
#
#
# secret_file = Secret('volume', '/etc/sql_conn', 'airflow-secrets', 'sql_alchemy_conn')
# secret_env  = Secret('env', 'SQL_CONN', 'airflow-secrets', 'sql_alchemy_conn')
# secret_all_keys  = Secret('env', None, 'airflow-secrets-2')
# volume_mount = VolumeMount('test-volume',
#                             mount_path='/root/mount_file',
#                             sub_path=None,
#                             read_only=True)
# port = Port('http', 80)
# configmaps = ['test-configmap-1', 'test-configmap-2']
#
# volume_config= {
#     'persistentVolumeClaim':
#       {
#         'claimName': 'test-volume'
#       }
#     }
# volume = Volume(name='test-volume', configs=volume_config)
#
# k = KubernetesPodOperator(namespace='default',
#                           image="ubuntu:16.04",
#                           cmds=["bash", "-cx"],
#                           arguments=["echo", "10"],
#                           labels={"foo": "bar"},
#                           secrets=[secret_file, secret_env, secret_all_keys],
#                           ports=[port]
#                           volumes=[volume],
#                           volume_mounts=[volume_mount]
#                           name="test",
#                           task_id="task",
#                           affinity=affinity,
#                           is_delete_operator_pod=True,
#                           hostnetwork=False,
#                           tolerations=tolerations,
#                           configmaps=configmaps
#                           )