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
        'claimName': 'k8s-pvc-vol-1'
      }
    }

volume = Volume(name='k8s-pv-vol-1', configs=volume_config)

volume_mount = VolumeMount('k8s-pv-vol-1',
                            mount_path='/mydir',
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

dag = DAG('k8s_vol_demo_1',
          schedule_interval='@once',
          default_args=default_args)



with dag:
    task1 = KubernetesPodOperator(
        namespace='default',
        image="localhost:5000/vol_demo_image:v1",
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
    )