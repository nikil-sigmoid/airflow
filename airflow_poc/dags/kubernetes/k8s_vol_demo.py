from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow import configuration as conf
# from airflow.kubernetes.volume import Volume
from airflow.kubernetes.volume_mount import VolumeMount
from airflow.providers.cncf.kubernetes.backcompat.volume import Volume
from kubernetes.client import models as k8s

#
# volume = k8s.V1Volume(name='k8s-vol-1', persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='k8s-pvc-1'))
#
# volume_mount = VolumeMount('k8s-vol-1',
#                            mount_path='/k8s_data',
#                            sub_path=None,
#                            read_only=False)


volume = k8s.V1Volume(name='models-1-0-0', persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='models-1-0-0-claim'))

volume_mount = VolumeMount('models-1-0-0',
                           mount_path='/mnt/random',
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

dag = DAG('k8s_vol_demo',
          schedule_interval='@once',
          default_args=default_args)

with dag:
    task1 = KubernetesPodOperator(
        namespace='default',
        image="localhost:5000/ubuntu_bash:v1",
        labels={"foo": "bar"},
        # name="airflow-test-pod1",
        image_pull_policy='Always',
        task_id="task-one",
        in_cluster=False,  # if set to true, will look in the cluster, if false, looks for file
        cluster_context='docker-desktop',  # is ignored when in_cluster is set to True
        get_logs=True,
        annotations={"some-key": "some-value"},
        volumes=[volume],
        volume_mounts=[volume_mount],
        # service_account_name='build-robot',
        pod_template_file="/Users/nik/pod_file.yml"
    )

    task1






#
# jobs:
#   selectivecopy:
#     if: github.event.pull_request.merged == true
#     name: Selective Copy to temp folder
#     runs-on: ubuntu-latest
#     container:
#       image: us-east4-docker.pkg.dev/cp-artifact-registry/gdo-analytics/schemachange:dev
#       credentials:
#         username:
#         password:





# Error: INSTALLATION FAILED: unable to build kubernetes objects from release manifest:
# [unable to recognize "": no matches for kind "ClusterRoleBinding" in version "rbac.authorization.k8s.io/v1beta1",
# unable to recognize "": no matches for kind "MutatingWebhookConfiguration" in version "admissionregistration.k8s.io/v1beta1"]