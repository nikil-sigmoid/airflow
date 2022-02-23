from airflow import DAG
from datetime import datetime, timedelta
# from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('test_dbt_run',
          schedule_interval='@once',
          default_args=default_args)


with dag:
    task1 = KubernetesPodOperator(
        namespace='default',
        image="localhost:5000/dbt_run:v1",
        labels={"foo": "bar"},
        name="airflow-jdhlf-copy-pod1",
        image_pull_policy='Always',
        task_id="task-one",
        in_cluster=False,
        cluster_context='docker-desktop',
        get_logs=True,
        is_delete_operator_pod=True,
    )

