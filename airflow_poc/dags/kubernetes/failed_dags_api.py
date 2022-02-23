from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

default_args = {
    'owner': 'Nikhil',
    'start_date': datetime(2022, 1, 5),
}

dag = DAG('test_failed_dags_api',
          schedule_interval='@once',
          default_args=default_args)

with dag:
    task1 = KubernetesPodOperator(
        namespace='default',
        image="localhost:5000/failed_dags_api:v1",
        labels={"foo": "bar"},
        name="failed_dags_api-pod1",
        image_pull_policy='Always',
        task_id="task-one",
        in_cluster=False,
        get_logs=True,
        is_delete_operator_pod=True,
    )