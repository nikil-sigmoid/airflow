from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.cncf.kubernetes.operators import kubernetes_pod as kubernetes_pod_operator

args = {
    "owner": "Nikhil",
    "start_date": datetime(2021, 12, 8),
    "email_on_failure": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

env_vars = {"elt_ts": "some_value78"}

with DAG(dag_id='dbt_seed_gcs_demo', schedule_interval=None, start_date=datetime(2021, 12, 9),
         catchup=False,
         default_args=args,
         tags=['dbt']) as dag:
    start_task = DummyOperator(task_id='start'),

    dbt_run = kubernetes_pod_operator.KubernetesPodOperator(
        task_id="dbt_run",
        image="localhost:5000/dbt_seed_gcs:v1",
        namespace='default',
        startup_timeout_seconds=300,
        image_pull_policy="Always",
        in_cluster=False,
        get_logs=True,
        is_delete_operator_pod=True,
        name="dbt_bigquery"
    )

    start_task >> dbt_run
