from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.hashicorp.secrets.vault import VaultBackend
from airflow.secrets import BaseSecretsBackend

from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

# ok = BaseSecretsBackend.get_connection('smtp_default')
# ok = BaseHook.get_connection('smtp_default')

# ok = VaultBackend.get_connection()
# print(f"ok: {ok.get_uri()}")

# val = conn = BaseHook.get_connection('smtp_default')
# print(conn.get_uri())

# def get_secrets(**kwargs):
#     conn = BaseHook.get_connection(kwargs['my_conn_id'])
#     print(f"Password: {conn.password}, Login: {conn.login}, URI: {conn.get_uri()}, Host: {conn.host}")
#



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('vault_k8s_operator',
          schedule_interval='@once',
          default_args=default_args)

with dag:
    task1 = KubernetesPodOperator(
        namespace='default',
        image="localhost:5000/vault_k8s_operator:v1",
        labels={"foo": "bar"},
        name="airflow-vault-k8s-pod1",
        image_pull_policy='Always',
        task_id="task-one",
        env_vars={"NAME_1": "Batman"},
        in_cluster=False,
        get_logs=True,
        is_delete_operator_pod=True,
    )

































