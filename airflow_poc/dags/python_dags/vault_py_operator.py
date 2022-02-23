from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.hooks.base import BaseHook
# from airflow.providers.hashicorp.secrets.vault import VaultBackend
from airflow.secrets import BaseSecretsBackend

# ok = BaseSecretsBackend.get_connection('smtp_default')
# ok = BaseHook.get_connection('smtp_default')

# ok = VaultBackend.get_connection()
# print(f"ok: {ok.get_uri()}")

val = conn = BaseHook.get_connection('smtp_default')
print(conn.get_uri())

def get_secrets(**kwargs):
    conn = BaseHook.get_connection(kwargs['my_conn_id'])
    print(f"Password: {conn.password}, Login: {conn.login}, URI: {conn.get_uri()}, Host: {conn.host}")

with DAG('vault_demo', start_date=datetime(2022, 1, 18), schedule_interval=None) as dag:

    test_task = PythonOperator(
        task_id='test-task',
        python_callable=get_secrets,
        op_kwargs={'my_conn_id': 'smtp_default'},
    )
