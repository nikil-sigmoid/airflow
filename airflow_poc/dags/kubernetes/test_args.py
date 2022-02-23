from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('args_test',
          schedule_interval='@once',
          default_args=default_args)

with dag:
    write_xcom = KubernetesPodOperator(
        namespace='default',
        image='localhost:5000/argstest:v1',
        # cmds=["sh", "-c", "mkdir -p /airflow/xcom/;echo '[1,2,3,4]' > /airflow/xcom/return.json"],
        name="write-xcom",
        # do_xcom_push=True,
        is_delete_operator_pod=True,
        image_pull_policy='Always',
        in_cluster=False,
        # cluster_context='docker-desktop',
        task_id="write-xcom",
        get_logs=True,
        # arguments=["--name", '[ "name":"John", "age":30, "city":"New York"]'],
        arguments=["--name", 'my name is  = {{ dag_run.conf["my_name"] }}'],
    )

    write_xcom


    # write_xcom = KubernetesPodOperator(
    #     namespace='default',
    #     image='f46e78e2b8',
    #     # cmds=["sh", "-c", "mkdir -p /airflow/xcom/;echo '[1,2,3,4]' > /airflow/xcom/return.json"],
    #     name="write-xcom",
    #     do_xcom_push=True,
    #     is_delete_operator_pod=True,
    #     image_pull_policy='Always',
    #     in_cluster=False,
    #     # cluster_context='docker-desktop',
    #     task_id="write-xcom",
    #     get_logs=True,
    # )
