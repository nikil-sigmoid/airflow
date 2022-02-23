from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow import configuration as conf

default_args = {
    'owner': 'Nikhil',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

sfmc_elt_by = """dag id: {{dag.dag_id}} task id:{{task.task_id}} run id:{{ run_id }} {{params.temp_var}} """


dag = DAG('csv_to_parquet_test',
          schedule_interval='@once',
          default_args=default_args)


with dag:
    task1 = KubernetesPodOperator(
        namespace='default',
        image="localhost:5000/csv_to_parquet_image:v1",
        # labels={"foo": "bar"},
        name="airflow-parquet-pod1",
        image_pull_policy='Always',
        task_id="task-one",
        in_cluster=False,  # if set to true, will look in the cluster, if false, looks for file
        cluster_context='docker-desktop',  # is ignored when in_cluster is set to True
        get_logs=True,
        annotations={"some-key": "some-value"},
        is_delete_operator_pod=True,
        # resources={'limit_memory': "6G", 'limit_cpu': "4000m"},
        # cmds=["python", "-c", "print('hello task 1 ..................')"],
    )

    task1