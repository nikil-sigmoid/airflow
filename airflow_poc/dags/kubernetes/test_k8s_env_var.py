# image name: env_test_kub:v1

from datetime import datetime, timedelta

from airflow import DAG
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

dag = DAG('env_var_test',
          schedule_interval='@once',
          default_args=default_args)

with dag:
    env_var_test = KubernetesPodOperator(
        namespace='default',
        image='localhost:5000/env_test_kub:v1',
        name="env_var_test",
        is_delete_operator_pod=True,
        image_pull_policy='Always',
        in_cluster=False,
        task_id="env_var_test",
        get_logs=True,
        # arguments=["--name", 'my name is  = {{ dag_run.conf["my_name"] }}'],
        # env_vars={"GREET": "Hey there! how are you doing?"}
        env_vars={'GREETING': 'hey! my name is  = {{ dag_run.conf["my_name"] }}', "NAME":"BatSupIronman"}
        # bucket_name =
    )

    env_var_test









# {
#     "ds_1":{
#         "file_name_stem":"ds_1{{ yesterday_ds_nodash }}",
#         "sf_table_name":"ds_1",
# }
# }

