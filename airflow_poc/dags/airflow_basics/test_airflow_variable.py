# image name: env_test_kub:v1

from datetime import datetime, timedelta
from airflow.models.dagrun import DagRun

from airflow import DAG
# from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
# from airflow.operators.python import PythonOperator
from airflow.operators.python import get_current_context


from airflow.models import Variable
import json



import re
import ast
# dataset_failed_str = '{{ dag_run.conf }}'
# datasets_failed_str = re.sub('\'', '\"', dataset_failed_str)
# dataset_failed_json = json.loads(dataset_failed_str)
# dataset_failed_json = ast.literal_eval(dataset_failed_str)
# dataset_failed_json = eval(dataset_failed_str)

# dataset_failed_str = '{"dataset_names": ["ds_1", "ds_4", "ds_5"]}'
# dataset_failed_json = json.loads(dataset_failed_str)
# key = "dataset_names"




from demjson import decode
# dataset_failed_str = '{{ dag_run.conf["value"]}}'
# # print(type(dataset_failed_str))
# print(dataset_failed_str)
# dataset_failed_json = json.loads(dataset_failed_str)

# dataset_failed_str = re.sub('\'', '\"', dataset_failed_str)
# dataset_failed_str = ast.literal_eval(dataset_failed_str)
# dataset_failed_str = eval(dataset_failed_str)
# dataset_failed_json = decode(dataset_failed_str)

# dataset_failed_json = json.loads(dataset_failed_str)
#
# print(type(dataset_failed_str))
# print(dataset_failed_str)
# key = "dataset_names"
#


# dataset_list_str = Variable.get("dataset_list_str")
# dataset_list_json = json.loads(dataset_list_str)
#
# if key not in dataset_failed_json.keys():
#     datasets_to_be_processed = dataset_list_json
# else:
#     dataset_names = dataset_failed_json[key]
#     datasets_to_be_processed = {dataset_name: dataset_info for (dataset_name, dataset_info) in dataset_list_json.items() for name in dataset_names if dataset_name == name}
#
# print("Datasets that are to be processed: ")
# print(datasets_to_be_processed)

from airflow.configuration import  conf

datasets_to_be_processed = ""


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# value =

dataset_failed_str = "{{ dag_run.conf }}"

temp_var = ""


def test_fun(**kwargs):

    print(repr(dataset_failed_str))

    print("here")
    print(kwargs)
    temp_var = kwargs["params"]
    print(kwargs["params"])
    print("ends")
    # print('{{ params }}')
    # dataset_list_str = Variable.get("dag_run.conf")
    # print(dataset_list_str)
    # print("{{ ds }}")
    # print(type(dataset_failed_str))
    # print(val)
    # print(dataset_failed_str)





dag = DAG('airflow_var_test',
          schedule_interval='@once',
          default_args=default_args)


# val = dag.get_dagrun()

with dag:

    env_var_test = KubernetesPodOperator(
        namespace='default',
        image='localhost:5000/airflow_var:v1',
        name="airflow_var_test",
        is_delete_operator_pod=True,
        image_pull_policy='Always',
        in_cluster=False,
        task_id="airflow_var_test",
        get_logs=True,
        # arguments=["--datasets", dataset_list_str, "--failed", datasets_failed_str],
        # arguments=["--datasets", dataset_list_str, "--failed", '{{ dag_run.conf["dataset_names"]}}' ],
        arguments=["--datasets", temp_var],

        # arguments=["--name", '{{ dag_run.conf["dataset_names"]}}'],
        # env_vars={'GREETING': 'hey! my name is  = {{ dag_run.conf["my_name"] }}', "NAME":"BatSupIronman"}
    )

    python_task = PythonOperator(
        task_id="python_task",
        python_callable=test_fun,
        # provide_context=True
        # start_date=
    )

    python_task >> env_var_test
