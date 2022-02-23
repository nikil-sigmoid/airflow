from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow import configuration as conf
from filters.config_params_override import return_first_matching_result
from macros.ds_utilities import ds_slash

default_args = {
    'owner': 'Nikhil',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG('config_override_test',
          schedule_interval='@once',
          default_args=default_args,
          user_defined_macros={'return_first_matching_result': return_first_matching_result, 'ds_slash':ds_slash},
          jinja_environment_kwargs={
              'variable_start_string': '<<',
              'variable_end_string': '>>',
          },
          )

# lis = '{{dag_run.conf["my_key"]}}'

with dag:
    task1 = KubernetesPodOperator(
        namespace='default',
        image="localhost:5000/alpine",
        labels={"foo": "bar"},
        name="airflow-test-pod1",
        image_pull_policy='Always',
        task_id="task-one",
        in_cluster=False,  # if set to true, will look in the cluster, if false, looks for file
        cluster_context='docker-desktop',  # is ignored when in_cluster is set to True
        get_logs=True,
        is_delete_operator_pod=True,
        arguments=[
            '--input_bucket_name',
            'bucket-name',
            '--elt_by',
            '<< ds >>',
            # '{{ return_first_matching_result(None, "", "Actual_value_1") }}',
            # '{{ return_first_matching_result(dag_run.conf["my_key"], None, "", "real_value") }}',
            '<< ds_slash(ds) >>'
            # '{{ params.sfmc_elt_by }}'
        ],
    )