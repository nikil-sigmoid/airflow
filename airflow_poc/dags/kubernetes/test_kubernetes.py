from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow import configuration as conf

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

sfmc_elt_by = """dag id: {{dag.dag_id}} task id:{{task.task_id}} run id:{{ run_id }} {{params.temp_var}} """

# namespace = conf.get('kubernetes', 'NAMESPACE')

# This will detect the default namespace locally and read the
# environment namespace when deployed to Astronomer.
# if namespace =='default':
#     config_file = '/usr/local/airflow/include/.kube/kube_config'
#     in_cluster=False
# else:
#     in_cluster=True
#     config_file=None

dag = DAG('test_kubernetes_pod',
          schedule_interval='@once',
          default_args=default_args)



with dag:
    task1 = KubernetesPodOperator(
        # resources={},
        namespace='default',
        image="nikilr/hello_test1:dev",
        labels={"foo": "bar"},
        name="airflow-test-pod1",
        image_pull_policy='Always',
        task_id="task-one",
        in_cluster=False,  # if set to true, will look in the cluster, if false, looks for file
        cluster_context='docker-desktop',  # is ignored when in_cluster is set to True
        get_logs=True,
        annotations={"some-key": "some-value"},
        is_delete_operator_pod=True,
        cmds=["python", "-c", "print('hello task 1 ..................')"],
        # params={'sfmc_elt_by': "{{ds}}"},
        arguments=[
            '--input_bucket_name',
            'bucket-name',
            '--output_bucket_name',
            'working-bucket-name',
            '--input_path_filenames',
            'output_file_json',
            '--output_path',
            'appended_csv',
            '--output_filename',
            'csv combined filename',
            '--elt_by',
            '{{ ds }}'
            # '{{ params.sfmc_elt_by }}'
        ],


        # service_account_name='build-robot',
    )


    task2 = KubernetesPodOperator(
        namespace='default',
        image="python:3.7",
        labels={"foo": "bar"},
        name="airflow-test-pod2",
        image_pull_policy='Always',
        task_id="task-two",
        in_cluster=False,  # if set to true, will look in the cluster, if false, looks for file
        cluster_context='docker-desktop',  # is ignored when in_cluster is set to True
        get_logs=True,
        annotations={"some-key": "some-value"},
        is_delete_operator_pod=True,
        cmds=["python", "-c", "print('hello task 2 ..................')"],
        arguments=[
            '--input_bucket_name',
            'bucket-name',
            '--output_bucket_name',
            'working-bucket-name',
            '--input_path_filenames',
            'output_file_json',
            '--output_path',
            'appended_csv',
            '--output_filename',
            'csv combined filename'
        ],
        # service_account_name='build-robot',
    )

    task1 >> task2

# config_file = os.path.expanduser('~') + "/.kube/kube_config",

# with dag:
#     k = KubernetesPodOperator(
#         namespace='default',
#         image="hello-world",
#         labels={"foo": "bar"},
#         name="airflow-test-pod",
#         cmds=["python", "2 + 2"],
#         image_pull_policy='Always',
#         arguments=[
#             '--input_bucket_name',
#             'bucket-name',
#             '--output_bucket_name',
#             'working-bucket-name',
#             '--input_path_filenames',
#             'output_file_json',
#             '--output_path',
#              "/appended_csv",
#             '--output_filename',
#             "csvs combined filename"
#         ],
#         task_id="task-one",
#         in_cluster=False, # if set to true, will look in the cluster, if false, looks for file
#         cluster_context='docker-desktop', # is ignored when in_cluster is set to True
#         get_logs=True,
#         # dag=dag,
#         is_delete_operator_pod=False,
#         service_account_name='vault-sidecar',
#         annotations={"some-key": "some-value"}
#     )




# [kubernetes]
# airflow_configmap = dev-gdo-analytics-airflow-kube_config
# airflow_local_settings_configmap = dev-gdo-analytics-airflow-kube_config
# multi_namespace_mode = False
# namespace = dev-gdo-analytics
# pod_template_file = /opt/airflow/pod_templates/pod_template_file.yaml
# worker_container_repository = us-east4-docker.pkg.dev/cp-artifact-registry/airflow/airflow
# worker_container_tag = 2.2.2-python3.7
# worker_pods_creation_batch_size = 4






# append_csv_in_bucket_task = kubernetes_pod_operator.KubernetesPodOperator(
#         task_id='append_csv_in_bucket_task',
#         name='append_csv_in_bucket_task',
#         namespace=namespace,
#         image=APPEND_CSV_BUCKET_IMG,
#         image_pull_policy='Always',
#         arguments=[
#             '--input_bucket_name',
#             WORKING_BUCKET_NAME,
#             '--output_bucket_name',
#             WORKING_BUCKET_NAME,
#             '--input_path_filenames',
#             api_output_file_json,
#             '--output_path',
#             WORKING_BUCKET_PATH + "/appended_csv",
#             '--output_filename',
#             CSVS_COMBINED_FILENAME
#         ],
#         annotations=annotations,
#         get_logs=True,
#         service_account_name='vault-sidecar',
#         dag=dag,
#         is_delete_operator_pod=False
#     )
#


# append_csv_in_bucket_task = kubernetes_pod_operator.KubernetesPodOperator(
#         task_id='append_csv_in_bucket_task',
#         name='append_csv_in_bucket_task',

#         cmds=["python", TEST_VAULT_SCRIPT_PATH],
#         TEST_VAULT_SCRIPT_PATH = "./entrypoint.py

#         namespace=namespace,
#         image=APPEND_CSV_BUCKET_IMG,
#         image_pull_policy='Always',
#         arguments=[
#             '--input_bucket_name',
#             WORKING_BUCKET_NAME,
#             '--output_bucket_name',
#             WORKING_BUCKET_NAME,
#             '--input_path_filenames',
#             api_output_file_json,
#             '--output_path',
#             WORKING_BUCKET_PATH + "/appended_csv",
#             '--output_filename',
#             CSVS_COMBINED_FILENAME
#         ],
#         annotations=annotations,
#         get_logs=True,
#         service_account_name='vault-sidecar',
#         dag=dag,
#         is_delete_operator_pod=False
#     )